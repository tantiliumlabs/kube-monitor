package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ochestra-tech/k8s-monitor/internal/app/billing"
)

const billingSchema = `
CREATE TABLE IF NOT EXISTS k8s_monitor.cost_daily (
    day         DATE             NOT NULL,
    usage_type  TEXT             NOT NULL,
    record_type TEXT             NOT NULL,
    amount      DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL DEFAULT 0,
    unit        TEXT             NOT NULL DEFAULT '',
    currency    TEXT             NOT NULL DEFAULT 'USD',
    PRIMARY KEY (day, usage_type, record_type)
);

CREATE TABLE IF NOT EXISTS k8s_monitor.cost_sync (
    id        INT         PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    synced_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS k8s_monitor.cost_run_rate (
    sampled_at         TIMESTAMPTZ      PRIMARY KEY,
    effective_hourly   DOUBLE PRECISION NOT NULL,
    on_demand_hourly   DOUBLE PRECISION NOT NULL,
    covers_seconds     INT              NOT NULL
);
`

// BillingStore implements billing.Store on the time-series store's connection.
type BillingStore struct {
	db *sql.DB
}

// Billing returns the billing tables, creating them if needed.
func (s *Store) Billing(ctx context.Context) (*BillingStore, error) {
	if _, err := s.db.ExecContext(ctx, billingSchema); err != nil {
		return nil, fmt.Errorf("billing migrate: %w", err)
	}
	return &BillingStore{db: s.db}, nil
}

func (b *BillingStore) ReplaceDaily(ctx context.Context, start, end time.Time, items []billing.LineItem, syncedAt time.Time) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM k8s_monitor.cost_daily WHERE day >= $1 AND day < $2`, start, end); err != nil {
		return err
	}
	for _, item := range items {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO k8s_monitor.cost_daily (day, usage_type, record_type, amount, quantity, unit, currency)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (day, usage_type, record_type) DO UPDATE
SET amount = EXCLUDED.amount, quantity = EXCLUDED.quantity, unit = EXCLUDED.unit, currency = EXCLUDED.currency`,
			item.Day, item.UsageType, item.RecordType, item.Amount, item.Quantity, item.Unit, item.Currency); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO k8s_monitor.cost_sync (id, synced_at) VALUES (1, $1)
ON CONFLICT (id) DO UPDATE SET synced_at = EXCLUDED.synced_at`, syncedAt); err != nil {
		return err
	}
	return tx.Commit()
}

func (b *BillingStore) Daily(ctx context.Context, start, end time.Time) ([]billing.LineItem, error) {
	rows, err := b.db.QueryContext(ctx, `
SELECT day, usage_type, record_type, amount, quantity, unit, currency
FROM k8s_monitor.cost_daily
WHERE day >= $1 AND day < $2
ORDER BY day`, start, end)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var items []billing.LineItem
	for rows.Next() {
		var item billing.LineItem
		if err := rows.Scan(&item.Day, &item.UsageType, &item.RecordType, &item.Amount, &item.Quantity, &item.Unit, &item.Currency); err != nil {
			return nil, err
		}
		item.Day = item.Day.UTC()
		items = append(items, item)
	}
	return items, rows.Err()
}

// LastSync returns the zero time before the first sync.
func (b *BillingStore) LastSync(ctx context.Context) (time.Time, error) {
	var at time.Time
	err := b.db.QueryRowContext(ctx, `SELECT synced_at FROM k8s_monitor.cost_sync WHERE id = 1`).Scan(&at)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	return at.UTC(), err
}

func (b *BillingStore) AppendRunRate(ctx context.Context, at time.Time, effective, onDemand float64, seconds int) error {
	_, err := b.db.ExecContext(ctx, `
INSERT INTO k8s_monitor.cost_run_rate (sampled_at, effective_hourly, on_demand_hourly, covers_seconds)
VALUES ($1,$2,$3,$4)
ON CONFLICT (sampled_at) DO NOTHING`, at, effective, onDemand, seconds)
	return err
}

func (b *BillingStore) EstimatedByDay(ctx context.Context, start, end time.Time) (map[string]float64, error) {
	rows, err := b.db.QueryContext(ctx, `
SELECT to_char(sampled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'), SUM(effective_hourly * covers_seconds / 3600.0)
FROM k8s_monitor.cost_run_rate
WHERE sampled_at >= $1 AND sampled_at < $2
GROUP BY 1`, start, end)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]float64{}
	for rows.Next() {
		var day string
		var amount float64
		if err := rows.Scan(&day, &amount); err != nil {
			return nil, err
		}
		out[day] = amount
	}
	return out, rows.Err()
}

func (b *BillingStore) Prune(ctx context.Context, before time.Time) error {
	if _, err := b.db.ExecContext(ctx, `DELETE FROM k8s_monitor.cost_daily WHERE day < $1`, before); err != nil {
		return err
	}
	_, err := b.db.ExecContext(ctx, `DELETE FROM k8s_monitor.cost_run_rate WHERE sampled_at < $1`, before)
	return err
}

var _ billing.Store = (*BillingStore)(nil)
