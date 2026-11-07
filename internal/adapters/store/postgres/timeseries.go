// Package postgres provides a PostgreSQL-backed TimeSeriesStore.
// It is opt-in: the store is only instantiated when DATABASE_URL is set.
// The inmemory ring buffer remains the default.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/ochestra-tech/k8s-monitor/internal/ports/store"
)

const schema = `
CREATE SCHEMA IF NOT EXISTS k8s_monitor;

CREATE TABLE IF NOT EXISTS k8s_monitor.metric_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    cluster_id      TEXT        NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    health_score    INT         NOT NULL DEFAULT 0,
    cpu_usage_pct   FLOAT       NOT NULL DEFAULT 0,
    mem_usage_pct   FLOAT       NOT NULL DEFAULT 0,
    ready_nodes     INT         NOT NULL DEFAULT 0,
    total_nodes     INT         NOT NULL DEFAULT 0,
    total_pods      INT         NOT NULL DEFAULT 0,
    failed_pods     INT         NOT NULL DEFAULT 0,
    crash_loops     INT         NOT NULL DEFAULT 0,
    apiserver_ms    FLOAT       NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_cluster_time
    ON k8s_monitor.metric_snapshots (cluster_id, captured_at DESC);

ALTER TABLE k8s_monitor.metric_snapshots ADD COLUMN IF NOT EXISTS mem_used_bytes BIGINT;
ALTER TABLE k8s_monitor.metric_snapshots ADD COLUMN IF NOT EXISTS mem_allocatable_bytes BIGINT;
`

// Store is a Postgres-backed implementation of store.TimeSeriesStore.
type Store struct {
	db *sql.DB
}

// New opens a connection to the given DSN, runs the migration, and returns a Store.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("postgres migrate: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() error { return s.db.Close() }

// Append inserts a single metric snapshot.
func (s *Store) Append(ctx context.Context, pt store.MetricPoint) error {
	const q = `
INSERT INTO k8s_monitor.metric_snapshots
    (cluster_id, captured_at, health_score, cpu_usage_pct, mem_usage_pct,
     ready_nodes, total_nodes, total_pods, failed_pods, crash_loops, apiserver_ms,
     mem_used_bytes, mem_allocatable_bytes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`
	_, err := s.db.ExecContext(ctx, q,
		pt.ClusterID, pt.Timestamp,
		pt.HealthScore, pt.CPUUsagePct, pt.MemoryUsagePct,
		pt.ReadyNodeCount, pt.TotalNodeCount,
		pt.TotalPodCount, pt.FailedPodCount, pt.CrashLoopCount,
		pt.APIServerLatencyMs,
		pt.MemoryUsedBytes, pt.MemoryAllocatableBytes,
	)
	return err
}

// Latest returns the n most-recent snapshots for clusterID, oldest-first.
func (s *Store) Latest(ctx context.Context, clusterID string, n int) ([]store.MetricPoint, error) {
	limit := 1000
	if n > 0 {
		limit = n
	}
	const q = `
SELECT cluster_id, captured_at, health_score, cpu_usage_pct, mem_usage_pct,
       ready_nodes, total_nodes, total_pods, failed_pods, crash_loops, apiserver_ms,
       COALESCE(mem_used_bytes, 0), COALESCE(mem_allocatable_bytes, 0)
FROM (
    SELECT * FROM k8s_monitor.metric_snapshots
    WHERE cluster_id = $1
    ORDER BY captured_at DESC
    LIMIT $2
) sub
ORDER BY captured_at ASC`
	rows, err := s.db.QueryContext(ctx, q, clusterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// QueryRange returns snapshots within [start, end] for clusterID, oldest-first.
func (s *Store) QueryRange(ctx context.Context, clusterID string, start, end time.Time) ([]store.MetricPoint, error) {
	const q = `
SELECT cluster_id, captured_at, health_score, cpu_usage_pct, mem_usage_pct,
       ready_nodes, total_nodes, total_pods, failed_pods, crash_loops, apiserver_ms,
       COALESCE(mem_used_bytes, 0), COALESCE(mem_allocatable_bytes, 0)
FROM k8s_monitor.metric_snapshots
WHERE cluster_id = $1 AND captured_at >= $2 AND captured_at <= $3
ORDER BY captured_at ASC`
	rows, err := s.db.QueryContext(ctx, q, clusterID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func scanRows(rows *sql.Rows) ([]store.MetricPoint, error) {
	var pts []store.MetricPoint
	for rows.Next() {
		var pt store.MetricPoint
		if err := rows.Scan(
			&pt.ClusterID, &pt.Timestamp,
			&pt.HealthScore, &pt.CPUUsagePct, &pt.MemoryUsagePct,
			&pt.ReadyNodeCount, &pt.TotalNodeCount,
			&pt.TotalPodCount, &pt.FailedPodCount, &pt.CrashLoopCount,
			&pt.APIServerLatencyMs,
			&pt.MemoryUsedBytes, &pt.MemoryAllocatableBytes,
		); err != nil {
			return nil, err
		}
		pts = append(pts, pt)
	}
	return pts, rows.Err()
}
