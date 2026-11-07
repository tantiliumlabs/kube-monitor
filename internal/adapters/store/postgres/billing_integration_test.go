package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ochestra-tech/k8s-monitor/internal/app/billing"
)

// Needs a disposable Postgres: TEST_DATABASE_URL=postgres://... Skipped otherwise.
func TestBillingStore(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, err := s.Billing(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Running the migration twice is what every restart does.
	if _, err := s.Billing(ctx); err != nil {
		t.Fatal(err)
	}

	d1 := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)
	d2 := d1.AddDate(0, 0, 1)
	end := d1.AddDate(0, 0, 2)
	at := d2.Add(5 * time.Hour)

	if last, err := b.LastSync(ctx); err != nil || !last.IsZero() {
		t.Fatalf("last sync before any: %v %v", last, err)
	}
	item := func(day time.Time, usage string, amount float64) billing.LineItem {
		return billing.LineItem{Day: day, UsageType: usage, RecordType: "Usage", Amount: amount, Quantity: 2, Unit: "Hrs", Currency: "USD"}
	}
	if err := b.ReplaceDaily(ctx, d1, end, []billing.LineItem{item(d1, "a", 1), item(d1, "gone", 9), item(d2, "a", 2)}, at); err != nil {
		t.Fatal(err)
	}
	// A later sync drops the line AWS removed and updates the rest.
	if err := b.ReplaceDaily(ctx, d1, end, []billing.LineItem{item(d1, "a", 1.5), item(d2, "a", 2)}, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	items, err := b.Daily(ctx, d1, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Amount != 1.5 || !items[0].Day.Equal(d1) || items[0].Unit != "Hrs" {
		t.Fatalf("items: %+v", items)
	}
	if last, err := b.LastSync(ctx); err != nil || !last.Equal(at.Add(time.Hour)) {
		t.Fatalf("last sync: %v %v", last, err)
	}

	// 12 five-minute samples at $0.60 an hour is one hour: $0.60.
	for i := 0; i < 12; i++ {
		if err := b.AppendRunRate(ctx, d1.Add(time.Duration(i)*5*time.Minute), 0.6, 1, 300); err != nil {
			t.Fatal(err)
		}
	}
	est, err := b.EstimatedByDay(ctx, d1, end)
	if err != nil {
		t.Fatal(err)
	}
	if got := est["2020-03-01"]; got < 0.5999 || got > 0.6001 {
		t.Fatalf("estimate %v", est)
	}

	if err := b.Prune(ctx, end); err != nil {
		t.Fatal(err)
	}
	if items, err := b.Daily(ctx, d1, end); err != nil || len(items) != 0 {
		t.Fatalf("after prune: %v %d", err, len(items))
	}
	if est, err := b.EstimatedByDay(ctx, d1, end); err != nil || len(est) != 0 {
		t.Fatalf("estimates after prune: %v %v", err, est)
	}
}
