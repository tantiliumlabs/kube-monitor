package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ochestra-tech/k8s-monitor/internal/ports/store"
)

// Needs a disposable Postgres: TEST_DATABASE_URL=postgres://... Skipped otherwise.
func TestMemoryBytesSurviveAndOldRowsStillRead(t *testing.T) {
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

	const cluster = "test-memory-bytes"
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	// A row from before the columns existed: they stay NULL.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO k8s_monitor.metric_snapshots (cluster_id, captured_at, mem_usage_pct) VALUES ($1, $2, 50)`, cluster, base); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, store.MetricPoint{
		ClusterID: cluster, Timestamp: base.Add(time.Minute), MemoryUsagePct: 60,
		MemoryUsedBytes: 6 << 30, MemoryAllocatableBytes: 10 << 30,
	}); err != nil {
		t.Fatal(err)
	}

	pts, err := s.QueryRange(ctx, cluster, base.Add(-time.Minute), base.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 2 || pts[0].MemoryUsedBytes != 0 || pts[1].MemoryUsedBytes != 6<<30 || pts[1].MemoryAllocatableBytes != 10<<30 {
		t.Fatalf("points: %+v", pts)
	}
	latest, err := s.Latest(ctx, cluster, 1)
	if err != nil || len(latest) != 1 || latest[0].MemoryUsedBytes != 6<<30 {
		t.Fatalf("latest: %v %+v", err, latest)
	}
}
