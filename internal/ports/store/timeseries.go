package store

import (
	"context"
	"time"
)

// MetricPoint is a single snapshot of cluster-level metrics captured at a point in time.
type MetricPoint struct {
	Timestamp          time.Time `json:"timestamp"`
	ClusterID          string    `json:"cluster_id"`
	HealthScore        int       `json:"health_score"`
	CPUUsagePct        float64   `json:"cpu_usage_pct"`
	MemoryUsagePct     float64   `json:"memory_usage_pct"`
	ReadyNodeCount     int       `json:"ready_node_count"`
	TotalNodeCount     int       `json:"total_node_count"`
	TotalPodCount      int       `json:"total_pod_count"`
	FailedPodCount     int       `json:"failed_pod_count"`
	CrashLoopCount     int       `json:"crash_loop_count"`
	APIServerLatencyMs float64   `json:"apiserver_latency_ms"`
	// Memory at capture time. Zero on points recorded before these existed.
	MemoryUsedBytes        int64 `json:"memory_used_bytes,omitempty"`
	MemoryAllocatableBytes int64 `json:"memory_allocatable_bytes,omitempty"`
}

// TimeSeriesStore persists and queries MetricPoint snapshots.
// Implementations must be safe for concurrent use.
type TimeSeriesStore interface {
	// Append stores a single metric snapshot.
	Append(ctx context.Context, pt MetricPoint) error

	// Latest returns the n most-recent snapshots for the given clusterID,
	// ordered oldest-first. If n <= 0 all stored points are returned.
	Latest(ctx context.Context, clusterID string, n int) ([]MetricPoint, error)

	// QueryRange returns snapshots whose timestamp falls within [start, end].
	QueryRange(ctx context.Context, clusterID string, start, end time.Time) ([]MetricPoint, error)
}
