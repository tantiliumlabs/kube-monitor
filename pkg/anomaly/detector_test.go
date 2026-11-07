package anomaly

import (
	"testing"

	"github.com/ochestra-tech/k8s-monitor/internal/ports/store"
)

// history returns n points varying around the given values, then the latest point.
func history(n int, base store.MetricPoint, latest store.MetricPoint) []store.MetricPoint {
	pts := make([]store.MetricPoint, 0, n+1)
	for i := 0; i < n; i++ {
		p := base
		d := float64(i%2)*2 - 1 // -1, +1, ...
		p.CPUUsagePct += d
		p.MemoryUsagePct += d
		p.APIServerLatencyMs += d
		p.HealthScore += int(d)
		pts = append(pts, p)
	}
	return append(pts, latest)
}

func quiet() store.MetricPoint {
	return store.MetricPoint{HealthScore: 98, CPUUsagePct: 15, MemoryUsagePct: 65, APIServerLatencyMs: 6}
}

func metrics(evs []AnomalyEvent) map[string]AnomalyEvent {
	m := map[string]AnomalyEvent{}
	for _, e := range evs {
		m[e.Metric] = e
	}
	return m
}

func TestDetect(t *testing.T) {
	tests := []struct {
		name   string
		change func(*store.MetricPoint)
		want   string // metric that must fire; empty means nothing fires
	}{
		{"nothing changed", func(*store.MetricPoint) {}, ""},
		{"CPU unusual but harmless", func(p *store.MetricPoint) { p.CPUUsagePct = 30 }, ""},
		{"CPU falling", func(p *store.MetricPoint) { p.CPUUsagePct = 1 }, ""},
		{"CPU really high", func(p *store.MetricPoint) { p.CPUUsagePct = 90 }, "cpu_usage_pct"},
		{"memory unusual but harmless", func(p *store.MetricPoint) { p.MemoryUsagePct = 80 }, ""},
		{"memory really high", func(p *store.MetricPoint) { p.MemoryUsagePct = 95 }, "memory_usage_pct"},
		{"latency unusual but harmless", func(p *store.MetricPoint) { p.APIServerLatencyMs = 90 }, ""},
		{"latency really high", func(p *store.MetricPoint) { p.APIServerLatencyMs = 3000 }, "apiserver_latency_ms"},
		{"health dips but stays fine", func(p *store.MetricPoint) { p.HealthScore = 90 }, ""},
		{"health collapses", func(p *store.MetricPoint) { p.HealthScore = 40 }, "health_score"},
		{"first crash on a healthy cluster", func(p *store.MetricPoint) { p.CrashLoopCount = 1 }, "crash_loop_count"},
		{"first failed pod on a healthy cluster", func(p *store.MetricPoint) { p.FailedPodCount = 1 }, "failed_pod_count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			latest := quiet()
			tt.change(&latest)
			got := metrics(NewDetector(2.5, 10).Detect(history(30, quiet(), latest)))
			if tt.want == "" {
				if len(got) != 0 {
					t.Fatalf("fired %v, want nothing", got)
				}
				return
			}
			if _, ok := got[tt.want]; !ok || len(got) != 1 {
				t.Fatalf("fired %v, want only %s", got, tt.want)
			}
		})
	}
}

func TestDetect_CountSeverity(t *testing.T) {
	for _, tt := range []struct {
		count int
		want  Severity
	}{{1, SeverityMedium}, {2, SeverityCritical}} {
		latest := quiet()
		latest.CrashLoopCount = tt.count
		got := metrics(NewDetector(2.5, 10).Detect(history(30, quiet(), latest)))
		if got["crash_loop_count"].Severity != tt.want {
			t.Errorf("count %d: severity %q, want %q", tt.count, got["crash_loop_count"].Severity, tt.want)
		}
	}
}

func TestDetect_ResolvedCountDoesNotFire(t *testing.T) {
	base := quiet()
	base.CrashLoopCount = 2
	got := NewDetector(2.5, 10).Detect(history(30, base, quiet()))
	if len(got) != 0 {
		t.Fatalf("fired %v when crashes cleared", metrics(got))
	}
}
