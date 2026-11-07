package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	anomalypub "github.com/ochestra-tech/k8s-monitor/internal/adapters/anomaly"
	cacheadapter "github.com/ochestra-tech/k8s-monitor/internal/adapters/cache"
	awsp "github.com/ochestra-tech/k8s-monitor/internal/adapters/pricing/aws"
	azurep "github.com/ochestra-tech/k8s-monitor/internal/adapters/pricing/azure"
	gcpp "github.com/ochestra-tech/k8s-monitor/internal/adapters/pricing/gcp"
	staticp "github.com/ochestra-tech/k8s-monitor/internal/adapters/pricing/static"
	reportingadapter "github.com/ochestra-tech/k8s-monitor/internal/adapters/reporting"
	storeimpl "github.com/ochestra-tech/k8s-monitor/internal/adapters/store/inmemory"
	pgstore "github.com/ochestra-tech/k8s-monitor/internal/adapters/store/postgres"
	apppricing "github.com/ochestra-tech/k8s-monitor/internal/app/pricing"
	appreporting "github.com/ochestra-tech/k8s-monitor/internal/app/reporting"
	domainpricing "github.com/ochestra-tech/k8s-monitor/internal/domain/pricing"
	portspricing "github.com/ochestra-tech/k8s-monitor/internal/ports/pricing"
	storeport "github.com/ochestra-tech/k8s-monitor/internal/ports/store"
	"github.com/ochestra-tech/k8s-monitor/pkg/anomaly"
	"github.com/ochestra-tech/k8s-monitor/pkg/cost"
	"github.com/ochestra-tech/k8s-monitor/pkg/health"
	"github.com/ochestra-tech/k8s-monitor/pkg/optimizer"
	"github.com/ochestra-tech/k8s-monitor/pkg/reports"
)

const defaultPricingConfigPath = "configs/pricing-config.json"

var (
	reportRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "k8s_monitor_report_duration_seconds",
			Help:    "Duration of report generation by type and status.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"report_type", "status"},
	)

	reportRunTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "k8s_monitor_report_total",
			Help: "Total number of report runs by type and status.",
		},
		[]string{"report_type", "status"},
	)

	reportLastSuccessTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "k8s_monitor_report_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful report run by type.",
		},
		[]string{"report_type"},
	)

	podPhaseTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "k8s_monitor_pods_phase_total",
			Help: "Number of pods by phase across the cluster.",
		},
		[]string{"phase"},
	)

	namespacePodPhaseTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "k8s_monitor_namespace_pods_phase_total",
			Help: "Number of pods by phase for top namespaces (others aggregated).",
		},
		[]string{"namespace", "phase"},
	)

	namespacePodTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "k8s_monitor_namespace_pods_total",
			Help: "Number of pods per top namespace (others aggregated).",
		},
		[]string{"namespace"},
	)
)

func init() {
	prometheus.MustRegister(reportRunDuration)
	prometheus.MustRegister(reportRunTotal)
	prometheus.MustRegister(reportLastSuccessTimestamp)
	prometheus.MustRegister(podPhaseTotal)
	prometheus.MustRegister(namespacePodPhaseTotal)
	prometheus.MustRegister(namespacePodTotal)
}

// Config holds the application configuration
type Config struct {
	KubeConfigPath           string
	PricingConfigPath        string
	ReportFormat             reports.ReportFormat
	ReportPath               string
	CheckInterval            time.Duration
	MetricsPort              int
	MetricsReadHeaderTimeout time.Duration
	RequestTimeout           time.Duration
	ShutdownTimeout          time.Duration
	KubeQPS                  float32
	KubeBurst                int
	EnableDetailedMetrics    bool
	MetricsTopNamespaces     int
	DetailedMetricsInterval  time.Duration
	PricingDebug             bool
	PricingDebugLogPath      string
	OneShot                  bool
	ReportType               string // "health", "cost", "combined"
	// Anomaly detection / time-series options
	ClusterID          string
	AnomalyWindowSize  int
	AnomalyZThreshold  float64
	RingBufferCapacity int
	// Optional Postgres-backed time-series store (opt-in via DATABASE_URL env var).
	DatabaseURL string
}

func main() {
	config := parseFlags()
	validateConfig(config)

	// Default cluster ID to hostname when not explicitly set.
	if config.ClusterID == "" {
		if h, err := os.Hostname(); err == nil {
			config.ClusterID = h
		} else {
			config.ClusterID = "default"
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clientset, metricsClient := initKubernetesClients(config.KubeConfigPath, config.KubeQPS, config.KubeBurst)
	pricing, err := resolvePricing(ctx, config.PricingConfigPath, config.PricingDebug, config.PricingDebugLogPath)
	if err != nil {
		// KUB-114: pricing-config.json's source is now "aws" only (no
		// static/auto fallback), by design -- but that means a transient
		// AWS API issue (rate limit, brief network blip, credential
		// expiry) must not take down cluster health/anomaly monitoring
		// along with it. Degrade to a nil pricing map instead of exiting:
		// GetNodeCosts/GetPodCosts only ever read from this map (a nil map
		// read is a safe zero-value lookup, never a panic), so cost
		// figures come back honestly zero/unavailable rather than the
		// whole process going down over a pricing-only failure.
		log.Printf("[pricing] resolution failed, cost data will be unavailable until this recovers: %v", err)
		pricing = nil
	}

	// ── Time-series store + anomaly detection ──────────────────────────────────
	// Default: in-memory ring buffer. Opt-in to Postgres when DATABASE_URL is set.
	var tsStore storeport.TimeSeriesStore = storeimpl.NewRingBuffer(config.RingBufferCapacity)
	if config.DatabaseURL != "" {
		pg, err := pgstore.New(config.DatabaseURL)
		if err != nil {
			log.Printf("[store] postgres unavailable (%v), falling back to ring buffer", err)
		} else {
			log.Printf("[store] using postgres time-series store")
			defer pg.Close()
			tsStore = pg
		}
	}
	detector := anomaly.NewDetector(config.AnomalyZThreshold, config.AnomalyWindowSize)
	publisher := anomalypub.NewPublisher() // nil when RABBITMQ_URL is unset
	defer publisher.Close()

	if config.OneShot {
		if err := runWithTimeout(ctx, config.RequestTimeout, func(runCtx context.Context) error {
			return executeReportCycle(runCtx, clientset, metricsClient, pricing, config)
		}); err != nil {
			log.Fatalf("Failed to generate report: %v", err)
		}
		captureSnapshot(ctx, clientset, metricsClient, config.ClusterID, tsStore, detector, publisher)
		if config.EnableDetailedMetrics {
			if err := updateDetailedMetrics(ctx, clientset, config.MetricsTopNamespaces); err != nil {
				log.Printf("Failed to update detailed metrics: %v", err)
			}
		}
		return
	}

	metricsServer := startMetricsServer(config, clientset, metricsClient, pricing, tsStore)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Metrics server shutdown error: %v", err)
		}
	}()

	ticker := time.NewTicker(config.CheckInterval)
	defer ticker.Stop()

	if err := runWithTimeout(ctx, config.RequestTimeout, func(runCtx context.Context) error {
		return executeReportCycle(runCtx, clientset, metricsClient, pricing, config)
	}); err != nil {
		log.Printf("Failed to generate initial report: %v", err)
	}
	captureSnapshot(ctx, clientset, metricsClient, config.ClusterID, tsStore, detector, publisher)

	if config.EnableDetailedMetrics {
		if err := updateDetailedMetrics(ctx, clientset, config.MetricsTopNamespaces); err != nil {
			log.Printf("Failed to update detailed metrics: %v", err)
		}
	}

	var detailedTicker *time.Ticker
	var detailedC <-chan time.Time
	if config.EnableDetailedMetrics {
		interval := config.DetailedMetricsInterval
		if interval <= 0 {
			interval = config.CheckInterval
		}
		detailedTicker = time.NewTicker(interval)
		detailedC = detailedTicker.C
		defer detailedTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Shutting down: %v", ctx.Err())
			return
		case <-ticker.C:
			if err := runWithTimeout(ctx, config.RequestTimeout, func(runCtx context.Context) error {
				return executeReportCycle(runCtx, clientset, metricsClient, pricing, config)
			}); err != nil {
				log.Printf("Failed to generate report: %v", err)
			}
			captureSnapshot(ctx, clientset, metricsClient, config.ClusterID, tsStore, detector, publisher)
		case <-detailedC:
			if err := updateDetailedMetrics(ctx, clientset, config.MetricsTopNamespaces); err != nil {
				log.Printf("Failed to update detailed metrics: %v", err)
			}
		}
	}
}

func parseFlags() *Config {
	config := &Config{}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to get user home directory: %v", err)
	}
	defaultKubeConfig := filepath.Join(homeDir, ".kube", "config")

	flag.StringVar(&config.KubeConfigPath, "kubeconfig", defaultKubeConfig, "Path to kubeconfig file")
	flag.StringVar(&config.PricingConfigPath, "pricing-config", defaultPricingConfigPath, "Path to pricing configuration file")
	flag.StringVar((*string)(&config.ReportFormat), "format", string(reports.FormatText), "Report format (text, json, html)")
	flag.StringVar(&config.ReportPath, "output", "", "Output file path (empty for stdout)")
	flag.DurationVar(&config.CheckInterval, "interval", 60*time.Second, "Check interval for continuous monitoring")
	flag.IntVar(&config.MetricsPort, "metrics-port", 8085, "Prometheus metrics port")
	flag.DurationVar(&config.MetricsReadHeaderTimeout, "metrics-read-header-timeout", 5*time.Second, "Read header timeout for metrics server")
	flag.DurationVar(&config.RequestTimeout, "request-timeout", 90*time.Second, "Timeout for a single report cycle")
	flag.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "Graceful shutdown timeout")
	kubeQPS := 20.0
	flag.Float64Var(&kubeQPS, "kube-qps", 20, "Kubernetes client QPS (rate limiter)")
	flag.IntVar(&config.KubeBurst, "kube-burst", 40, "Kubernetes client burst (rate limiter)")
	flag.BoolVar(&config.EnableDetailedMetrics, "enable-detailed-metrics", false, "Enable namespace/phase metrics (top namespaces only)")
	flag.IntVar(&config.MetricsTopNamespaces, "metrics-top-namespaces", 10, "Max namespaces to export metrics for (others aggregated)")
	flag.DurationVar(&config.DetailedMetricsInterval, "detailed-metrics-interval", 5*time.Minute, "Interval for detailed metrics collection")
	flag.BoolVar(&config.PricingDebug, "pricing-debug", false, "Enable debug logging for pricing providers")
	flag.StringVar(&config.PricingDebugLogPath, "pricing-debug-log", "pricing-debug.log", "File path for pricing debug logs")
	flag.BoolVar(&config.OneShot, "one-shot", false, "Run once and exit")
	flag.StringVar(&config.ReportType, "type", "combined", "Report type (health, cost, combined)")
	flag.StringVar(&config.ClusterID, "cluster-id", "", "Cluster identifier stamped on anomaly events (defaults to hostname)")
	flag.IntVar(&config.AnomalyWindowSize, "anomaly-window", 60, "Number of recent snapshots used for Z-score anomaly detection")
	flag.Float64Var(&config.AnomalyZThreshold, "anomaly-z-threshold", 2.5, "Z-score threshold above which a metric is flagged as anomalous")
	flag.IntVar(&config.RingBufferCapacity, "ring-buffer-capacity", 1000, "Max metric snapshots held in memory per cluster")
	// Postgres is opt-in — read from env so it doesn't appear in -help output.
	config.DatabaseURL = os.Getenv("DATABASE_URL")

	flag.Parse()
	config.KubeQPS = float32(kubeQPS)
	return config
}

// validateConfig checks that flag values are within safe bounds and logs fatal
// errors before any Kubernetes client is created.
func validateConfig(config *Config) {
	var errs []string

	if config.MetricsPort < 1 || config.MetricsPort > 65535 {
		errs = append(errs, fmt.Sprintf("metrics-port %d is not in valid range 1–65535", config.MetricsPort))
	}
	if config.CheckInterval < time.Second {
		errs = append(errs, fmt.Sprintf("interval %v is too short (minimum 1s)", config.CheckInterval))
	}
	if config.RequestTimeout < time.Second {
		errs = append(errs, fmt.Sprintf("request-timeout %v is too short (minimum 1s)", config.RequestTimeout))
	}
	if config.ShutdownTimeout < time.Second {
		errs = append(errs, fmt.Sprintf("shutdown-timeout %v is too short (minimum 1s)", config.ShutdownTimeout))
	}
	if config.KubeQPS <= 0 {
		errs = append(errs, fmt.Sprintf("kube-qps %.1f must be positive", config.KubeQPS))
	}
	if config.KubeBurst <= 0 {
		errs = append(errs, fmt.Sprintf("kube-burst %d must be positive", config.KubeBurst))
	}
	if config.MetricsTopNamespaces < 1 {
		errs = append(errs, fmt.Sprintf("metrics-top-namespaces %d must be at least 1", config.MetricsTopNamespaces))
	}
	validTypes := map[string]bool{"health": true, "cost": true, "combined": true}
	if !validTypes[config.ReportType] {
		errs = append(errs, fmt.Sprintf("type %q is not valid; choose health, cost, or combined", config.ReportType))
	}

	if len(errs) > 0 {
		for _, e := range errs {
			log.Printf("config error: %s", e)
		}
		log.Fatalf("invalid configuration; exiting")
	}
}

func startMetricsServer(
	config *Config,
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	pricing map[string]cost.ResourcePricing,
	tsStore storeport.TimeSeriesStore,
) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.Handle("/api/health", withCORS(reportHandler("health", clientset, metricsClient, pricing, config.RequestTimeout)))
	mux.Handle("/api/cost", withCORS(reportHandler("cost", clientset, metricsClient, pricing, config.RequestTimeout)))
	mux.Handle("/api/combined", withCORS(reportHandler("combined", clientset, metricsClient, pricing, config.RequestTimeout)))
	mux.Handle("/api/optimizer", withCORS(optimizerHandler(clientset, metricsClient, pricing, config.RequestTimeout)))
	mux.Handle("/api/cost/namespace", withCORS(namespaceCostHandler(clientset, metricsClient, pricing, config.RequestTimeout)))
	mux.Handle("/api/health/namespace", withCORS(namespaceHealthHandler(clientset, metricsClient, config.RequestTimeout)))
	mux.Handle("/api/history", withCORS(historyHandler(tsStore, config.ClusterID, config.RequestTimeout)))
	mux.Handle("/api/anomalies", withCORS(anomaliesHandler(tsStore, config.ClusterID, config.AnomalyWindowSize, config.AnomalyZThreshold, config.RequestTimeout)))

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.MetricsPort),
		Handler:           mux,
		ReadHeaderTimeout: config.MetricsReadHeaderTimeout,
	}

	go func() {
		log.Printf("Starting metrics server on port %d", config.MetricsPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	return server
}

// captureSnapshot calls the health checker, builds a MetricPoint and appends it
// to the time-series store, then runs anomaly detection and optionally publishes.
func captureSnapshot(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	clusterID string,
	tsStore storeport.TimeSeriesStore,
	detector *anomaly.Detector,
	publisher *anomalypub.Publisher,
) {
	snapCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ch, err := health.GetClusterHealth(snapCtx, clientset, metricsClient)
	if err != nil {
		log.Printf("[snapshot] health check failed: %v", err)
		return
	}

	pt := storeport.MetricPoint{
		Timestamp:          ch.Timestamp,
		ClusterID:          clusterID,
		HealthScore:        ch.HealthScore,
		CPUUsagePct:        ch.ResourceUsage.ClusterCPUUsage,
		MemoryUsagePct:     ch.ResourceUsage.ClusterMemoryUsage,
		ReadyNodeCount:     ch.NodeStatus.ReadyNodes,
		TotalNodeCount:     ch.NodeStatus.TotalNodes,
		TotalPodCount:      ch.PodStatus.TotalPods,
		FailedPodCount:     ch.PodStatus.FailedPods,
		CrashLoopCount:     len(ch.PodStatus.CrashLoopingPods),
		APIServerLatencyMs: ch.ControlPlaneStatus.APIServerLatency,

		MemoryUsedBytes:        ch.ResourceUsage.ClusterMemoryUsedBytes,
		MemoryAllocatableBytes: ch.ResourceUsage.ClusterMemoryAllocatableBytes,
	}

	if err := tsStore.Append(ctx, pt); err != nil {
		log.Printf("[snapshot] store append failed: %v", err)
		return
	}

	// Run anomaly detection over the last AnomalyWindowSize+1 points.
	recent, err := tsStore.Latest(ctx, clusterID, detector.MinWindowSize+1)
	if err != nil || len(recent) < detector.MinWindowSize {
		return
	}
	events := detector.Detect(recent)
	if len(events) > 0 {
		publisher.Publish(ctx, events)
	}
}

// historyHandler returns the stored metric snapshots for a cluster.
// Query params: cluster_id (overrides default), start (RFC3339), end (RFC3339), limit (int).
func historyHandler(tsStore storeport.TimeSeriesStore, defaultClusterID string, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		q := r.URL.Query()
		clusterID := q.Get("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		var points []storeport.MetricPoint
		var err error

		startStr := q.Get("start")
		endStr := q.Get("end")
		if startStr != "" && endStr != "" {
			start, serr := time.Parse(time.RFC3339, startStr)
			end, eerr := time.Parse(time.RFC3339, endStr)
			if serr != nil || eerr != nil {
				http.Error(w, "invalid start/end; use RFC3339 format", http.StatusBadRequest)
				return
			}
			points, err = tsStore.QueryRange(ctx, clusterID, start, end)
		} else {
			limit := 100
			if v := q.Get("limit"); v != "" {
				if parsed, perr := parseInt(v); perr == nil && parsed > 0 {
					limit = parsed
				}
			}
			points, err = tsStore.Latest(ctx, clusterID, limit)
		}
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"cluster_id": clusterID,
			"count":      len(points),
			"points":     points,
		})
	}
}

// anomaliesHandler runs the anomaly detector over the stored window and returns current anomalies.
func anomaliesHandler(tsStore storeport.TimeSeriesStore, defaultClusterID string, windowSize int, zThreshold float64, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		q := r.URL.Query()
		clusterID := q.Get("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		detector := anomaly.NewDetector(zThreshold, windowSize)
		points, err := tsStore.Latest(ctx, clusterID, windowSize+1)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		events := detector.Detect(points)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"cluster_id": clusterID,
			"count":      len(events),
			"anomalies":  events,
		})
	}
}

func parseInt(s string) (int, error) {
	var v int
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func optimizerHandler(
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	pricing map[string]cost.ResourcePricing,
	timeout time.Duration,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		q := r.URL.Query()
		opts := optimizer.Options{
			Namespace: q.Get("namespace"),
			Node:      q.Get("node"),
			Pod:       q.Get("pod"),
			View:      optimizer.ReportView(q.Get("view")),
		}

		// Toggles (default true)
		opts.IncludeIdle = q.Get("includeIdle") != "false"
		opts.IncludeOverprovisioned = q.Get("includeOverprovisioned") != "false"
		opts.IncludeCleanup = q.Get("includeCleanup") != "false"
		opts.DryRunCleanup = q.Get("dryRunCleanup") != "false"
		opts.IncludeNetwork = q.Get("includeNetwork") != "false"
		opts.IncludeStorage = q.Get("includeStorage") != "false"

		// Thresholds
		if v := q.Get("idleCpuPercent"); v != "" {
			if parsed, err := parseFloat(v); err == nil {
				opts.IdleCPUPercent = parsed
			}
		}
		if v := q.Get("idleMemoryPercent"); v != "" {
			if parsed, err := parseFloat(v); err == nil {
				opts.IdleMemoryPercent = parsed
			}
		}
		if v := q.Get("overprovisionedFactor"); v != "" {
			if parsed, err := parseFloat(v); err == nil {
				opts.OverprovisionedFactor = parsed
			}
		}
		if v := q.Get("headroomFactor"); v != "" {
			if parsed, err := parseFloat(v); err == nil {
				opts.HeadroomFactor = parsed
			}
		}
		if v := q.Get("networkIdleBytesPerSec"); v != "" {
			if parsed, err := parseFloat(v); err == nil {
				opts.NetworkIdleBytesPerSec = parsed
			}
		}
		if v := q.Get("storageLowUtilPercent"); v != "" {
			if parsed, err := parseFloat(v); err == nil {
				opts.StorageLowUtilPercent = parsed
			}
		}

		opt := optimizer.NewResourceOptimizerWithPrometheus(clientset, metricsClient, os.Getenv("PROMETHEUS_URL"))
		report, err := opt.GenerateOptimizationReport(ctx, pricing, opts)
		if err != nil {
			status := http.StatusInternalServerError
			payload := map[string]string{
				"error":   "failed to generate optimizer report",
				"details": err.Error(),
			}
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
				payload["suggestion"] = "Increase --request-timeout or filter by namespace/node to reduce the amount of data scanned."
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(payload)
			return
		}

		// Node-level Details always carry the node's true cluster-wide
		// capacity/usage (nodeCapacities/buildNodeUsageMap in analysis.go
		// aren't namespace-filtered even when opts.Namespace is), regardless
		// of how the pod-driven aggregation that decides idle/overprovisioned
		// status was scoped. Strip them whenever a namespace filter is set --
		// this endpoint is reachable by a tenant caller scoped to one
		// namespace, and cluster-wide node capacity/utilization isn't theirs
		// to see.
		if opts.Namespace != "" {
			filtered := report.Details[:0]
			for _, d := range report.Details {
				if d.Level != "node" {
					filtered = append(filtered, d)
				}
			}
			report.Details = filtered
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(report); err != nil {
			log.Printf("Failed to write optimizer report response: %v", err)
		}
	}
}

func parseFloat(s string) (float64, error) {
	// Keep dependencies minimal; use fmt for parsing.
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	return v, err
}

// withCORS enforces an origin allowlist read from the CORS_ALLOWED_ORIGINS
// environment variable (comma-separated list).  If the variable is empty, no
// cross-origin requests are allowed.  Only the exact request origin is echoed
// back — the wildcard "*" is never used.
func withCORS(next http.Handler) http.Handler {
	raw := strings.TrimSpace(os.Getenv("CORS_ALLOWED_ORIGINS"))
	allowed := make(map[string]struct{})
	for _, o := range strings.Split(raw, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = struct{}{}
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if _, ok := allowed[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
				w.Header().Set("Vary", "Origin")
			}
		}

		if r.Method == http.MethodOptions {
			if _, ok := allowed[origin]; ok {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusForbidden)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}

func reportHandler(
	reportType string,
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	pricing map[string]cost.ResourcePricing,
	timeout time.Duration,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		var buf bytes.Buffer
		generator := reportingadapter.NewGenerator(clientset, metricsClient, reports.FormatJSON, &buf)
		service := appreporting.NewService(generator)
		if err := service.Generate(ctx, reportType, pricing); err != nil {
			status := http.StatusInternalServerError
			payload := map[string]string{
				"error":   fmt.Sprintf("failed to generate %s report", reportType),
				"details": err.Error(),
			}
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
				payload["suggestion"] = "Increase --request-timeout or reduce report scope to avoid API throttling."
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			if encodeErr := json.NewEncoder(w).Encode(payload); encodeErr != nil {
				log.Printf("Failed to write %s error response: %v", reportType, encodeErr)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(buf.Bytes()); err != nil {
			log.Printf("Failed to write %s report response: %v", reportType, err)
		}
	}
}

// namespaceCostHandler serves namespace/pod cost data pre-filtered to one
// namespace -- a separate, standalone handler (mirroring optimizerHandler's
// pattern) rather than adding a namespace branch to reportHandler/
// Service.Generate, so the existing /api/cost (used by the admin dashboard)
// is untouched. Deliberately omits node costs: nodes are shared host
// infrastructure with no per-namespace attribution, so returning them here
// would either leak other tenants' cost data or require inventing a
// pro-rating scheme that doesn't exist today (see the "Cost" tier-1 note in
// the tenant-scoping plan). `namespace` is required -- this endpoint is not
// a scoped-down /api/cost, it's a single-namespace lookup.
func namespaceCostHandler(
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	pricing map[string]cost.ResourcePricing,
	timeout time.Duration,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		namespace := r.URL.Query().Get("namespace")
		if namespace == "" {
			http.Error(w, `{"error":"namespace query param is required"}`, http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		podCosts, err := cost.GetPodCosts(ctx, clientset, metricsClient, pricing)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to compute namespace cost", "details": err.Error()})
			return
		}

		filtered := make([]cost.PodCostData, 0, len(podCosts))
		for _, p := range podCosts {
			if p.Namespace == namespace {
				filtered = append(filtered, p)
			}
		}
		namespaceCosts := cost.GetNamespaceCosts(filtered)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"pods":       filtered,
			"namespaces": namespaceCosts,
			"timestamp":  time.Now().UTC(),
		})
	}
}

// namespacePodHealth is a namespace-scoped subset of pkg/health's
// PodHealthStatus, computed directly here rather than reusing
// health.checkPodHealth (unexported, and scoped to all-namespaces by
// design) -- same "standalone handler mirrors optimizerHandler's pattern"
// choice as namespaceCostHandler, to avoid touching the cluster-wide health
// path at all.
type namespacePodHealth struct {
	TotalPods        int      `json:"totalPods"`
	RunningPods      int      `json:"runningPods"`
	PendingPods      int      `json:"pendingPods"`
	FailedPods       int      `json:"failedPods"`
	RestartingPods   int      `json:"restartingPods"`
	CrashLoopingPods []string `json:"crashLoopingPods"`
}

// namespaceHealthHandler serves live, point-in-time pod health + resource
// usage scoped to one namespace -- the namespace-scoped counterpart to
// namespaceCostHandler, filling the gap found while wiring
// observability-agent-srv for per-tenant AI Operations: k8s-monitor's
// existing /api/health is cluster-wide only (checkResourceUsage sums every
// node; checkNamespaceHealth exists but its ResourceUsage is explicitly
// left empty and its HealthScore field is dead, always 100). `namespace` is
// required, same convention as /api/cost/namespace.
func namespaceHealthHandler(
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	timeout time.Duration,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		namespace := r.URL.Query().Get("namespace")
		if namespace == "" {
			http.Error(w, `{"error":"namespace query param is required"}`, http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to compute namespace health", "details": err.Error()})
			return
		}

		podHealth := namespacePodHealth{CrashLoopingPods: []string{}}
		const restartRecentWindow = 15 * time.Minute
		for _, pod := range pods.Items {
			podHealth.TotalPods++
			switch pod.Status.Phase {
			case corev1.PodRunning:
				podHealth.RunningPods++
			case corev1.PodPending:
				podHealth.PendingPods++
			case corev1.PodFailed:
				podHealth.FailedPods++
			}

			restartedRecently := false
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.RestartCount > 0 {
					if cs.LastTerminationState.Terminated != nil &&
						time.Since(cs.LastTerminationState.Terminated.FinishedAt.Time) <= restartRecentWindow {
						restartedRecently = true
					}
					if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
						restartedRecently = true
						podHealth.CrashLoopingPods = append(podHealth.CrashLoopingPods, pod.Name)
					}
				}
			}
			if restartedRecently {
				podHealth.RestartingPods++
			}
		}

		// Live CPU/Mem usage (point-in-time, no history -- same metrics.k8s.io
		// source as /api/cost/namespace and the frontend's own metrics-pods
		// passthrough) rather than the cluster-wide %s checkResourceUsage
		// computes.
		var cpuMilli, memBytes int64
		if podMetrics, err := metricsClient.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{}); err == nil {
			for _, pm := range podMetrics.Items {
				for _, c := range pm.Containers {
					cpuMilli += c.Usage.Cpu().MilliValue()
					memBytes += c.Usage.Memory().Value()
				}
			}
		}

		// A simple, distinctly-labeled approximation -- k8s-monitor's own
		// per-namespace HealthScore field (checkNamespaceHealth) is dead
		// (always 100), there's no real formula to reuse from the cluster-wide
		// score either since node/control-plane inputs don't apply to one
		// namespace.
		var healthScore int
		if podHealth.TotalPods == 0 {
			healthScore = 100
		} else {
			runningPct := float64(podHealth.RunningPods) / float64(podHealth.TotalPods) * 100
			penalty := float64(podHealth.RestartingPods)*10 + float64(podHealth.FailedPods)*15
			healthScore = int(runningPct - penalty)
			if healthScore < 0 {
				healthScore = 0
			}
			if healthScore > 100 {
				healthScore = 100
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"namespace":   namespace,
			"healthScore": healthScore,
			"podStatus":   podHealth,
			"cpuMilli":    cpuMilli,
			"memoryBytes": memBytes,
			"timestamp":   time.Now().UTC(),
		})
	}
}

func runWithTimeout(parentCtx context.Context, timeout time.Duration, run func(context.Context) error) error {
	if timeout <= 0 {
		return run(parentCtx)
	}
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()
	return run(ctx)
}

func initKubernetesClients(kubeConfigPath string, qps float32, burst int) (*kubernetes.Clientset, *metricsv.Clientset) {
	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		if err != nil {
			log.Fatalf("Failed to create Kubernetes config: %v", err)
		}
	}

	if qps > 0 {
		config.QPS = qps
	}
	if burst > 0 {
		config.Burst = burst
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	metricsClient, err := metricsv.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Metrics client: %v", err)
	}

	return clientset, metricsClient
}

func resolvePricing(ctx context.Context, configPath string, debug bool, debugLogPath string) (map[string]cost.ResourcePricing, error) {
	configData, err := apppricing.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	configData = applyProviderEnvOverrides(configData)

	var debugLogger *log.Logger
	var debugFile *os.File
	if debug {
		path := strings.TrimSpace(debugLogPath)
		if path == "" {
			path = "pricing-debug.log"
		}
		debugFile, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, fmt.Errorf("open pricing debug log: %w", err)
		}
		debugLogger = log.New(debugFile, "", log.LstdFlags|log.LUTC)
	}
	if debugFile != nil {
		defer debugFile.Close()
	}

	providers := make([]portspricing.Provider, 0)
	staticProvider := staticp.New(configData.InstanceTypes, configData.Providers.AWS.Region, configData.Providers.AWS.Currency)
	providers = append(providers, staticProvider)

	azureProvider := azurep.New(nil, debug, debugLogger)
	providers = append(providers, azureProvider)

	if configData.Providers.GCP.APIKey != "" {
		gcpProvider := gcpp.New(nil, configData.Providers.GCP.APIKey, debug, debugLogger)
		providers = append(providers, gcpProvider)
	}

	awsProvider, err := awsp.New(ctx, debug, debugLogger)
	if err == nil {
		providers = append(providers, awsProvider)
	}

	cacheStore := cacheadapter.NewFromEnv()
	service := apppricing.NewService(providers, cacheStore)
	return service.Resolve(ctx, configData)
}

func applyProviderEnvOverrides(cfg domainpricing.Config) domainpricing.Config {
	if value := os.Getenv("K8S_MONITOR_GCP_API_KEY"); value != "" {
		cfg.Providers.GCP.APIKey = value
	}
	return cfg
}

func generateReport(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	pricing map[string]cost.ResourcePricing,
	config *Config,
) error {
	var output io.Writer = os.Stdout
	if config.ReportPath != "" {
		file, err := os.Create(config.ReportPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer file.Close()
		output = file
	}

	generator := reportingadapter.NewGenerator(clientset, metricsClient, config.ReportFormat, output)
	service := appreporting.NewService(generator)
	return service.Generate(ctx, config.ReportType, pricing)
}

func executeReportCycle(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
	pricing map[string]cost.ResourcePricing,
	config *Config,
) error {
	start := time.Now()
	status := "success"
	err := generateReport(ctx, clientset, metricsClient, pricing, config)
	if err != nil {
		status = "error"
	}

	reportRunDuration.WithLabelValues(config.ReportType, status).Observe(time.Since(start).Seconds())
	reportRunTotal.WithLabelValues(config.ReportType, status).Inc()
	if status == "success" {
		reportLastSuccessTimestamp.WithLabelValues(config.ReportType).Set(float64(time.Now().Unix()))
	}

	return err
}

func updateDetailedMetrics(ctx context.Context, clientset *kubernetes.Clientset, topNamespaces int) error {
	if topNamespaces <= 0 {
		topNamespaces = 10
	}

	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list pods for metrics: %w", err)
	}

	phaseTotals := map[string]float64{}
	perNamespace := map[string]map[string]float64{}
	perNamespaceTotal := map[string]float64{}

	for _, pod := range pods.Items {
		phase := string(pod.Status.Phase)
		if phase == "" {
			phase = "Unknown"
		}
		phaseTotals[phase]++

		ns := pod.Namespace
		if _, ok := perNamespace[ns]; !ok {
			perNamespace[ns] = map[string]float64{}
		}
		perNamespace[ns][phase]++
		perNamespaceTotal[ns]++
	}

	for phase, count := range phaseTotals {
		podPhaseTotal.WithLabelValues(phase).Set(count)
	}

	topList := topNamespaceList(perNamespaceTotal, topNamespaces)
	allowed := map[string]struct{}{}
	for _, ns := range topList {
		allowed[ns] = struct{}{}
	}

	// Reset namespace vectors to avoid stale series from previous intervals
	namespacePodPhaseTotal.Reset()
	namespacePodTotal.Reset()

	otherPhaseTotals := map[string]float64{}
	var otherTotal float64

	for ns, phaseMap := range perNamespace {
		if _, ok := allowed[ns]; ok {
			for phase, count := range phaseMap {
				namespacePodPhaseTotal.WithLabelValues(ns, phase).Set(count)
			}
			namespacePodTotal.WithLabelValues(ns).Set(perNamespaceTotal[ns])
			continue
		}
		for phase, count := range phaseMap {
			otherPhaseTotals[phase] += count
		}
		otherTotal += perNamespaceTotal[ns]
	}

	if otherTotal > 0 {
		for phase, count := range otherPhaseTotals {
			namespacePodPhaseTotal.WithLabelValues("other", phase).Set(count)
		}
		namespacePodTotal.WithLabelValues("other").Set(otherTotal)
	}

	return nil
}

func topNamespaceList(counts map[string]float64, limit int) []string {
	type pair struct {
		name  string
		count float64
	}
	items := make([]pair, 0, len(counts))
	for name, count := range counts {
		items = append(items, pair{name: name, count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].count > items[j].count
	})
	if limit > len(items) {
		limit = len(items)
	}
	result := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		result = append(result, items[i].name)
	}
	return result
}
