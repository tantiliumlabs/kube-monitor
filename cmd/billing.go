package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	awsbilling "github.com/ochestra-tech/k8s-monitor/internal/adapters/aws/billing"
	awsp "github.com/ochestra-tech/k8s-monitor/internal/adapters/pricing/aws"
	pgstore "github.com/ochestra-tech/k8s-monitor/internal/adapters/store/postgres"
	appbilling "github.com/ochestra-tech/k8s-monitor/internal/app/billing"
)

const (
	// billingCheckEvery only decides whether a sync is due; SyncIfDue limits
	// the paid calls. The cluster stops at night, so a daily timer alone could
	// keep missing its slot.
	billingCheckEvery = time.Hour
	runRateEvery      = 5 * time.Minute
	runRateCacheFor   = time.Minute
	billingTimeout    = 2 * time.Minute
)

// billingAPI serves the bill and run rate. Nil-safe: with billing off, the
// routes say so rather than failing.
type billingAPI struct {
	service   *appbilling.Service
	catalog   appbilling.Catalog
	clientset *kubernetes.Clientset

	mu       sync.Mutex
	rate     *appbilling.RunRate
	rateTime time.Time
}

// newBillingAPI returns nil unless COST_ACTUALS=on and Postgres is available.
func newBillingAPI(ctx context.Context, clientset *kubernetes.Clientset, pg *pgstore.Store) *billingAPI {
	if os.Getenv("COST_ACTUALS") != "on" {
		return nil
	}
	if pg == nil {
		log.Printf("[billing] COST_ACTUALS=on needs DATABASE_URL; staying off")
		return nil
	}
	store, err := pg.Billing(ctx)
	if err != nil {
		log.Printf("[billing] staying off: %v", err)
		return nil
	}
	source, err := awsbilling.NewCostExplorer(ctx)
	if err != nil {
		log.Printf("[billing] staying off: %v", err)
		return nil
	}
	provider, err := awsp.New(ctx, false, nil)
	if err != nil {
		log.Printf("[billing] staying off: %v", err)
		return nil
	}
	return &billingAPI{
		service:   appbilling.NewService(source, store),
		catalog:   awsbilling.NewCatalog(provider),
		clientset: clientset,
	}
}

// run keeps charges and run-rate samples current until ctx ends.
func (b *billingAPI) run(ctx context.Context) {
	b.syncCharges(ctx)
	b.sampleRunRate(ctx)
	charges := time.NewTicker(billingCheckEvery)
	defer charges.Stop()
	samples := time.NewTicker(runRateEvery)
	defer samples.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-charges.C:
			b.syncCharges(ctx)
		case <-samples.C:
			b.sampleRunRate(ctx)
		}
	}
}

func (b *billingAPI) syncCharges(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, billingTimeout)
	defer cancel()
	if err := b.service.SyncIfDue(ctx, time.Now()); err != nil {
		log.Printf("[billing] sync failed: %v", err)
	}
}

func (b *billingAPI) sampleRunRate(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, billingTimeout)
	defer cancel()
	rate, err := b.currentRate(ctx, true)
	if err != nil {
		log.Printf("[billing] run rate failed: %v", err)
		return
	}
	if err := b.service.RecordRunRate(ctx, rate, runRateEvery); err != nil {
		log.Printf("[billing] storing run rate failed: %v", err)
	}
}

func (b *billingAPI) currentRate(ctx context.Context, fresh bool) (*appbilling.RunRate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !fresh && b.rate != nil && time.Since(b.rateTime) < runRateCacheFor {
		return b.rate, nil
	}
	list, err := b.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	nodes := make([]appbilling.NodeRef, 0, len(list.Items))
	for _, node := range list.Items {
		nodes = append(nodes, appbilling.NodeRef{Name: node.Name, ProviderID: node.Spec.ProviderID})
	}
	rate, err := appbilling.CalculateRunRate(ctx, b.catalog, nodes, time.Now())
	if err != nil {
		return nil, err
	}
	b.rate, b.rateTime = rate, time.Now()
	return rate, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("[billing] write response: %v", err)
	}
}

func parseDay(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, true
	}
	t, err := time.Parse("2006-01-02", raw)
	return t, err == nil
}

// actualHandler serves GET /api/cost/actual?from=YYYY-MM-DD&to=YYYY-MM-DD.
func (b *billingAPI) actualHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if b == nil {
			writeJSON(w, http.StatusOK, map[string]bool{"enabled": false})
			return
		}
		q := r.URL.Query()
		from, okFrom := parseDay(q.Get("from"))
		to, okTo := parseDay(q.Get("to"))
		if !okFrom || !okTo {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from and to must be YYYY-MM-DD"})
			return
		}
		report, err := b.service.Report(r.Context(), time.Now(), from, to)
		if err != nil {
			log.Printf("[billing] report failed: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read billing data"})
			return
		}
		writeJSON(w, http.StatusOK, report)
	}
}

// runRateHandler serves GET /api/cost/run-rate.
func (b *billingAPI) runRateHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if b == nil {
			writeJSON(w, http.StatusOK, map[string]bool{"enabled": false})
			return
		}
		rate, err := b.currentRate(r.Context(), false)
		if err != nil {
			log.Printf("[billing] run rate failed: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read node prices"})
			return
		}
		writeJSON(w, http.StatusOK, rate)
	}
}
