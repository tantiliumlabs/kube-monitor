// Package billing reports what the cloud account was actually charged, and how
// that compares with what the running nodes should cost.
package billing

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	dayLayout   = "2006-01-02"
	monthLayout = "2006-01"

	// monthsKept is how much history is fetched and stored: enough to compare
	// a month with the same month a year earlier.
	monthsKept = 13
	// resyncDays is re-fetched on each sync, because recent days keep changing
	// as the provider finalises them.
	resyncDays = 5
	// finalisedByDay is when the previous month is taken as final. Until then it
	// is re-fetched in full, since tax and adjustments post around month end.
	finalisedByDay = 5
	// syncEvery limits paid calls: Cost Explorer charges per request.
	syncEvery = 23 * time.Hour
	// forecastDays is the window behind the forecast's typical day.
	forecastDays = 7
)

// Cost categories.
const (
	CategoryComputeOnDemand = "compute_on_demand"
	CategoryComputeSpot     = "compute_spot"
	CategoryCPUCredits      = "cpu_credits"
	CategoryDisks           = "disks"
	CategoryLoadBalancers   = "load_balancers"
	CategoryPublicIPs       = "public_ips"
	CategoryNAT             = "nat_gateways"
	CategoryDataTransfer    = "data_transfer"
	CategoryMonitoring      = "monitoring"
	CategoryDNS             = "dns"
	CategoryObjectStorage   = "object_storage"
	CategoryCostAPI         = "cost_api"
	CategoryTax             = "tax"
	CategoryCredits         = "credits"
	CategoryOther           = "other"
)

// LineItem is one day's charge for one usage type.
type LineItem struct {
	Day        time.Time
	UsageType  string
	RecordType string
	Amount     float64
	Quantity   float64
	Unit       string
	Currency   string
}

// Category groups a provider usage type under a name people recognise.
func Category(usageType, recordType string) string {
	switch recordType {
	case "Tax":
		return CategoryTax
	case "Credit", "Refund":
		return CategoryCredits
	}
	has := func(parts ...string) bool {
		for _, p := range parts {
			if strings.Contains(usageType, p) {
				return true
			}
		}
		return false
	}
	switch {
	case has("SpotUsage"):
		return CategoryComputeSpot
	case has("BoxUsage"):
		return CategoryComputeOnDemand
	case has("CPUCredits"):
		return CategoryCPUCredits
	case has("EBS:"):
		return CategoryDisks
	case has("LoadBalancerUsage", "LCUUsage"):
		return CategoryLoadBalancers
	case has("PublicIPv4"):
		return CategoryPublicIPs
	case has("NatGateway"):
		return CategoryNAT
	case has("DataTransfer", "-Out-Bytes", "-In-Bytes"):
		return CategoryDataTransfer
	case has("CW:"):
		return CategoryMonitoring
	case has("HostedZone", "DNS-Queries"):
		return CategoryDNS
	case has("TimedStorage", "Requests-Tier"):
		return CategoryObjectStorage
	case has("APIRequest"):
		return CategoryCostAPI
	}
	return CategoryOther
}

// Source fetches line items for [start, end) from the provider.
type Source interface {
	Daily(ctx context.Context, start, end time.Time) ([]LineItem, error)
}

// Store keeps fetched charges and run-rate samples.
type Store interface {
	// ReplaceDaily swaps [start, end) for items, so lines the provider has
	// since removed don't linger.
	ReplaceDaily(ctx context.Context, start, end time.Time, items []LineItem, syncedAt time.Time) error
	Daily(ctx context.Context, start, end time.Time) ([]LineItem, error)
	LastSync(ctx context.Context) (time.Time, error)
	AppendRunRate(ctx context.Context, at time.Time, effective, onDemand float64, seconds int) error
	// EstimatedByDay is each UTC day's sampled node cost in dollars, keyed by
	// YYYY-MM-DD.
	EstimatedByDay(ctx context.Context, start, end time.Time) (map[string]float64, error)
	Prune(ctx context.Context, before time.Time) error
}

// Service syncs and reports charges.
type Service struct {
	source Source
	store  Store
}

func NewService(source Source, store Store) *Service {
	return &Service{source: source, store: store}
}

func startOfMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// SyncIfDue syncs unless a sync succeeded recently.
func (s *Service) SyncIfDue(ctx context.Context, now time.Time) error {
	last, err := s.store.LastSync(ctx)
	if err != nil {
		return fmt.Errorf("read last sync: %w", err)
	}
	if !last.IsZero() && now.Sub(last) < syncEvery {
		return nil
	}
	return s.sync(ctx, now, last.IsZero())
}

func (s *Service) sync(ctx context.Context, now time.Time, firstSync bool) error {
	now = now.UTC()
	end := startOfDay(now).AddDate(0, 0, 1)
	start := startOfDay(now).AddDate(0, 0, -resyncDays)
	if now.Day() <= finalisedByDay {
		start = startOfMonth(now).AddDate(0, -1, 0)
	}
	if firstSync {
		start = startOfMonth(now).AddDate(0, -monthsKept, 0)
	}
	items, err := s.source.Daily(ctx, start, end)
	if err != nil {
		return fmt.Errorf("fetch charges: %w", err)
	}
	if err := s.store.ReplaceDaily(ctx, start, end, items, now); err != nil {
		return fmt.Errorf("store charges: %w", err)
	}
	if err := s.store.Prune(ctx, startOfMonth(now).AddDate(0, -monthsKept-1, 0)); err != nil {
		return fmt.Errorf("prune: %w", err)
	}
	return nil
}

// CategoryAmount is one category's total for the selected period.
type CategoryAmount struct {
	Category string  `json:"category"`
	Amount   float64 `json:"amount"`
}

// DayAmount is one day's total.
type DayAmount struct {
	Day    string  `json:"day"`
	Amount float64 `json:"amount"`
}

// MonthAmount is one month's total.
type MonthAmount struct {
	Month  string  `json:"month"`
	Amount float64 `json:"amount"`
}

// DayComparison sets a day's sampled node cost beside what compute was billed.
type DayComparison struct {
	Day            string  `json:"day"`
	EstimatedNodes float64 `json:"estimated_nodes"`
	BilledCompute  float64 `json:"billed_compute"`
	Difference     float64 `json:"difference"`
}

// Report is what the dashboards read.
type Report struct {
	Enabled          bool             `json:"enabled"`
	Currency         string           `json:"currency"`
	From             string           `json:"from"`
	To               string           `json:"to"`
	LastSync         *time.Time       `json:"last_sync,omitempty"`
	LatestDay        string           `json:"latest_day,omitempty"`
	MonthToDate      float64          `json:"month_to_date"`
	PreviousMonth    float64          `json:"previous_month"`
	ForecastMonthEnd *float64         `json:"forecast_month_end,omitempty"`
	ForecastMethod   string           `json:"forecast_method,omitempty"`
	ByCategory       []CategoryAmount `json:"by_category"`
	Daily            []DayAmount      `json:"daily"`
	Monthly          []MonthAmount    `json:"monthly"`
	Comparison       []DayComparison  `json:"comparison"`
}

func round2(v float64) float64 {
	if v >= 0 {
		return float64(int64(v*100+0.5)) / 100
	}
	return float64(int64(v*100-0.5)) / 100
}

// Report summarises charges. from and to (inclusive) select the period behind
// the category and daily figures, defaulting to this month; the month figures
// and forecast always describe the current month.
func (s *Service) Report(ctx context.Context, now time.Time, from, to time.Time) (*Report, error) {
	now = now.UTC()
	thisMonth := startOfMonth(now)
	if from.IsZero() {
		from = thisMonth
	}
	if to.IsZero() {
		to = startOfDay(now)
	}
	items, err := s.store.Daily(ctx, thisMonth.AddDate(0, -monthsKept, 0), startOfDay(now).AddDate(0, 0, 1))
	if err != nil {
		return nil, fmt.Errorf("read charges: %w", err)
	}

	rep := &Report{
		Enabled:    true,
		Currency:   "USD",
		From:       from.Format(dayLayout),
		To:         to.Format(dayLayout),
		ByCategory: []CategoryAmount{},
		Daily:      []DayAmount{},
		Monthly:    []MonthAmount{},
		Comparison: []DayComparison{},
	}
	if last, err := s.store.LastSync(ctx); err != nil {
		return nil, fmt.Errorf("read last sync: %w", err)
	} else if !last.IsZero() {
		rep.LastSync = &last
	}

	byCategory := map[string]float64{}
	byDay := map[string]float64{}
	byMonth := map[string]float64{}
	computeByDay := map[string]float64{}
	preTaxByDay := map[string]float64{}
	var taxByMonth, preTaxByMonth = map[string]float64{}, map[string]float64{}

	for _, item := range items {
		if item.Currency != "" {
			rep.Currency = item.Currency
		}
		day := item.Day.UTC().Format(dayLayout)
		month := item.Day.UTC().Format(monthLayout)
		category := Category(item.UsageType, item.RecordType)
		byMonth[month] += item.Amount
		if category == CategoryTax {
			taxByMonth[month] += item.Amount
		} else {
			preTaxByMonth[month] += item.Amount
			preTaxByDay[day] += item.Amount
		}
		if category == CategoryComputeOnDemand || category == CategoryComputeSpot {
			computeByDay[day] += item.Amount
		}
		if !item.Day.Before(from) && !item.Day.After(to) {
			byCategory[category] += item.Amount
			byDay[day] += item.Amount
		}
		if day > rep.LatestDay {
			rep.LatestDay = day
		}
	}

	rep.MonthToDate = round2(byMonth[thisMonth.Format(monthLayout)])
	rep.PreviousMonth = round2(byMonth[thisMonth.AddDate(0, -1, 0).Format(monthLayout)])

	for category, amount := range byCategory {
		rep.ByCategory = append(rep.ByCategory, CategoryAmount{Category: category, Amount: round2(amount)})
	}
	sort.Slice(rep.ByCategory, func(i, j int) bool {
		if rep.ByCategory[i].Amount != rep.ByCategory[j].Amount {
			return rep.ByCategory[i].Amount > rep.ByCategory[j].Amount
		}
		return rep.ByCategory[i].Category < rep.ByCategory[j].Category
	})
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		key := day.Format(dayLayout)
		if amount, ok := byDay[key]; ok {
			rep.Daily = append(rep.Daily, DayAmount{Day: key, Amount: round2(amount)})
		}
	}
	for month := thisMonth.AddDate(0, -monthsKept+1, 0); !month.After(thisMonth); month = month.AddDate(0, 1, 0) {
		key := month.Format(monthLayout)
		if amount, ok := byMonth[key]; ok {
			rep.Monthly = append(rep.Monthly, MonthAmount{Month: key, Amount: round2(amount)})
		}
	}

	if len(items) > 0 {
		applyForecast(rep, now, preTaxByDay, preTaxByMonth, taxByMonth)
	}

	estimated, err := s.store.EstimatedByDay(ctx, from, to.AddDate(0, 0, 1))
	if err != nil {
		return nil, fmt.Errorf("read estimates: %w", err)
	}
	today := startOfDay(now).Format(dayLayout)
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		key := day.Format(dayLayout)
		est, sampled := estimated[key]
		billed, hasBill := computeByDay[key]
		// Today is still accumulating on both sides.
		if key >= today || !sampled || !hasBill {
			continue
		}
		rep.Comparison = append(rep.Comparison, DayComparison{
			Day: key, EstimatedNodes: round2(est), BilledCompute: round2(billed), Difference: round2(billed - est),
		})
	}
	return rep, nil
}

// applyForecast projects the month from a typical recent day. The median keeps
// one-off charges, such as a monthly DNS fee, from being repeated every
// remaining day. Tax posts as one line around month end, so it's projected from
// last month's rate.
func applyForecast(rep *Report, now time.Time, preTaxByDay, preTaxByMonth, taxByMonth map[string]float64) {
	today := startOfDay(now)
	recent := make([]float64, 0, forecastDays)
	for i := 1; i <= forecastDays; i++ {
		if amount, ok := preTaxByDay[today.AddDate(0, 0, -i).Format(dayLayout)]; ok {
			recent = append(recent, amount)
		}
	}
	if len(recent) == 0 {
		return
	}
	sort.Float64s(recent)
	typical := recent[len(recent)/2]
	if len(recent)%2 == 0 {
		typical = (recent[len(recent)/2-1] + recent[len(recent)/2]) / 2
	}

	month := startOfMonth(now)
	soFar := 0.0
	for day := month; day.Before(today); day = day.AddDate(0, 0, 1) {
		soFar += preTaxByDay[day.Format(dayLayout)]
	}
	remaining := month.AddDate(0, 1, 0).Sub(today).Hours() / 24
	preTax := soFar + typical*remaining

	method := fmt.Sprintf("Charged so far this month, plus %d remaining days at $%.2f, the median of the last %d days.", int(remaining), typical, len(recent))
	lastMonth := month.AddDate(0, -1, 0).Format(monthLayout)
	if base := preTaxByMonth[lastMonth]; base > 0 && taxByMonth[lastMonth] > 0 {
		rate := taxByMonth[lastMonth] / base
		preTax *= 1 + rate
		method += fmt.Sprintf(" Includes tax at last month's %.0f%%.", rate*100)
	} else {
		method += " Excludes tax."
	}
	forecast := round2(preTax)
	rep.ForecastMonthEnd = &forecast
	rep.ForecastMethod = method
}
