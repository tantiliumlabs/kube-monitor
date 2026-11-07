package billing

import (
	"context"
	"errors"
	"testing"
	"time"
)

func day(s string) time.Time {
	t, err := time.Parse(dayLayout, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestCategory(t *testing.T) {
	cases := []struct{ usage, record, want string }{
		{"EUN1-SpotUsage:t3.medium", "Usage", CategoryComputeSpot},
		{"EUN1-BoxUsage:t3.large", "Usage", CategoryComputeOnDemand},
		{"EUN1-CPUCredits:t3", "Usage", CategoryCPUCredits},
		{"EUN1-EBS:VolumeUsage.gp3", "Usage", CategoryDisks},
		{"EUN1-EBS:SnapshotUsage", "Usage", CategoryDisks},
		{"EUN1-LoadBalancerUsage", "Usage", CategoryLoadBalancers},
		{"EUN1-LCUUsage", "Usage", CategoryLoadBalancers},
		{"EUN1-PublicIPv4:InUseAddress", "Usage", CategoryPublicIPs},
		{"EUN1-NatGateway-Hours", "Usage", CategoryNAT},
		{"EUN1-DataTransfer-Regional-Bytes", "Usage", CategoryDataTransfer},
		{"EUN1-USE1-AWS-Out-Bytes", "Usage", CategoryDataTransfer},
		{"EUN1-CW:MetricMonitorUsage", "Usage", CategoryMonitoring},
		{"HostedZone", "Usage", CategoryDNS},
		{"DNS-Queries", "Usage", CategoryDNS},
		{"EUN1-TimedStorage-ByteHrs", "Usage", CategoryObjectStorage},
		{"USE1-APIRequest", "Usage", CategoryCostAPI},
		{"NoUsageType", "Tax", CategoryTax},
		{"NoUsageType", "Credit", CategoryCredits},
		{"EUN1-BoxUsage:t3.large", "Refund", CategoryCredits},
		{"Something-New", "Usage", CategoryOther},
	}
	for _, c := range cases {
		if got := Category(c.usage, c.record); got != c.want {
			t.Errorf("%s/%s: got %s, want %s", c.usage, c.record, got, c.want)
		}
	}
}

type fakeStore struct {
	items     []LineItem
	estimates map[string]float64
	lastSync  time.Time
	replaced  [][2]time.Time
	pruned    time.Time
}

func (f *fakeStore) ReplaceDaily(_ context.Context, start, end time.Time, items []LineItem, at time.Time) error {
	f.replaced = append(f.replaced, [2]time.Time{start, end})
	f.items = items
	f.lastSync = at
	return nil
}
func (f *fakeStore) Daily(context.Context, time.Time, time.Time) ([]LineItem, error) {
	return f.items, nil
}
func (f *fakeStore) LastSync(context.Context) (time.Time, error) { return f.lastSync, nil }
func (f *fakeStore) AppendRunRate(context.Context, time.Time, float64, float64, int) error {
	return nil
}
func (f *fakeStore) EstimatedByDay(context.Context, time.Time, time.Time) (map[string]float64, error) {
	return f.estimates, nil
}
func (f *fakeStore) Prune(_ context.Context, before time.Time) error {
	f.pruned = before
	return nil
}

type fakeSource struct {
	start, end time.Time
	calls      int
	err        error
}

func (f *fakeSource) Daily(_ context.Context, start, end time.Time) ([]LineItem, error) {
	f.start, f.end = start, end
	f.calls++
	return nil, f.err
}

func TestSyncWindows(t *testing.T) {
	ctx := context.Background()
	now := day("2026-11-20").Add(9 * time.Hour)

	t.Run("first sync backfills 13 months", func(t *testing.T) {
		src, st := &fakeSource{}, &fakeStore{}
		if err := NewService(src, st).SyncIfDue(ctx, now); err != nil {
			t.Fatal(err)
		}
		if !src.start.Equal(day("2025-10-01")) || !src.end.Equal(day("2026-11-21")) {
			t.Fatalf("window %s to %s", src.start, src.end)
		}
		if !st.pruned.Equal(day("2025-09-01")) {
			t.Fatalf("pruned before %s", st.pruned)
		}
	})
	t.Run("a recent sync costs no call", func(t *testing.T) {
		src, st := &fakeSource{}, &fakeStore{lastSync: now.Add(-time.Hour)}
		if err := NewService(src, st).SyncIfDue(ctx, now); err != nil {
			t.Fatal(err)
		}
		if src.calls != 0 {
			t.Fatalf("calls %d", src.calls)
		}
	})
	t.Run("a daily sync re-fetches recent days", func(t *testing.T) {
		src, st := &fakeSource{}, &fakeStore{lastSync: now.Add(-24 * time.Hour)}
		if err := NewService(src, st).SyncIfDue(ctx, now); err != nil {
			t.Fatal(err)
		}
		if !src.start.Equal(day("2026-11-15")) {
			t.Fatalf("start %s", src.start)
		}
	})
	t.Run("early in the month it re-fetches last month", func(t *testing.T) {
		src, st := &fakeSource{}, &fakeStore{lastSync: day("2026-11-02")}
		if err := NewService(src, st).SyncIfDue(ctx, day("2026-11-03").Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if !src.start.Equal(day("2026-10-01")) {
			t.Fatalf("start %s", src.start)
		}
	})
	t.Run("a failed fetch stores nothing", func(t *testing.T) {
		src, st := &fakeSource{err: errors.New("denied")}, &fakeStore{}
		if err := NewService(src, st).SyncIfDue(ctx, now); err == nil {
			t.Fatal("want error")
		}
		if len(st.replaced) != 0 {
			t.Fatal("stored after failure")
		}
	})
}

func charge(d, usage string, amount float64) LineItem {
	return LineItem{Day: day(d), UsageType: usage, RecordType: "Usage", Amount: amount, Currency: "USD"}
}

func TestReport(t *testing.T) {
	const (
		box  = "EUN1-BoxUsage:t3.medium"
		spot = "EUN1-SpotUsage:t3.medium"
		disk = "EUN1-EBS:VolumeUsage.gp3"
	)
	st := &fakeStore{
		lastSync:  day("2026-11-07"),
		estimates: map[string]float64{"2026-11-05": 4.5, "2026-11-06": 5, "2026-11-07": 2},
		items: []LineItem{
			// Last month: 100 before tax, 20% tax.
			charge("2026-10-15", box, 100),
			{Day: day("2026-10-31"), UsageType: "NoUsageType", RecordType: "Tax", Amount: 20, Currency: "USD"},
			// This month: a one-off on the 1st, then a steady $10 a day.
			charge("2026-11-01", "HostedZone", 50),
			charge("2026-11-02", disk, 10),
			charge("2026-11-03", disk, 10),
			charge("2026-11-04", disk, 10),
			charge("2026-11-05", box, 6),
			charge("2026-11-05", disk, 4),
			charge("2026-11-06", spot, 6),
			charge("2026-11-06", disk, 4),
		},
	}
	rep, err := NewService(&fakeSource{}, st).Report(context.Background(), day("2026-11-07").Add(9*time.Hour), time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	if rep.MonthToDate != 100 || rep.PreviousMonth != 120 || rep.LatestDay != "2026-11-06" {
		t.Fatalf("totals: %+v", rep)
	}
	// $100 so far + 24 remaining days (7th to 30th) at the $10 median, plus 20% tax.
	if rep.ForecastMonthEnd == nil || *rep.ForecastMonthEnd != 408 {
		t.Fatalf("forecast %v: %s", rep.ForecastMonthEnd, rep.ForecastMethod)
	}
	if rep.ByCategory[0].Category != CategoryDisks && rep.ByCategory[0].Category != CategoryDNS {
		t.Fatalf("categories: %+v", rep.ByCategory)
	}
	if len(rep.Daily) != 6 || rep.Daily[0] != (DayAmount{"2026-11-01", 50}) {
		t.Fatalf("daily: %+v", rep.Daily)
	}
	if len(rep.Monthly) != 2 || rep.Monthly[0].Month != "2026-10" || rep.Monthly[1].Amount != 100 {
		t.Fatalf("monthly: %+v", rep.Monthly)
	}
	// Today is left out as still accumulating; a day with no compute bill too.
	want := []DayComparison{
		{"2026-11-05", 4.5, 6, 1.5},
		{"2026-11-06", 5, 6, 1},
	}
	if len(rep.Comparison) != 2 || rep.Comparison[0] != want[0] || rep.Comparison[1] != want[1] {
		t.Fatalf("comparison: %+v", rep.Comparison)
	}
}

func TestReportPeriodOnlyNarrowsCategoriesAndDays(t *testing.T) {
	st := &fakeStore{items: []LineItem{charge("2026-11-02", "EUN1-EBS:x", 10), charge("2026-11-03", "EUN1-EBS:x", 5)}}
	rep, err := NewService(&fakeSource{}, st).Report(context.Background(), day("2026-11-07"), day("2026-11-03"), day("2026-11-03"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.ByCategory[0].Amount != 5 || len(rep.Daily) != 1 || rep.MonthToDate != 15 {
		t.Fatalf("report: %+v", rep)
	}
}

func TestForecastWithoutLastMonthSaysItExcludesTax(t *testing.T) {
	st := &fakeStore{items: []LineItem{charge("2026-11-06", "EUN1-EBS:x", 10)}}
	rep, err := NewService(&fakeSource{}, st).Report(context.Background(), day("2026-11-07"), time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ForecastMonthEnd == nil || *rep.ForecastMonthEnd != 250 {
		t.Fatalf("forecast %v", rep.ForecastMonthEnd)
	}
	if rep.ForecastMethod == "" {
		t.Fatal("no method")
	}
}

func TestNoChargesHasNoForecast(t *testing.T) {
	rep, err := NewService(&fakeSource{}, &fakeStore{}).Report(context.Background(), day("2026-11-07"), time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ForecastMonthEnd != nil || rep.MonthToDate != 0 {
		t.Fatalf("report: %+v", rep)
	}
}

type fakeCatalog struct {
	instances map[string]Instance
	failSpot  bool
}

func (f *fakeCatalog) Instances(context.Context, string, []string) (map[string]Instance, error) {
	return f.instances, nil
}
func (f *fakeCatalog) SpotPrice(context.Context, string, string, string) (float64, error) {
	if f.failSpot {
		return 0, errors.New("throttled")
	}
	return 0.01, nil
}
func (f *fakeCatalog) OnDemandPrice(_ context.Context, _ string, instanceType string) (float64, error) {
	if instanceType == "t3.large" {
		return 0.0864, nil
	}
	return 0.0432, nil
}

func TestCalculateRunRate(t *testing.T) {
	catalog := &fakeCatalog{instances: map[string]Instance{
		"i-spot":  {Type: "t3.medium", AvailabilityZone: "eu-north-1a", Spot: true},
		"i-plane": {Type: "t3.large", AvailabilityZone: "eu-north-1a"},
	}}
	nodes := []NodeRef{
		{Name: "worker", ProviderID: "aws:///eu-north-1a/i-spot"},
		{Name: "plane", ProviderID: "aws:///eu-north-1a/i-plane"},
		{Name: "gone", ProviderID: "aws:///eu-north-1a/i-missing"},
		{Name: "elsewhere", ProviderID: "gce://project/zone/name"},
	}
	rate, err := CalculateRunRate(context.Background(), catalog, nodes, day("2026-11-07"))
	if err != nil {
		t.Fatal(err)
	}
	if rate.Nodes[0].Basis != BasisSpot || rate.Nodes[0].Hourly != 0.01 || rate.Nodes[0].OnDemandHourly != 0.0432 {
		t.Fatalf("spot node: %+v", rate.Nodes[0])
	}
	if rate.Nodes[1].Basis != BasisOnDemand || rate.Nodes[1].InstanceType != "t3.large" || rate.Nodes[1].Hourly != 0.0864 {
		t.Fatalf("on-demand node: %+v", rate.Nodes[1])
	}
	if rate.UnpricedNodes != 2 || rate.Nodes[2].Hourly != 0 || rate.Nodes[3].Basis != BasisUnknown {
		t.Fatalf("unpriced: %+v", rate)
	}
	if rate.Hourly != 0.0964 {
		t.Fatalf("hourly %v", rate.Hourly)
	}
}

func TestASpotPriceFailureLeavesTheNodeUnpriced(t *testing.T) {
	catalog := &fakeCatalog{failSpot: true, instances: map[string]Instance{"i-spot": {Type: "t3.medium", Spot: true}}}
	rate, err := CalculateRunRate(context.Background(), catalog, []NodeRef{{ProviderID: "aws:///eu-north-1a/i-spot"}}, day("2026-11-07"))
	if err != nil {
		t.Fatal(err)
	}
	if rate.UnpricedNodes != 1 || rate.Hourly != 0 {
		t.Fatalf("rate: %+v", rate)
	}
}
