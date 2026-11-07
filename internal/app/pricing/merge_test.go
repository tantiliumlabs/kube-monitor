package pricing

import (
	"testing"

	domain "github.com/ochestra-tech/k8s-monitor/internal/domain/pricing"
)

func TestRegionMultiplierOnlyAdjustsStaticPrices(t *testing.T) {
	regions := map[string]float64{"eu-north-1": 1.08}
	price := domain.ResourcePricing{TotalPerHour: 0.0432, CPU: 0.01, Memory: 0.005}
	got := mergeDefaults(domain.ResourcePricing{}, []domain.InstancePricing{
		{InstanceType: "live", Region: "eu-north-1", Source: "aws", Pricing: price},
		{InstanceType: "fixed", Region: "eu-north-1", Source: staticSourceName, Pricing: price},
	}, regions)
	if got["live"].TotalPerHour != 0.0432 {
		t.Errorf("provider price changed: %v", got["live"].TotalPerHour)
	}
	if v := got["fixed"].TotalPerHour; v < 0.0466 || v > 0.0467 {
		t.Errorf("static price not adjusted: %v", v)
	}
}
