package pricing

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	domain "github.com/ochestra-tech/k8s-monitor/internal/domain/pricing"
	cacheports "github.com/ochestra-tech/k8s-monitor/internal/ports/cache"
	ports "github.com/ochestra-tech/k8s-monitor/internal/ports/pricing"
	"github.com/ochestra-tech/k8s-monitor/pkg/cost"
)

// Service orchestrates pricing providers.
type Service struct {
	providers map[string]ports.Provider
	cache     cacheports.Store
}

// NewService creates a pricing service with provider registry.
func NewService(providers []ports.Provider, cacheStore cacheports.Store) *Service {
	registry := make(map[string]ports.Provider)
	for _, provider := range providers {
		registry[provider.Name()] = provider
	}
	return &Service{providers: registry, cache: cacheStore}
}

// Resolve returns pricing data mapped to cost.ResourcePricing.
func (s *Service) Resolve(ctx context.Context, cfg domain.Config) (map[string]cost.ResourcePricing, error) {
	source := strings.ToLower(cfg.Source)
	if source == "" {
		source = "static"
	}

	req := ports.Request{
		Region:          cfg.Providers.AWS.Region,
		Currency:        cfg.Providers.AWS.Currency,
		OperatingSystem: cfg.Providers.AWS.OperatingSystem,
		Tenancy:         cfg.Providers.AWS.Tenancy,
		InstanceTypes:   mapKeys(cfg.InstanceTypes),
	}

	order := []string{source}
	if source == "auto" {
		order = []string{"aws", "azure", "gcp", "static"}
	}

	var lastErr error
	for _, name := range order {
		provider, ok := s.providers[name]
		if !ok {
			continue
		}
		providerReq := req
		switch name {
		case "azure":
			providerReq.Region = cfg.Providers.Azure.Region
			providerReq.Currency = cfg.Providers.Azure.Currency
		case "gcp":
			providerReq.Region = cfg.Providers.GCP.Region
			providerReq.Currency = cfg.Providers.GCP.Currency
		}

		cacheKey := cacheKey(provider.Name(), providerReq)
		if s.cache != nil && cfg.CacheTTL.Duration > 0 {
			cached, ok, err := s.cache.Get(ctx, cacheKey)
			if err != nil {
				log.Printf("pricing cache get failed: %v", err)
			} else if ok {
				var cachedPricing map[string]cost.ResourcePricing
				if err := json.Unmarshal([]byte(cached), &cachedPricing); err != nil {
					log.Printf("pricing cache decode failed: %v", err)
				} else {
					return cachedPricing, nil
				}
			}
		}

		prices, err := fetchWithRetry(ctx, provider, providerReq, cfg.Retry)
		if err != nil {
			lastErr = err
			continue
		}

		mapped := mergeDefaults(cfg.Defaults, prices, cfg.Regions)
		if s.cache != nil && cfg.CacheTTL.Duration > 0 {
			encoded, err := json.Marshal(mapped)
			if err != nil {
				log.Printf("pricing cache encode failed: %v", err)
			} else if err := s.cache.Set(ctx, cacheKey, string(encoded), cfg.CacheTTL.Duration); err != nil {
				log.Printf("pricing cache set failed: %v", err)
			}
		}
		return mapped, nil
	}

	return nil, fmt.Errorf("pricing resolution failed: %w", lastErr)
}

// staticSourceName is the static provider's Name().
const staticSourceName = "static"

func mergeDefaults(defaults domain.ResourcePricing, prices []domain.InstancePricing, regionMultipliers map[string]float64) map[string]cost.ResourcePricing {
	result := make(map[string]cost.ResourcePricing)
	for _, item := range prices {
		pricing := item.Pricing
		if pricing.CPU == 0 && pricing.Memory == 0 && pricing.Storage == 0 && pricing.Network == 0 && pricing.TotalPerHour == 0 {
			pricing = defaults
		}
		// Only static prices need adjusting for region. A provider's price is
		// already that region's, so multiplying again overstated it (t3.medium
		// in eu-north-1 was shown at $0.0467 against AWS's $0.0432).
		if item.Source == "" || item.Source == staticSourceName {
			pricing = applyRegionMultiplier(pricing, item.Region, regionMultipliers)
		}
		pricing = splitTotalIfNeeded(pricing, defaults)
		result[item.InstanceType] = cost.ResourcePricing{
			CPU:          pricing.CPU,
			Memory:       pricing.Memory,
			Storage:      pricing.Storage,
			Network:      pricing.Network,
			TotalPerHour: pricing.TotalPerHour,
			GPUPricing:   pricing.GPUPricing,
		}
	}
	if len(result) == 0 {
		defaults = applyRegionMultiplier(defaults, "", regionMultipliers)
		defaults = splitTotalIfNeeded(defaults, defaults)
		result["default"] = cost.ResourcePricing{
			CPU:          defaults.CPU,
			Memory:       defaults.Memory,
			Storage:      defaults.Storage,
			Network:      defaults.Network,
			TotalPerHour: defaults.TotalPerHour,
			GPUPricing:   defaults.GPUPricing,
		}
	}
	if _, ok := result["default"]; !ok {
		fallback := applyRegionMultiplier(defaults, "", regionMultipliers)
		fallback = splitTotalIfNeeded(fallback, defaults)
		result["default"] = cost.ResourcePricing{
			CPU:          fallback.CPU,
			Memory:       fallback.Memory,
			Storage:      fallback.Storage,
			Network:      fallback.Network,
			TotalPerHour: fallback.TotalPerHour,
			GPUPricing:   fallback.GPUPricing,
		}
	}
	return result
}

func mapKeys(values map[string]domain.ResourcePricing) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cacheKey(provider string, req ports.Request) string {
	parts := []string{
		provider,
		req.Region,
		req.Currency,
		req.OperatingSystem,
		req.Tenancy,
		strings.Join(req.InstanceTypes, ","),
	}
	h := sha1.Sum([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:])
}

func fetchWithRetry(ctx context.Context, provider ports.Provider, req ports.Request, retry domain.RetryConfig) ([]domain.InstancePricing, error) {
	attempts := retry.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	backoff := retry.Backoff.Duration
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		prices, err := provider.Fetch(ctx, req)
		if err == nil {
			return prices, nil
		}
		lastErr = err
		if i == attempts-1 {
			break
		}
		wait := backoff * time.Duration(i+1)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func applyRegionMultiplier(pricing domain.ResourcePricing, region string, multipliers map[string]float64) domain.ResourcePricing {
	if len(multipliers) == 0 {
		return pricing
	}
	multiplier := 1.0
	if region != "" {
		if value, ok := multipliers[region]; ok {
			multiplier = value
		}
	}
	if multiplier == 1.0 {
		return pricing
	}
	pricing.CPU *= multiplier
	pricing.Memory *= multiplier
	pricing.Storage *= multiplier
	pricing.Network *= multiplier
	pricing.TotalPerHour *= multiplier
	return pricing
}

func splitTotalIfNeeded(pricing domain.ResourcePricing, defaults domain.ResourcePricing) domain.ResourcePricing {
	if pricing.TotalPerHour <= 0 {
		return pricing
	}
	if pricing.CPU > 0 || pricing.Memory > 0 || pricing.Storage > 0 {
		return pricing
	}

	denom := defaults.CPU + defaults.Memory + defaults.Storage
	if denom <= 0 {
		pricing.CPU = pricing.TotalPerHour * 0.7
		pricing.Memory = pricing.TotalPerHour * 0.3
		pricing.Storage = 0
		return pricing
	}

	pricing.CPU = pricing.TotalPerHour * (defaults.CPU / denom)
	pricing.Memory = pricing.TotalPerHour * (defaults.Memory / denom)
	pricing.Storage = pricing.TotalPerHour * (defaults.Storage / denom)
	return pricing
}
