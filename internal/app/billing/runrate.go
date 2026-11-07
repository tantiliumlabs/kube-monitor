package billing

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// Price bases, saying where a node's rate came from.
const (
	BasisSpot     = "spot"
	BasisOnDemand = "on_demand"
	BasisUnknown  = "unknown"
)

// Instance is what the provider says about a node's machine.
type Instance struct {
	Type             string
	AvailabilityZone string
	Spot             bool
}

// Catalog looks up machines and their current prices.
type Catalog interface {
	Instances(ctx context.Context, region string, ids []string) (map[string]Instance, error)
	SpotPrice(ctx context.Context, region, instanceType, zone string) (float64, error)
	OnDemandPrice(ctx context.Context, region, instanceType string) (float64, error)
}

// NodeRef is a cluster node to price.
type NodeRef struct {
	Name       string
	ProviderID string
}

// NodeRate is what one node costs right now.
type NodeRate struct {
	Name           string  `json:"name"`
	InstanceID     string  `json:"instance_id,omitempty"`
	InstanceType   string  `json:"instance_type,omitempty"`
	Hourly         float64 `json:"hourly"`
	OnDemandHourly float64 `json:"on_demand_hourly"`
	Basis          string  `json:"basis"`
}

// RunRate is what the nodes running now cost per hour. It covers machines only:
// disks, load balancers and the like appear in the bill, not here.
type RunRate struct {
	AsOf           time.Time  `json:"as_of"`
	Hourly         float64    `json:"hourly"`
	OnDemandHourly float64    `json:"on_demand_hourly"`
	UnpricedNodes  int        `json:"unpriced_nodes"`
	Nodes          []NodeRate `json:"nodes"`
}

// parseProviderID splits "aws:///eu-north-1a/i-0abc" into its zone, region and
// instance ID.
func parseProviderID(id string) (zone, region, instance string, ok bool) {
	rest, found := strings.CutPrefix(id, "aws:///")
	if !found {
		return "", "", "", false
	}
	zone, instance, found = strings.Cut(rest, "/")
	if !found || len(zone) < 2 || !strings.HasPrefix(instance, "i-") {
		return "", "", "", false
	}
	return zone, zone[:len(zone)-1], instance, true
}

// CalculateRunRate prices nodes at what they cost now. A node that can't be
// priced counts as unpriced and adds nothing, so the total never rests on a guess.
func CalculateRunRate(ctx context.Context, catalog Catalog, nodes []NodeRef, now time.Time) (*RunRate, error) {
	rate := &RunRate{AsOf: now.UTC(), Nodes: make([]NodeRate, 0, len(nodes))}

	idsByRegion := map[string][]string{}
	for _, node := range nodes {
		if _, region, id, ok := parseProviderID(node.ProviderID); ok {
			idsByRegion[region] = append(idsByRegion[region], id)
		}
	}
	instances := map[string]Instance{}
	for region, ids := range idsByRegion {
		found, err := catalog.Instances(ctx, region, ids)
		if err != nil {
			return nil, fmt.Errorf("describe instances in %s: %w", region, err)
		}
		for id, instance := range found {
			instances[id] = instance
		}
	}

	for _, node := range nodes {
		nr := NodeRate{Name: node.Name, Basis: BasisUnknown}
		if _, region, id, ok := parseProviderID(node.ProviderID); ok {
			nr.InstanceID = id
			if instance, ok := instances[id]; ok {
				nr.InstanceType = instance.Type
				priceNode(ctx, catalog, &nr, region, instance)
			}
		}
		if nr.Basis == BasisUnknown {
			rate.UnpricedNodes++
		}
		rate.Hourly += nr.Hourly
		rate.OnDemandHourly += nr.OnDemandHourly
		rate.Nodes = append(rate.Nodes, nr)
	}
	return rate, nil
}

func priceNode(ctx context.Context, catalog Catalog, nr *NodeRate, region string, instance Instance) {
	onDemand, err := catalog.OnDemandPrice(ctx, region, instance.Type)
	if err != nil {
		log.Printf("[billing] on-demand price for %s: %v", instance.Type, err)
	} else {
		nr.OnDemandHourly = onDemand
	}
	if !instance.Spot {
		if err == nil && onDemand > 0 {
			nr.Hourly, nr.Basis = onDemand, BasisOnDemand
		}
		return
	}
	spot, err := catalog.SpotPrice(ctx, region, instance.Type, instance.AvailabilityZone)
	if err != nil {
		log.Printf("[billing] spot price for %s in %s: %v", instance.Type, instance.AvailabilityZone, err)
		return
	}
	if spot > 0 {
		nr.Hourly, nr.Basis = spot, BasisSpot
	}
}

// RecordRunRate stores a sample of the rate, covering the given interval.
func (s *Service) RecordRunRate(ctx context.Context, rate *RunRate, interval time.Duration) error {
	return s.store.AppendRunRate(ctx, rate.AsOf, rate.Hourly, rate.OnDemandHourly, int(interval.Seconds()))
}
