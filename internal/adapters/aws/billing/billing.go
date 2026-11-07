// Package billing reads charges and instance prices from AWS.
package billing

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	appbilling "github.com/ochestra-tech/k8s-monitor/internal/app/billing"
	portspricing "github.com/ochestra-tech/k8s-monitor/internal/ports/pricing"
)

const (
	// Cost Explorer's API is served from one region, whichever region is billed.
	costExplorerRegion = "us-east-1"
	spotPriceTTL       = time.Hour
	onDemandPriceTTL   = 24 * time.Hour
	metricCost         = "UnblendedCost"
	metricUsage        = "UsageQuantity"
)

// CostExplorer implements billing.Source. Each page is a charged request.
type CostExplorer struct {
	client *costexplorer.Client
}

func NewCostExplorer(ctx context.Context) (*CostExplorer, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(costExplorerRegion))
	if err != nil {
		return nil, err
	}
	return &CostExplorer{client: costexplorer.NewFromConfig(cfg)}, nil
}

func (c *CostExplorer) Daily(ctx context.Context, start, end time.Time) ([]appbilling.LineItem, error) {
	input := &costexplorer.GetCostAndUsageInput{
		TimePeriod:  &cetypes.DateInterval{Start: aws.String(start.Format("2006-01-02")), End: aws.String(end.Format("2006-01-02"))},
		Granularity: cetypes.GranularityDaily,
		Metrics:     []string{metricCost, metricUsage},
		GroupBy: []cetypes.GroupDefinition{
			{Type: cetypes.GroupDefinitionTypeDimension, Key: aws.String("USAGE_TYPE")},
			{Type: cetypes.GroupDefinitionTypeDimension, Key: aws.String("RECORD_TYPE")},
		},
	}
	var items []appbilling.LineItem
	for {
		out, err := c.client.GetCostAndUsage(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, result := range out.ResultsByTime {
			if result.TimePeriod == nil {
				continue
			}
			day, err := time.Parse("2006-01-02", aws.ToString(result.TimePeriod.Start))
			if err != nil {
				return nil, fmt.Errorf("parse day: %w", err)
			}
			for _, group := range result.Groups {
				if item, ok := lineItem(day, group); ok {
					items = append(items, item)
				}
			}
		}
		if out.NextPageToken == nil {
			return items, nil
		}
		input.NextPageToken = out.NextPageToken
	}
}

func lineItem(day time.Time, group cetypes.Group) (appbilling.LineItem, bool) {
	if len(group.Keys) != 2 {
		return appbilling.LineItem{}, false
	}
	cost := group.Metrics[metricCost]
	amount, err := strconv.ParseFloat(aws.ToString(cost.Amount), 64)
	if err != nil || amount == 0 {
		return appbilling.LineItem{}, false
	}
	usage := group.Metrics[metricUsage]
	quantity, _ := strconv.ParseFloat(aws.ToString(usage.Amount), 64)
	return appbilling.LineItem{
		Day:        day,
		UsageType:  group.Keys[0],
		RecordType: group.Keys[1],
		Amount:     amount,
		Quantity:   quantity,
		Unit:       aws.ToString(usage.Unit),
		Currency:   aws.ToString(cost.Unit),
	}, true
}

type cachedPrice struct {
	price   float64
	fetched time.Time
}

// Catalog implements billing.Catalog.
type Catalog struct {
	loadRegion func(ctx context.Context, region string) (*ec2.Client, error)
	onDemand   portspricing.Provider

	mu      sync.Mutex
	clients map[string]*ec2.Client
	prices  map[string]cachedPrice
}

func NewCatalog(onDemand portspricing.Provider) *Catalog {
	return &Catalog{
		onDemand: onDemand,
		clients:  map[string]*ec2.Client{},
		prices:   map[string]cachedPrice{},
		loadRegion: func(ctx context.Context, region string) (*ec2.Client, error) {
			cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
			if err != nil {
				return nil, err
			}
			return ec2.NewFromConfig(cfg), nil
		},
	}
}

func (c *Catalog) client(ctx context.Context, region string) (*ec2.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[region]; ok {
		return client, nil
	}
	client, err := c.loadRegion(ctx, region)
	if err != nil {
		return nil, err
	}
	c.clients[region] = client
	return client, nil
}

func (c *Catalog) cached(key string, ttl time.Duration) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.prices[key]
	if !ok || time.Since(p.fetched) > ttl {
		return 0, false
	}
	return p.price, true
}

func (c *Catalog) remember(key string, price float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prices[key] = cachedPrice{price: price, fetched: time.Now()}
}

// Instances filters by ID instead of naming the IDs, because naming one that
// has been terminated fails the whole request.
func (c *Catalog) Instances(ctx context.Context, region string, ids []string) (map[string]appbilling.Instance, error) {
	client, err := c.client(ctx, region)
	if err != nil {
		return nil, err
	}
	found := make(map[string]appbilling.Instance, len(ids))
	paginator := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{{Name: aws.String("instance-id"), Values: ids}},
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				zone := ""
				if instance.Placement != nil {
					zone = aws.ToString(instance.Placement.AvailabilityZone)
				}
				found[aws.ToString(instance.InstanceId)] = appbilling.Instance{
					Type:             string(instance.InstanceType),
					AvailabilityZone: zone,
					Spot:             instance.InstanceLifecycle == ec2types.InstanceLifecycleTypeSpot,
				}
			}
		}
	}
	return found, nil
}

func (c *Catalog) SpotPrice(ctx context.Context, region, instanceType, zone string) (float64, error) {
	key := "spot/" + zone + "/" + instanceType
	if price, ok := c.cached(key, spotPriceTTL); ok {
		return price, nil
	}
	client, err := c.client(ctx, region)
	if err != nil {
		return 0, err
	}
	out, err := client.DescribeSpotPriceHistory(ctx, &ec2.DescribeSpotPriceHistoryInput{
		InstanceTypes:       []ec2types.InstanceType{ec2types.InstanceType(instanceType)},
		ProductDescriptions: []string{"Linux/UNIX"},
		AvailabilityZone:    aws.String(zone),
		StartTime:           aws.Time(time.Now()),
		MaxResults:          aws.Int32(1),
	})
	if err != nil {
		return 0, err
	}
	if len(out.SpotPriceHistory) == 0 {
		return 0, fmt.Errorf("no spot price for %s in %s", instanceType, zone)
	}
	price, err := strconv.ParseFloat(aws.ToString(out.SpotPriceHistory[0].SpotPrice), 64)
	if err != nil {
		return 0, fmt.Errorf("parse spot price: %w", err)
	}
	c.remember(key, price)
	return price, nil
}

func (c *Catalog) OnDemandPrice(ctx context.Context, region, instanceType string) (float64, error) {
	key := "on-demand/" + region + "/" + instanceType
	if price, ok := c.cached(key, onDemandPriceTTL); ok {
		return price, nil
	}
	prices, err := c.onDemand.Fetch(ctx, portspricing.Request{Region: region, InstanceTypes: []string{instanceType}})
	if err != nil {
		return 0, err
	}
	for _, p := range prices {
		if strings.EqualFold(p.InstanceType, instanceType) && p.Pricing.TotalPerHour > 0 {
			c.remember(key, p.Pricing.TotalPerHour)
			return p.Pricing.TotalPerHour, nil
		}
	}
	return 0, fmt.Errorf("no on-demand price for %s in %s", instanceType, region)
}
