// Package anomaly provides opt-in publishing of anomaly events to RabbitMQ.
// When RABBITMQ_URL is not set all events are logged locally and no AMQP
// connection is attempted — the service continues to work without a broker.
package anomaly

import (
	"context"
	"encoding/json"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ochestra-tech/k8s-monitor/pkg/anomaly"
)

const (
	defaultExchange   = "k8s.anomalies"
	defaultRoutingKey = "anomaly"
)

// Publisher sends anomaly events to a RabbitMQ topic exchange.
// All fields are safe to use after construction via NewPublisher.
type Publisher struct {
	link *publisherLink
}

// NewPublisher prepares the publisher and tries to connect. A broker that's down
// isn't fatal: each publish tries to reconnect, and events are logged until then.
// Returns nil (not an error) when RABBITMQ_URL is unset — callers must check for nil
// before calling Publish.
func NewPublisher() *Publisher {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return nil
	}
	exchange := os.Getenv("ANOMALY_RABBITMQ_EXCHANGE")
	if exchange == "" {
		exchange = defaultExchange
	}
	return &Publisher{link: newPublisherLink(url, exchange)}
}

// Publish sends a slice of AnomalyEvents.  Each event is routed as
// "anomaly.{severity}" so consumers can subscribe to severity subsets.
// If p is nil (no broker configured) the events are only logged.
func (p *Publisher) Publish(ctx context.Context, events []anomaly.AnomalyEvent) {
	for _, ev := range events {
		body, err := json.Marshal(ev)
		if err != nil {
			log.Printf("[anomaly publisher] marshal error: %v", err)
			continue
		}

		routingKey := defaultRoutingKey + "." + string(ev.Severity)

		if p == nil {
			log.Printf("[anomaly] %s cluster=%s metric=%s value=%.2f z=%.2f",
				ev.Severity, ev.ClusterID, ev.Metric, ev.Value, ev.ZScore)
			continue
		}

		if err := p.link.publish(ctx, routingKey, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    ev.DetectedAt,
			Body:         body,
		}); err != nil {
			log.Printf("[anomaly publisher] publish error (routing_key=%s): %v", routingKey, err)
		}
	}
}

// Close releases the AMQP channel and connection.  Safe to call on nil.
func (p *Publisher) Close() {
	if p == nil {
		return
	}
	p.link.close()
}
