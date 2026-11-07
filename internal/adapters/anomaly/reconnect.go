package anomaly

// Why this file exists: amqp091-go never reconnects by itself. When the broker
// restarts, a consumer's delivery channel just closes and a publisher's channel
// returns ErrClosed from then on, so without this a RabbitMQ restart silently
// stops the service's messaging until its pod is restarted. RabbitMQ on dev has
// no persistent storage, so exchanges and queues must be redeclared as well.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	brokerDialTimeout     = 5 * time.Second
	brokerHeartbeat       = 10 * time.Second
	publishRedialCooldown = 5 * time.Second
)

func dialBroker(url string) (*amqp.Connection, error) {
	return amqp.DialConfig(url, amqp.Config{
		Heartbeat: brokerHeartbeat,
		Locale:    "en_US",
		Dial:      amqp.DefaultDial(brokerDialTimeout),
	})
}

// publisherLink is a publisher's connection that repairs itself. If the broker
// is unavailable, publishes return an error, and it redials at most once per
// publishRedialCooldown so a broker outage doesn't make every caller wait out a
// dial timeout.
type publisherLink struct {
	url      string
	exchange string

	mu       sync.Mutex
	conn     *amqp.Connection
	ch       *amqp.Channel
	nextDial time.Time
}

// newPublisherLink tries to connect straight away, but a broker that's down at
// startup isn't fatal: the next publish tries again.
func newPublisherLink(url, exchange string) *publisherLink {
	l := &publisherLink{url: url, exchange: exchange}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.connectLocked(); err != nil {
		slog.Warn("rabbitmq publisher not connected, will retry on publish", "exchange", exchange, "err", err)
	}
	return l
}

func (l *publisherLink) publish(ctx context.Context, routingKey string, msg amqp.Publishing) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.ch == nil || l.ch.IsClosed() {
		if err := l.connectLocked(); err != nil {
			return err
		}
	}
	err := l.ch.PublishWithContext(ctx, l.exchange, routingKey, false, false, msg)
	if err == nil {
		return nil
	}
	// The connection may have died since the check above: reconnect and retry once.
	if reconnectErr := l.connectLocked(); reconnectErr != nil {
		return fmt.Errorf("publish failed (%w) and reconnect failed: %w", err, reconnectErr)
	}
	return l.ch.PublishWithContext(ctx, l.exchange, routingKey, false, false, msg)
}

func (l *publisherLink) connectLocked() error {
	if wait := time.Until(l.nextDial); wait > 0 {
		return fmt.Errorf("rabbitmq unavailable, next connection attempt in %s", wait.Round(time.Second))
	}
	l.closeLocked()

	conn, ch, err := l.open()
	if err != nil {
		l.nextDial = time.Now().Add(publishRedialCooldown)
		return err
	}
	l.conn, l.ch = conn, ch
	l.nextDial = time.Time{}
	slog.Info("rabbitmq publisher connected", "exchange", l.exchange)
	return nil
}

func (l *publisherLink) open() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := dialBroker(l.url)
	if err != nil {
		return nil, nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.ExchangeDeclare(l.exchange, "topic", true, false, false, false, nil); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("declare exchange %s: %w", l.exchange, err)
	}
	return conn, ch, nil
}

func (l *publisherLink) closeLocked() {
	if l.conn != nil {
		_ = l.conn.Close()
	}
	l.conn, l.ch = nil, nil
}

func (l *publisherLink) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closeLocked()
}
