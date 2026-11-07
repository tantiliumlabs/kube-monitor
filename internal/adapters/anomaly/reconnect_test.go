package anomaly

// These tests need a real broker and skip without one:
//   docker run -d --rm -p 5672:5672 rabbitmq:3.13-alpine
//   RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test ./internal/adapters/anomaly/
// A TCP proxy sits in front of the broker so a test can drop the connection or
// make the broker unreachable without restarting it.

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type brokerProxy struct {
	t      *testing.T
	target string
	mu     sync.Mutex
	ln     net.Listener
	addr   string
	conns  []net.Conn
}

func newBrokerProxy(t *testing.T, target string) *brokerProxy {
	t.Helper()
	p := &brokerProxy{t: t, target: target}
	p.listen("127.0.0.1:0")
	t.Cleanup(p.stop)
	return p
}

func (p *brokerProxy) listen(addr string) {
	p.t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		p.t.Fatalf("listen: %v", err)
	}
	p.mu.Lock()
	p.ln, p.addr = ln, ln.Addr().String()
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
}

// stop refuses new connections and drops the current ones.
func (p *brokerProxy) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *brokerProxy) url() string { return "amqp://guest:guest@" + p.addr + "/" }

func testBroker(t *testing.T) (target string, admin *amqp.Channel) {
	t.Helper()
	raw := os.Getenv("RABBITMQ_TEST_URL")
	if raw == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := amqp.Dial(raw)
	if err != nil {
		t.Fatalf("dial test broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	admin, err = conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return u.Host, admin
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s.%d", prefix, time.Now().UnixNano())
}

func TestPublisherRecoversFromDownAtStartAndConnectionDrop(t *testing.T) {
	target, admin := testBroker(t)
	proxy := newBrokerProxy(t, target)
	exchange, queue := uniqueName("test.ex"), uniqueName("test.q")
	t.Cleanup(func() {
		_, _ = admin.QueueDelete(queue, false, false, false)
		_ = admin.ExchangeDelete(exchange, false, false)
	})

	// Broker unreachable when the service starts.
	addr := proxy.addr
	proxy.stop()
	link := newPublisherLink("amqp://guest:guest@"+addr+"/", exchange)
	t.Cleanup(link.close)
	msg := amqp.Publishing{Body: []byte("x")}
	if err := link.publish(context.Background(), "k", msg); err == nil {
		t.Fatal("publish with the broker down should return an error")
	}
	proxy.listen(addr)

	publishEventually := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			err := link.publish(context.Background(), "k", msg)
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("publisher never recovered: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	publishEventually()

	// The link must have declared the exchange itself.
	if _, err := admin.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueueBind(queue, "#", exchange, false, nil); err != nil {
		t.Fatalf("exchange was not declared: %v", err)
	}

	// Connection drops mid-life.
	proxy.stop()
	time.Sleep(300 * time.Millisecond)
	proxy.listen(addr)
	publishEventually()

	deadline := time.Now().Add(10 * time.Second)
	for {
		d, ok, err := admin.Get(queue, true)
		if err != nil {
			t.Fatal(err)
		}
		if ok && string(d.Body) == "x" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("message published after the drop never arrived")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
