package gateway

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/relay"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// Merchant notification event types (relayd endpoint filters "pix.*").
const (
	EventPayoutSettled  = "pix.payout.settled"
	EventPayoutRejected = "pix.payout.rejected"
	EventPayoutBlocked  = "pix.payout.blocked"
	EventPayoutHeld     = "pix.payout.held"
	EventPayoutRefunded = "pix.payout.refunded"
)

type notification struct {
	eventType   string
	key         string
	payload     any
	traceparent string
}

// outbox hands notifications to relayd in order, retrying with backoff
// until relayd acknowledges. relayd takes it from there (WAL, retries,
// DLQ). The queue is in memory: a crash loses unsent notifications, which
// is the gap a transactional outbox closes in production (write the
// notification in the same database transaction as the ledger posting,
// as payments-service-go does).
type outbox struct {
	relay   *relay.Client
	log     *slog.Logger
	metrics *Metrics

	mu      sync.Mutex
	queue   []notification
	wake    chan struct{}
	pending int
}

func newOutbox(r *relay.Client, log *slog.Logger, m *Metrics) *outbox {
	return &outbox{relay: r, log: log, metrics: m, wake: make(chan struct{}, 1)}
}

// enqueue schedules a notification. key makes relayd deduplicate retries.
func (o *outbox) enqueue(ctx context.Context, eventType, key string, payload any) {
	tp := ""
	if s, ok := trace.From(ctx); ok {
		tp = s.String()
	}
	o.mu.Lock()
	o.queue = append(o.queue, notification{eventType: eventType, key: key, payload: payload, traceparent: tp})
	o.pending = len(o.queue)
	o.mu.Unlock()
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *outbox) depth() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending
}

// run delivers until ctx is done.
func (o *outbox) run(ctx context.Context) {
	backoff := 250 * time.Millisecond
	for {
		o.mu.Lock()
		var n *notification
		if len(o.queue) > 0 {
			n = &o.queue[0]
		}
		o.mu.Unlock()
		if n == nil {
			select {
			case <-ctx.Done():
				return
			case <-o.wake:
				continue
			}
		}
		nctx := trace.Continue(ctx, n.traceparent)
		cctx, cancel := context.WithTimeout(nctx, 10*time.Second)
		msg, err := o.relay.Publish(cctx, n.eventType, n.key, n.payload)
		cancel()
		if err != nil {
			o.metrics.Notifications.Inc(n.eventType, "retry")
			o.log.WarnContext(nctx, "relay publish failed; retrying", slog.String("event_type", n.eventType), slog.String("key", n.key), slog.Any("error", err), slog.Duration("backoff", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 250 * time.Millisecond
		o.metrics.Notifications.Inc(n.eventType, "published")
		o.log.InfoContext(nctx, "merchant notification published", slog.String("event_type", n.eventType), slog.String("message_id", msg.ID), slog.Int("deliveries", len(msg.Deliveries)))
		o.mu.Lock()
		o.queue = o.queue[1:]
		o.pending = len(o.queue)
		o.mu.Unlock()
	}
}
