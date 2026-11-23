package gateway

import (
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/metrics"
)

// Metrics are the gateway's Prometheus families. The names are the
// contract with deploy/grafana/dashboards/pix.json.
type Metrics struct {
	Registry      *metrics.Registry
	HTTP          httpx.ServerMetrics
	Downstream    httpx.Downstream
	Transfers     *metrics.CounterVec   // status: every transition into a status
	Risk          *metrics.CounterVec   // action
	Postings      *metrics.CounterVec   // kind (ledger-go transaction kind)
	SPIWebhooks   *metrics.CounterVec   // type, result
	Inbound       *metrics.CounterVec   // result
	Refunds       *metrics.CounterVec   // reason, status
	Notifications *metrics.CounterVec   // event_type, result
	Settlement    *metrics.HistogramVec // status: submit → ACSC/RJCT
	Replays       *metrics.CounterVec
}

func newMetrics() *Metrics {
	reg := metrics.NewRegistry()
	return &Metrics{
		Registry:      reg,
		HTTP:          httpx.NewServerMetrics(reg, "gateway"),
		Downstream:    httpx.NewDownstream(reg, "gateway"),
		Transfers:     reg.NewCounter("gateway_pix_transfers_total", "Outgoing Pix transitions into each status.", "status"),
		Risk:          reg.NewCounter("gateway_risk_decisions_total", "Risk decisions applied to outgoing Pix, by action (UNAVAILABLE when riskd failed and the gateway failed open).", "action"),
		Postings:      reg.NewCounter("gateway_ledger_postings_total", "Ledger transactions committed, by ledger-go kind (idempotent replays excluded).", "kind"),
		SPIWebhooks:   reg.NewCounter("gateway_spi_webhooks_total", "Webhooks received from the SPI simulator, by event type and result.", "type", "result"),
		Inbound:       reg.NewCounter("gateway_pix_inbound_total", "Pix received into the settlement account, by how it was handled.", "result"),
		Refunds:       reg.NewCounter("gateway_pix_refunds_total", "Devoluções of outgoing Pix, by return reason and SPI status.", "reason", "status"),
		Notifications: reg.NewCounter("gateway_notifications_total", "Merchant notifications handed to relayd, by event type and result.", "event_type", "result"),
		Settlement:    reg.NewHistogram("gateway_pix_settlement_seconds", "Time from SPI submission to final status.", []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60}, "status"),
		Replays:       reg.NewCounter("gateway_idempotent_replays_total", "Responses replayed from the Idempotency-Key store."),
	}
}
