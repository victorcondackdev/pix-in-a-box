package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/victorcondackdev/ledger-go/ledger"
	"github.com/victorcondackdev/pix-psp-simulator/webhook"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/payments"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
)

// Handler returns the gateway's HTTP API.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	w := s.idem.Wrap

	mux.HandleFunc("POST /v1/accounts", w(s.hOpenAccount))
	mux.HandleFunc("GET /v1/accounts/{id}", s.hAccount)

	mux.HandleFunc("POST /v1/pix/transfers", w(s.hCreateTransfer))
	mux.HandleFunc("GET /v1/pix/transfers", s.hListTransfers)
	mux.HandleFunc("GET /v1/pix/transfers/{id}", s.hTransfer)
	mux.HandleFunc("POST /v1/pix/transfers/{id}/confirm", s.hConfirm)
	mux.HandleFunc("POST /v1/pix/transfers/{id}/release", s.hRelease)
	mux.HandleFunc("POST /v1/pix/transfers/{id}/cancel", s.hCancel)
	mux.HandleFunc("POST /v1/pix/transfers/{id}/refunds", w(s.hRefund))
	mux.HandleFunc("POST /v1/pix/transfers/{id}/med", w(s.hMED))
	mux.HandleFunc("GET /v1/pix/received/{e2e}", s.hReceived)

	mux.HandleFunc("POST /v1/charges", s.hCreateCharge)
	mux.HandleFunc("GET /v1/charges/{id}", s.hCharge)

	mux.HandleFunc("POST /v1/webhooks/spi", s.hSPIWebhook)

	mux.HandleFunc("GET /v1/ledger/accounts/{id}/statement", s.hStatement)
	mux.HandleFunc("GET /v1/ledger/trial-balance", s.hTrialBalance)
	mux.HandleFunc("GET /v1/ledger/verify", s.hVerify)
	mux.HandleFunc("GET /v1/reconciliation/spi", s.hReconcile)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.hReady)
	mux.Handle("GET /metrics", s.metrics.Registry.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/not-found", Title: "Not found", Status: http.StatusNotFound})
	})
	return httpx.Instrument(mux, s.metrics.HTTP, s.log)
}

func (s *Service) fail(w http.ResponseWriter, r *http.Request, err error) {
	if p, ok := asProblem(err); ok {
		httpx.WriteProblem(w, r, httpx.Problem{Type: p.typ, Title: p.title, Status: p.status, Detail: p.detail, Extensions: p.ext})
		return
	}
	s.log.ErrorContext(r.Context(), "request failed", slog.String("path", r.URL.Path), slog.Any("error", err))
	httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/internal-error", Title: "Internal error", Status: http.StatusInternalServerError})
}

func (s *Service) hOpenAccount(w http.ResponseWriter, r *http.Request) {
	var req OpenAccountRequest
	if !httpx.DecodeJSON(w, r, &req) {
		return
	}
	v, err := s.OpenAccount(r.Context(), req)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/accounts/"+v.ID)
	httpx.WriteJSON(w, http.StatusCreated, v)
}

func (s *Service) hAccount(w http.ResponseWriter, r *http.Request) {
	v, err := s.Account(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Service) hCreateTransfer(w http.ResponseWriter, r *http.Request) {
	var req TransferRequest
	if !httpx.DecodeJSON(w, r, &req) {
		return
	}
	d, err := simDirectivesFrom(r.Header, s.cfg.SimHeaders)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	req.sim = d
	t, status, err := s.CreateTransfer(r.Context(), req)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/pix/transfers/"+t.ID)
	httpx.WriteJSON(w, status, t)
}

func (s *Service) hTransfer(w http.ResponseWriter, r *http.Request) {
	t, ok := s.Transfer(r.PathValue("id"))
	if !ok {
		s.fail(w, r, errNotFound)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, t)
}

func (s *Service) hListTransfers(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfers": s.Transfers(r.URL.Query().Get("account_id"))})
}

func (s *Service) hConfirm(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChallengeID string `json:"challenge_id"`
	}
	if !httpx.DecodeJSON(w, r, &req) {
		return
	}
	t, err := s.Confirm(r.Context(), r.PathValue("id"), req.ChallengeID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, t)
}

func (s *Service) hRelease(w http.ResponseWriter, r *http.Request) {
	t, err := s.Release(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, t)
}

func (s *Service) hCancel(w http.ResponseWriter, r *http.Request) {
	t, err := s.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, t)
}

func (s *Service) hRefund(w http.ResponseWriter, r *http.Request) {
	var req RefundRequest
	if !httpx.DecodeJSON(w, r, &req) {
		return
	}
	d, err := simDirectivesFrom(r.Header, s.cfg.SimHeaders)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ref, t, err := s.RequestRefund(r.Context(), r.PathValue("id"), req, d.chaos)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := http.StatusCreated
	if ref.Status != "ACSC" {
		status = http.StatusUnprocessableEntity
	}
	httpx.WriteJSON(w, status, map[string]any{"refund": ref, "transfer": t})
}

func (s *Service) hMED(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Details string `json:"details"`
	}
	if !httpx.DecodeJSON(w, r, &req) {
		return
	}
	res, err := s.ReportFraud(r.Context(), r.PathValue("id"), req.Details)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, res)
}

func (s *Service) hReceived(w http.ResponseWriter, r *http.Request) {
	in, ok := s.Received(r.PathValue("e2e"))
	if !ok {
		s.fail(w, r, errNotFound)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, in)
}

// hCreateCharge proxies to payments-service, which owns charges, their BR
// Code and their idempotency (its Idempotency-Key semantics apply as-is).
func (s *Service) hCreateCharge(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpx.MaxBody))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/payload-too-large", Title: "Request body too large", Status: http.StatusRequestEntityTooLarge})
		return
	}
	resp, err := s.payments.Forward(r.Context(), http.MethodPost, "/v1/charges", body, r.Header.Get("Idempotency-Key"))
	s.relayUpstream(w, r, resp, err)
}

func (s *Service) hCharge(w http.ResponseWriter, r *http.Request) {
	resp, err := s.payments.Forward(r.Context(), http.MethodGet, payments.ChargePath(r.PathValue("id")), nil, "")
	s.relayUpstream(w, r, resp, err)
}

func (s *Service) relayUpstream(w http.ResponseWriter, r *http.Request, resp *httpx.Response, err error) {
	if err != nil {
		s.fail(w, r, problem(http.StatusBadGateway, "payments-unavailable", "payments-service unavailable", err.Error()))
		return
	}
	for _, h := range []string{"Content-Type", "Location", "Idempotent-Replayed", "Retry-After"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

// hSPIWebhook receives the simulator's signed webhooks. The signature is
// verified with the simulator's own webhook package; deduplication is on
// the event ID, which is stable across retries and chaos duplicates.
func (s *Service) hSPIWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpx.MaxBody))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/payload-too-large", Title: "Request body too large", Status: http.StatusRequestEntityTooLarge})
		return
	}
	evType := r.Header.Get(webhook.EventTypeHeader)
	if err := webhook.Verify([]byte(s.cfg.PixSimWebhookSecret), r.Header.Get(webhook.SignatureHeader), body, webhookTolerance); err != nil {
		s.metrics.SPIWebhooks.Inc(evType, "invalid_signature")
		s.log.WarnContext(r.Context(), "SPI webhook rejected", slog.Any("error", err))
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/invalid-signature", Title: "Invalid webhook signature", Status: http.StatusUnauthorized})
		return
	}
	var ev webhook.Event
	if err := json.Unmarshal(body, &ev); err != nil || ev.ID == "" {
		s.metrics.SPIWebhooks.Inc(evType, "malformed")
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/malformed-request", Title: "Malformed event", Status: http.StatusBadRequest})
		return
	}
	result, err := s.HandleSPIEvent(r.Context(), ev)
	if err != nil {
		s.metrics.SPIWebhooks.Inc(ev.Type, "retry")
		s.log.WarnContext(r.Context(), "SPI webhook not processed; asking for redelivery", slog.String("event_id", ev.ID), slog.String("type", ev.Type), slog.Any("error", err))
		status := http.StatusServiceUnavailable
		if !errors.Is(err, errRetry) {
			status = http.StatusInternalServerError
		}
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/retry-later", Title: "Event not processed", Status: status})
		return
	}
	s.metrics.SPIWebhooks.Inc(ev.Type, result)
	s.log.InfoContext(r.Context(), "SPI webhook", slog.String("event_id", ev.ID), slog.String("type", ev.Type), slog.String("result", result),
		slog.String("delivery_attempt", r.Header.Get("X-Delivery-Attempt")))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.ID, "result": result})
}

func (s *Service) hStatement(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	v, err := s.Statement(r.Context(), r.PathValue("id"), ledger.StatementOptions{Cursor: q.Get("cursor"), Limit: limit, IncludeHolds: q.Get("include_holds") == "true"})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Service) hTrialBalance(w http.ResponseWriter, r *http.Request) {
	v, err := s.TrialBalance(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Service) hVerify(w http.ResponseWriter, r *http.Request) {
	v := s.VerifyChain(r.Context())
	status := http.StatusOK
	if !v.Valid || !v.BalancesMatch {
		status = http.StatusConflict
	}
	httpx.WriteJSON(w, status, v)
}

func (s *Service) hReconcile(w http.ResponseWriter, r *http.Request) {
	v, err := s.Reconcile(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Service) hReady(w http.ResponseWriter, r *http.Request) {
	checks := s.readiness(r.Context())
	status := http.StatusOK
	for _, v := range checks {
		if v != "ok" {
			status = http.StatusServiceUnavailable
		}
	}
	httpx.WriteJSON(w, status, checks)
}
