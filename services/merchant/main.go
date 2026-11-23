// Command merchant is the example merchant webhook receiver.
//
//	MERCHANT_ADDR            listen address (default :8080)
//	MERCHANT_WEBHOOK_SECRET  whsec_... secret(s), comma separated during rotation
//	MERCHANT_LOG_LEVEL       debug, info, warn, error
//
// "merchant -healthcheck" probes /healthz and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/merchant"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/logging"
)

func main() {
	health := flag.Bool("healthcheck", false, "probe /healthz and exit")
	flag.Parse()
	addr := os.Getenv("MERCHANT_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if *health {
		if err := httpx.Probe(httpx.ProbeAddr(addr, "/healthz"), 3*time.Second); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	log := logging.New(os.Stdout, logging.ParseLevel(os.Getenv("MERCHANT_LOG_LEVEL")), "merchant")
	var secrets []string
	for _, s := range strings.Split(os.Getenv("MERCHANT_WEBHOOK_SECRET"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			secrets = append(secrets, s)
		}
	}
	srv, err := merchant.New(log, secrets...)
	if err != nil {
		log.Error("MERCHANT_WEBHOOK_SECRET", slog.Any("error", err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hs := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("merchant receiver listening", slog.String("addr", addr), slog.Int("secrets", len(secrets)))
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", slog.Any("error", err))
			os.Exit(1)
		}
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}
}
