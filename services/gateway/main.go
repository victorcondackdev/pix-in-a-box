// Command gateway runs the pix-in-a-box PSP core: customer wallets on a
// double-entry ledger, outgoing Pix with risk checks and two-phase holds,
// incoming Pix and charges, refunds and MED, merchant notifications.
//
// Configuration is environment only (see internal/gateway/config.go).
// "gateway -healthcheck" probes /readyz and exits 0 or 1, for container
// health checks in an image without a shell.
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
	"syscall"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/gateway"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/logging"
)

func main() {
	health := flag.Bool("healthcheck", false, "probe /readyz on GATEWAY_ADDR and exit")
	flag.Parse()
	if *health {
		addr := os.Getenv("GATEWAY_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		if err := httpx.Probe(httpx.ProbeAddr(addr, "/readyz"), 3*time.Second); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		slog.Error("gateway stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := gateway.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout, logging.ParseLevel(cfg.LogLevel), "gateway")
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc := gateway.New(cfg, log)
	go svc.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("gateway listening", slog.String("addr", cfg.Addr), slog.Bool("sim_headers", cfg.SimHeaders))
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}
