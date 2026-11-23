# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-22

### Added

- `services/gateway`: the PSP core. Customer wallets on ledger-go
  (in-memory store), outgoing Pix with DICT lookup, risk evaluation through
  riskd (BLOCK / HOLD with cautionary delay / STEP_UP with confirmation /
  REVIEW / ALLOW), two-phase ledger holds, SPI submission with
  gateway-generated EndToEndIds, signed SPI webhooks with event-ID
  deduplication, settlement polling for lost webhooks, refunds (ledger
  reversal or compensating posting) and MED cases that feed fraud marks back
  into risk. Inbound Pix credited to merchants and forwarded to
  payments-service as settlements. Ledger statement, trial balance, hash
  chain verification and SPI reconciliation endpoints. Idempotency-Key on
  every mutating route, W3C trace context on every call, Prometheus metrics,
  JSON logs.
- `services/notifier`: payments-service CloudEvents from Kafka to relayd,
  at least once, deduplicated by CloudEvent ID.
- `services/merchant`: example receiver verifying Standard Webhooks
  signatures and deduplicating on `webhook-id`.
- `docker-compose.yml` with every component built from its repository,
  Redpanda, Postgres, Prometheus and a provisioned Grafana dashboard.
- `scripts/demo.sh`: five narrated scenarios, `--assert` for CI.
- `scripts/check-contracts.sh`: routes, environment, wire fields, metric
  names and shared secrets checked against the component sources.
- CI: unit tests with the real fraud-rules-go engine and webhook-relay-go
  engine in-process, contract checks, and an end-to-end job running the
  whole stack.

[0.1.0]: https://github.com/victorcondackdev/pix-in-a-box/releases/tag/v0.1.0
