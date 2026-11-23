#!/bin/sh
# Cheap contract test: asserts, against the component sources, that every
# route, environment variable, JSON field, metric name and shared secret
# this repository relies on exists and agrees (internal/contracts).
#
#   scripts/workspace.sh ..       # or CONTRACTS=1 scripts/workspace.sh .deps
#   scripts/check-contracts.sh
#
# Runs in seconds with no containers; CI runs it before building images.
set -eu
cd "$(dirname "$0")/.."
if [ ! -f go.work ]; then
	echo "check-contracts: no go.work; run scripts/workspace.sh <dir> first (CONTRACTS=1 to include payments-service-go)" >&2
	exit 2
fi
CONTRACTS_REQUIRED=1 exec go test -count=1 -v ./internal/contracts/
