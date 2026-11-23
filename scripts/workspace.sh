#!/bin/sh
# Assembles the Go workspace pix-in-a-box builds in: this module plus the
# component repositories it imports (brpay, ledger-go, pix-psp-simulator,
# webhook-relay-go, and fraud-rules-go for tests).
#
#   scripts/workspace.sh <dir>
#
# Each component is taken from <dir>/<name> when a checkout is already
# there (local development with sibling clones: `scripts/workspace.sh ..`),
# otherwise shallow-cloned from GitHub at the ref below. The generated
# go.work is git-ignored; it is the only place local paths ever appear.
#
# Refs can be pinned per component, e.g. LEDGER_GO_REF=v0.1.0. Untagged
# components default to main: "monorepo-style workspace over public repos".
# CONTRACTS=1 also fetches payments-service-go for scripts/check-contracts.sh.
set -eu

deps=${1:?usage: scripts/workspace.sh <dir>}
root=$(cd "$(dirname "$0")/.." && pwd)
base=${COMPONENTS_BASE_URL:-https://github.com/victorcondackdev}
mkdir -p "$deps"
deps=$(cd "$deps" && pwd)

fetch() {
	name=$1
	ref=$2
	if [ -f "$deps/$name/go.mod" ]; then
		echo "workspace: using $deps/$name"
		return
	fi
	echo "workspace: cloning $name@$ref"
	git clone --quiet --depth 1 --branch "$ref" "$base/$name.git" "$deps/$name"
}

fetch brpay "${BRPAY_REF:-v0.1.0}"
fetch ledger-go "${LEDGER_GO_REF:-main}"
fetch pix-psp-simulator "${PIXSIM_REF:-main}"
fetch webhook-relay-go "${RELAY_REF:-main}"
fetch fraud-rules-go "${FRAUD_RULES_REF:-main}"
# payments-service-go is not a Go dependency (it runs as a container); its
# source is only needed by the contract checks.
if [ "${CONTRACTS:-0}" = 1 ]; then
	fetch payments-service-go "${PAYMENTS_REF:-main}"
fi

cd "$root"
rm -f go.work go.work.sum
go work init . \
	"$deps/brpay" \
	"$deps/ledger-go" \
	"$deps/pix-psp-simulator" \
	"$deps/webhook-relay-go" \
	"$deps/fraud-rules-go"
echo "workspace: wrote $root/go.work"
