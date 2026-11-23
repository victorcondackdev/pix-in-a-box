#!/usr/bin/env bash
# Narrated end-to-end scenarios against a running stack (docker compose up).
#
#   ./scripts/demo.sh            narrate; report failed expectations at the end
#   ./scripts/demo.sh --assert   stop at the first failed expectation, exit 1 (CI)
#
# Requires bash, curl and jq. Every run uses fresh customers and payees,
# so the script can be re-run against the same stack. Endpoints can be
# overridden with GATEWAY, PIXSIM, MERCHANT, RELAY, RISKD.
set -euo pipefail

GATEWAY=${GATEWAY:-http://localhost:8080}
PIXSIM=${PIXSIM:-http://localhost:8090}
MERCHANT=${MERCHANT:-http://localhost:8070}
RELAY=${RELAY:-http://localhost:8084}
RISKD=${RISKD:-http://localhost:8083}
RELAY_TOKEN=${RELAY_TOKEN:-relay-local-dev-token}

ASSERT=0
for arg in "$@"; do
	case $arg in
	--assert) ASSERT=1 ;;
	-h | --help)
		sed -n '2,10p' "$0"
		exit 0
		;;
	*)
		echo "unknown argument: $arg" >&2
		exit 2
		;;
	esac
done

for bin in curl jq; do
	command -v "$bin" >/dev/null || {
		echo "demo.sh needs $bin" >&2
		exit 2
	}
done

if [ -t 1 ]; then
	B=$'\e[1m' D=$'\e[2m' G=$'\e[32m' R=$'\e[31m' Y=$'\e[33m' C=$'\e[36m' N=$'\e[0m'
else
	B='' D='' G='' R='' Y='' C='' N=''
fi

RUN=$(date +%s)-$RANDOM
FAILURES=0
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

title() { printf '\n%s━━ %s%s\n' "$B$C" "$*" "$N"; }
say() { printf '   %s\n' "$*"; }
detail() { printf '   %s%s%s\n' "$D" "$*" "$N"; }
ok() { printf '   %s✓%s %s\n' "$G" "$N" "$*"; }

fail() {
	printf '   %s✗ %s%s\n' "$R" "$*" "$N"
	if [ -s "$TMP/body" ]; then
		printf '%s' "$D"
		jq . "$TMP/body" 2>/dev/null | head -40 | sed 's/^/     /' || head -c 2000 "$TMP/body"
		printf '%s\n' "$N"
	fi
	FAILURES=$((FAILURES + 1))
	if [ "$ASSERT" = 1 ]; then
		printf '\n%sexpectation failed (--assert): stopping%s\n' "$R" "$N"
		exit 1
	fi
}

# expect <description> <jq boolean expression over the last response body>
expect() {
	if jq -e "$2" "$TMP/body" >/dev/null 2>&1; then ok "$1"; else fail "$1  [jq: $2]"; fi
}

expect_status() {
	if [ "$STATUS" = "$1" ]; then ok "HTTP $STATUS ${2:-}"; else fail "expected HTTP $1, got $STATUS ${2:-}"; fi
}

# call <METHOD> <URL> [JSON body] [header ...]  →  $STATUS, $TMP/body, $TMP/headers
call() {
	local method=$1 url=$2 data=${3:-}
	shift 2
	if [ $# -gt 0 ]; then shift; fi
	local args=(-sS -X "$method" -o "$TMP/body" -D "$TMP/headers" -w '%{http_code}' -H 'Accept: application/json')
	if [ -n "$data" ]; then args+=(-H 'Content-Type: application/json' --data "$data"); fi
	for h in "$@"; do args+=(-H "$h"); done
	STATUS=$(curl "${args[@]}" "$url") || STATUS=000
}

jb() { jq -r "$1" "$TMP/body"; }
header() { grep -i "^$1:" "$TMP/headers" | head -1 | cut -d' ' -f2- | tr -d '\r'; }
brl() { # cents → "R$ 1.234,56"
	awk -v c="$1" 'BEGIN {
		s = (c < 0) ? "-" : ""; c = (c < 0) ? -c : c
		i = sprintf("%d", int(c / 100)); o = ""
		while (length(i) > 3) { o = "." substr(i, length(i) - 2) o; i = substr(i, 1, length(i) - 3) }
		printf "R$ %s%s%s,%02d", s, i, o, c % 100
	}'
}

# wait_until <seconds> <description> <command...>: retries the command
# (which usually calls `call` and checks the body) every 250 ms.
wait_until() {
	local secs=$1 what=$2
	shift 2
	local deadline=$(($(date +%s) + secs))
	while true; do
		if "$@" >/dev/null 2>&1; then return 0; fi
		if [ "$(date +%s)" -ge "$deadline" ]; then
			fail "timed out after ${secs}s waiting for $what"
			return 1
		fi
		sleep 0.25
	done
}

transfer_is() { # <id> <status>
	call GET "$GATEWAY/v1/pix/transfers/$1"
	[ "$(jb .status)" = "$2" ]
}

merchant_has() { # <event type> <jq filter on .data that must be true>
	call GET "$MERCHANT/events?type=$1"
	jq -e --arg t "$1" "[.events[] | select($2)] | length > 0" "$TMP/body" >/dev/null
}

iso() { jq -nr --argjson t "$1" '$t | todate'; }

# Deterministic risk clock: the gateway sends X-Sim-Time to riskd as the
# transaction time (GATEWAY_SIM_HEADERS=true), so the night rules behave
# the same whatever hour CI runs. Brasília is UTC-3 all year.
NOW=$(date -u +%s)
DAY=$((NOW - ((NOW - 61200) % 86400)))   # latest 14:00 in São Paulo
NIGHT=$((NOW - ((NOW - 19800) % 86400))) # latest 02:30 in São Paulo
OPENED=2024-01-10T12:00:00Z
TRUSTED_DEVICE='{"id":"dev-trusted","trusted":true,"first_seen":"2025-01-01T00:00:00Z"}'

open_account() { # <id> <deposit cents> [extra json]
	local body extra=${3:-'{}'}
	body=$(jq -nc --arg id "$1" --argjson dep "$2" --arg opened "$OPENED" --argjson extra "$extra" \
		'{id: $id, name: ("Cliente " + $id), initial_deposit_cents: $dep, opened_at: $opened} + $extra')
	call POST "$GATEWAY/v1/accounts" "$body" "Idempotency-Key: open-$1"
	[ "$STATUS" = 201 ] || fail "opening account $1 (HTTP $STATUS)"
}

# A payee at another bank: the counterparty PSP opens the account and
# registers a random (EVP) key in the DICT.
new_payee() { # <name> <ispb> → prints the key
	call POST "$PIXSIM/accounts" "$(jq -nc --arg n "$1" --arg ispb "$2" '{owner_name: $n, document: "390.533.447-05", ispb: $ispb}')"
	[ "$STATUS" = 201 ] || {
		fail "simulator: opening payee account (HTTP $STATUS)"
		return 1
	}
	local acct
	acct=$(jb .id)
	call POST "$PIXSIM/dict/keys" "$(jq -nc --arg a "$acct" '{account_id: $a, evp: true}')"
	[ "$STATUS" = 201 ] || {
		fail "simulator: registering key (HTTP $STATUS)"
		return 1
	}
	jb .key
}

pay() { # <account> <key> <cents> <idempotency key> <sim time epoch> [device json] [header ...]
	local acct=$1 key=$2 cents=$3 idem=$4 at=$5 dev=${6:-$TRUSTED_DEVICE}
	shift 6 2>/dev/null || shift $#
	call POST "$GATEWAY/v1/pix/transfers" \
		"$(jq -nc --arg a "$acct" --arg k "$key" --argjson c "$cents" --argjson d "$dev" \
			'{account_id: $a, key: $k, amount_cents: $c, description: "pix-in-a-box demo", channel: "app", device: $d}')" \
		"Idempotency-Key: $idem" "X-Sim-Time: $(iso "$at")" "$@"
}

metric() { # <url> <series prefix> → sum of matching samples (0 when absent)
	curl -sS "$1/metrics" | awk -v p="$2" 'index($0, p) == 1 { s += $NF } END { printf "%d", s }'
}

riskd_recorded_more_than() { [ "$(metric "$RISKD" riskd_events_recorded_total)" -gt "$1" ]; }

received_forwarded() { # <e2e>
	call GET "$GATEWAY/v1/pix/received/$1"
	[ "$STATUS" = 200 ] && [ "$(jb .forward_result)" = settled ]
}

transfer_final() { # <id>
	call GET "$GATEWAY/v1/pix/transfers/$1"
	jq -e '.status == "SETTLED" or .status == "REJECTED" or .status == "FAILED"' "$TMP/body"
}

nothing_in_flight() {
	call GET "$GATEWAY/v1/reconciliation/spi"
	jq -e '.in_flight_cents == 0 and .in_sync == true' "$TMP/body"
}

# ---------------------------------------------------------------------------
title "pix-in-a-box demo (run $RUN)"
call GET "$GATEWAY/readyz"
if [ "$STATUS" != 200 ]; then
	fail "gateway not ready at $GATEWAY (is the stack up? docker compose up -d --build --wait)"
	exit 1
fi
ok "gateway ready: $(jq -c . "$TMP/body")"
detail "risk clock: day $(iso "$DAY"), night $(iso "$NIGHT")"

# ---------------------------------------------------------------------------
title "a) Happy path: Pix payout settled, merchant notified through relayd"
say "DICT lookup → riskd → ledger hold → SPI (ACSP) → signed webhook ACSC → post → riskd velocity → relayd → merchant"
ANA=ana-$RUN
open_account "$ANA" 100000
JOAO=$(new_payee "Joao Lima" 10000002)
detail "customer $ANA funded with $(brl 100000); payee key $JOAO"

call POST "$MERCHANT/admin/fail-next" '{"count":1}'
detail "merchant will fail its next delivery with 503, so relayd has to retry"

pay "$ANA" "$JOAO" 12500 "pay-a-$RUN" "$DAY" "$TRUSTED_DEVICE" "X-Sim-Webhook-Duplicates: 2"
expect_status 202 "(accepted, settlement is asynchronous)"
A_ID=$(jb .id)
A_E2E=$(jb .end_to_end_id)
A_TRACE=$(header X-Trace-Id)
say "transfer $A_ID  EndToEndId $A_E2E"
say "risk: $(jb '.risk.action') score $(jb '.risk.score') — $(jb '.risk.reasons | join("; ")')"
expect "admitted by risk (ALLOW or REVIEW)" '.risk.action == "ALLOW" or .risk.action == "REVIEW"'
expect "funds held in the ledger (two-phase)" '.ledger.hold != null and .ledger.hold != ""'

pay "$ANA" "$JOAO" 12500 "pay-a-$RUN" "$DAY" "$TRUSTED_DEVICE" "X-Sim-Webhook-Duplicates: 2"
if [ "$(jb .id)" = "$A_ID" ] && [ "$(header Idempotent-Replayed)" = "true" ]; then
	ok "retry with the same Idempotency-Key replays the original transfer"
else
	fail "idempotent replay returned $(jb .id)"
fi

wait_until 20 "transfer $A_ID SETTLED" transfer_is "$A_ID" SETTLED && ok "SPI settled it (ACSC); hold posted"
call GET "$GATEWAY/v1/accounts/$ANA"
expect "balance $(brl 87500) after the payout" '.balance_cents == 87500 and .available_cents == 87500'
DUPS=$(metric "$GATEWAY" 'gateway_spi_webhooks_total{type="pix.settled",result="duplicate"}')
if [ "$DUPS" -ge 2 ]; then ok "SPI chaos duplicates ignored on event ID ($DUPS duplicate deliveries)"; else fail "expected ≥2 duplicate pix.settled deliveries, saw $DUPS"; fi

wait_until 40 "merchant webhook pix.payout.settled" merchant_has pix.payout.settled ".data.id == \"$A_ID\"" &&
	ok "merchant received pix.payout.settled (Standard Webhooks signature verified)"
call GET "$MERCHANT/events?type=pix.payout.settled"
WEBHOOK_ID=$(jq -r --arg id "$A_ID" '[.events[] | select(.data.id == $id)][0].webhook_id' "$TMP/body")
MERCHANT_TRACE=$(jq -r --arg id "$A_ID" '[.events[] | select(.data.id == $id)][0].trace_id' "$TMP/body")
call GET "$RELAY/v1/messages/$WEBHOOK_ID/attempts" "" "Authorization: Bearer $RELAY_TOKEN"
expect "relayd retried after the merchant's 503 (≥ 2 attempts, same webhook-id)" '(.data | length) >= 2'
if [ -n "$A_TRACE" ] && [ "$MERCHANT_TRACE" = "$A_TRACE" ]; then
	ok "one trace end to end: $A_TRACE (gateway → relayd → merchant)"
else
	fail "trace id at the merchant ($MERCHANT_TRACE) differs from the gateway's ($A_TRACE)"
fi

# ---------------------------------------------------------------------------
title "b) Account takeover at night: burst blocked by riskd, with the explanation"
say "02:30 in São Paulo, a device first seen 12 minutes ago, two quick transfers to fresh keys"
VIC=vitima-$RUN
open_account "$VIC" 500000 '{"night_limit_cents":100000,"day_limit_cents":500000}'
MULA1=$(new_payee "Conta Mula Um" 10000009)
MULA2=$(new_payee "Conta Mula Dois" 10000009)
NEW_DEVICE=$(jq -nc --arg d "dev-ato-$RUN" --arg fs "$(iso $((NIGHT - 720)))" '{id: $d, trusted: false, first_seen: $fs}')
RECORDED=$(metric "$RISKD" riskd_events_recorded_total)

pay "$VIC" "$MULA1" 18000 "ato-1-$RUN" "$NIGHT" "$NEW_DEVICE"
expect_status 202 "first transfer, $(brl 18000)"
B1=$(jb .id)
say "risk: $(jb '.risk.action') — $(jb '.risk.reasons | join("; ")')"
wait_until 20 "first transfer settled" transfer_is "$B1" SETTLED && ok "first transfer settled"
wait_until 10 "riskd velocity updated" riskd_recorded_more_than "$RECORDED" &&
	ok "settled transfer recorded in riskd's velocity store (count(payer.transfers, 30m) = 1)"

pay "$VIC" "$MULA2" 45000 "ato-2-$RUN" $((NIGHT + 180)) "$NEW_DEVICE"
expect_status 422 "second transfer refused"
expect "status BLOCKED by risk" '.status == "BLOCKED" and .risk.action == "BLOCK"'
expect "ato.new_device.night_drain among the matched rules" '[.risk.hits[].rule_id] | index("ato.new_device.night_drain") != null'
say "reasons:"
jq -r '.risk.reasons[] | "     · " + .' "$TMP/body"
say "explanation of ato.new_device.night_drain:"
jq -r '.risk.hits[] | select(.rule_id == "ato.new_device.night_drain") | .features[] | "     · \(.name) = \(.display // .value)"' "$TMP/body"
call GET "$GATEWAY/v1/accounts/$VIC"
expect "only the first transfer left the account ($(brl 482000), nothing held)" '.balance_cents == 482000 and .available_cents == 482000'

# ---------------------------------------------------------------------------
title "c) SPI rejects with AM04 (chaos header) → ledger hold voided"
CARLA=carla-$RUN
open_account "$CARLA" 50000
PAYEE_C=$(new_payee "Loja da Esquina" 10000004)
pay "$CARLA" "$PAYEE_C" 20000 "am04-$RUN" $((DAY + 300)) "$TRUSTED_DEVICE" "X-Sim-Fail: AM04" "X-Sim-Latency: 3s"
expect_status 202
C_ID=$(jb .id)
call GET "$GATEWAY/v1/accounts/$CARLA"
expect "while in flight: balance $(brl 50000), available $(brl 30000)" '.balance_cents == 50000 and .available_cents == 30000'
wait_until 20 "transfer REJECTED" transfer_is "$C_ID" REJECTED && ok "SPI answered RJCT"
expect "reason AM04 and the hold voided" '.failure_reason == "AM04" and (.ledger.void // "") != ""'
call GET "$GATEWAY/v1/accounts/$CARLA"
expect "available back to $(brl 50000)" '.balance_cents == 50000 and .available_cents == 50000'
wait_until 30 "merchant webhook pix.payout.rejected" merchant_has pix.payout.rejected ".data.id == \"$C_ID\"" &&
	ok "merchant received pix.payout.rejected"

# ---------------------------------------------------------------------------
title "d) Devoluções and MED → ledger reversal and compensating postings"
call POST "$GATEWAY/v1/pix/transfers/$A_E2E/refunds" '{"amount_cents":12500,"reason":"MD06","description":"pedido cancelado"}' "Idempotency-Key: refund-a-$RUN"
expect_status 201 "full devolução of scenario (a)"
expect "booked as a ledger reversal (mirrored transaction linked by Ref)" '.refund.ledger_kind == "reversal" and .refund.status == "ACSC"'
call GET "$GATEWAY/v1/accounts/$ANA"
expect "$ANA back to $(brl 100000)" '.balance_cents == 100000'

DAVI=davi-$RUN
open_account "$DAVI" 100000
GOLPE=$(new_payee "Falso Parente" 10000009)
pay "$DAVI" "$GOLPE" 30000 "med-$RUN" $((DAY + 600))
D_ID=$(jb .id)
wait_until 20 "transfer settled" transfer_is "$D_ID" SETTLED && ok "$(brl 30000) paid to a scammer"
call POST "$GATEWAY/v1/pix/transfers/$D_ID/refunds" '{"amount_cents":10000,"reason":"MD06"}' "Idempotency-Key: refund-d-$RUN"
expect "partial devolução of $(brl 10000): compensating posting" '.refund.ledger_kind == "compensating" and .transfer.refunded_cents == 10000'
call POST "$GATEWAY/v1/pix/transfers/$D_ID/med" '{"details":"golpe do falso parente"}' "Idempotency-Key: med-d-$RUN"
expect_status 201 "MED case opened and driven to the end"
say "MED: $(jb '[.infraction.history[] | .from + " → " + .to] | join(", ")')"
expect "remaining $(brl 20000) returned with FR01" '.infraction.status == "REFUNDED" and .refund.reason == "FR01" and .refund.amount_cents == 20000'
expect "beneficiary now carries a fraud mark" '.beneficiary_fraud_marks == 1'
call GET "$GATEWAY/v1/accounts/$DAVI"
expect "$DAVI made whole: $(brl 100000)" '.balance_cents == 100000'
pay "$DAVI" "$GOLPE" 1000 "med-next-$RUN" $((DAY + 900))
expect "next Pix to that beneficiary: riskd sees the MED report" '[.risk.hits[].rule_id] | index("beneficiary.fraud_marks.single") != null'
D2_ID=$(jb .id)
wait_until 20 "follow-up transfer final" transfer_final "$D2_ID" || true

# ---------------------------------------------------------------------------
title "e) Customer pays a QR charge → payments-service → CloudEvent → merchant"
say "charge (payments-service, BR Code) → payer's bank pays the QR (SPI) → pix.received → gateway credits the merchant"
say "and settles the charge → outbox → Redpanda → notifier → relayd → merchant webhook charge.paid"
call POST "$GATEWAY/v1/charges" "$(jq -nc --arg d "Pedido $RUN" '{amount_cents: 4990, description: $d}')" "Idempotency-Key: charge-$RUN"
expect_status 201 "charge created by payments-service"
CH_ID=$(jb .id)
CH_BRCODE=$(jb .br_code)
detail "charge $CH_ID txid $(jb .txid)"
detail "BR Code ${CH_BRCODE:0:60}…"
call POST "$PIXSIM/pix/qr-payments" "$(jq -nc --arg b "$CH_BRCODE" '{from_account_id: "maria", brcode: $b}')"
expect_status 202 "Maria's bank paid the QR code"
E_E2E=$(jb .end_to_end_id)
wait_until 20 "gateway processed pix.received" received_forwarded "$E_E2E" &&
	ok "gateway credited merchant:loja and settled the charge"
call GET "$GATEWAY/v1/pix/received/$E_E2E"
expect "credited to the merchant's ledger account" '.credited_to == "merchant:loja" and .amount_cents == 4990'
call GET "$GATEWAY/v1/charges/$CH_ID"
expect "charge is PAID in payments-service" '.status == "PAID" and .end_to_end_id != null'
wait_until 60 "merchant webhook charge.paid" merchant_has charge.paid ".data.data.charge_id == \"$CH_ID\"" &&
	ok "merchant received charge.paid (outbox → Kafka → notifier → relayd)"

# ---------------------------------------------------------------------------
title "Books"
wait_until 20 "no transfer in flight and SPI reconciled" nothing_in_flight || true
call GET "$GATEWAY/v1/ledger/trial-balance"
expect "trial balance: debits = credits, posted and pending" '.balanced == true and .difference_cents == 0 and .pending_difference_cents == 0'
jq -r '.rows[] | select(.account | test("^(spi|clearing|equity|suspense|merchant)")) | "     \(.account | . + " " * (22 - length)) \(.balance_cents)"' "$TMP/body"
call GET "$GATEWAY/v1/ledger/verify"
expect "hash chain verifies and balances replay from entries" '.valid == true and .balances_match == true'
say "chain head: seq $(jb .seq), hash $(jb .head_hash)"
call GET "$GATEWAY/v1/reconciliation/spi"
expect "ledger reserves reconcile with the SPI settlement account" '.in_sync == true'
say "SPI balance $(brl "$(jb .spi_balance_cents)"), ledger $(brl "$(jb .ledger_posted_cents)")"

printf '\n'
if [ "$FAILURES" -eq 0 ]; then
	printf '%sall scenarios passed%s\n' "$G$B" "$N"
	printf '%sfollow scenario (a) across services: docker compose logs | grep %s%s\n' "$D" "$A_TRACE" "$N"
else
	printf '%s%d expectation(s) failed%s\n' "$R$B" "$FAILURES" "$N"
	exit 1
fi
