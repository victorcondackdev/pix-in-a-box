package riskd

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"": 0, "30m": 30 * time.Minute, "1h": time.Hour, "2h": 2 * time.Hour, "7d": 7 * 24 * time.Hour,
		"1d2h30m": 26*time.Hour + 30*time.Minute, "1s500ms": 1500 * time.Millisecond, "90m": 90 * time.Minute,
	} {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("%q: %v %v, want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"m", "10", "1y", "1h-", "-1h"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// TestTransactionOmitsUnknownTimes guards the wire contract: riskd decodes
// with DisallowUnknownFields and optional times must be omitted, not sent
// as year 1.
func TestTransactionOmitsUnknownTimes(t *testing.T) {
	b, _ := json.Marshal(Transaction{ID: "x", Time: time.Unix(0, 0).UTC(), Payer: Payer{ID: "p"}, Beneficiary: Beneficiary{ID: "b"}})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	payer := m["payer"].(map[string]any)
	if _, ok := payer["account_created"]; ok {
		t.Fatalf("zero account_created serialized: %s", b)
	}
	if _, ok := payer["device"].(map[string]any)["first_seen"]; ok {
		t.Fatalf("zero first_seen serialized: %s", b)
	}
}

func TestDecisionDecodesEmbeddedOutcome(t *testing.T) {
	raw := `{"transaction_id":"t","action":"HOLD","score":40,"hold_for":"1h","reasons":["rule limit.night.cumulative → HOLD"],
	  "hits":[{"rule_id":"limit.night.cumulative","action":"HOLD","score":40,"mode":"enforce","hold_for":"1h","features":[{"name":"tx.amount","value":"95000","display":"R$ 950,00"}]}],
	  "ruleset":"pix-default@0.1.0","revision":1,"elapsed_ns":5000}`
	var d Decision
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatal(err)
	}
	if d.Action != Hold || d.HoldFor != "1h" || d.Hits[0].Features[0].Display != "R$ 950,00" {
		t.Fatalf("%+v", d)
	}
}
