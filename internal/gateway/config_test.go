package gateway

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	env := map[string]string{
		"PIXSIM_WEBHOOK_SECRET": "s", "MERCHANT_PIX_KEY": "loja@pixinabox.dev",
		"GATEWAY_SIM_HEADERS": "true", "GATEWAY_STEP_UP_TTL": "2m", "GATEWAY_RISK_FAIL_MODE": "open",
	}
	c, err := LoadConfig(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if !c.SimHeaders || c.StepUpTTL != 2*time.Minute || !c.RiskFailOpen || c.ISPB != "13370001" || c.SPIAccountID != "pib" {
		t.Fatalf("%+v", c)
	}
	_, err = LoadConfig(func(k string) (string, bool) {
		return map[string]string{"GATEWAY_ISPB": "123", "GATEWAY_RISK_FAIL_MODE": "maybe", "GATEWAY_POLL_INTERVAL": "soon"}[k], true
	})
	for _, want := range []string{"GATEWAY_ISPB", "GATEWAY_RISK_FAIL_MODE", "GATEWAY_POLL_INTERVAL", "PIXSIM_WEBHOOK_SECRET", "MERCHANT_PIX_KEY"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}
