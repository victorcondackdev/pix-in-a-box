package gateway

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the gateway configuration. Every field comes from an
// environment variable; the defaults match docker-compose.yml.
type Config struct {
	Addr     string // GATEWAY_ADDR
	LogLevel string // GATEWAY_LOG_LEVEL

	// ISPB is this PSP's ISPB, used in the EndToEndIds it generates
	// (GATEWAY_ISPB).
	ISPB string
	// SPIAccountID is the PSP's settlement account at the simulator, the
	// stand-in for its reserves account ("conta PI") at the central bank
	// (GATEWAY_SPI_ACCOUNT_ID).
	SPIAccountID string

	PixSimURL           string // PIXSIM_URL
	PixSimWebhookSecret string // PIXSIM_WEBHOOK_SECRET: HMAC secret of the SPI webhooks

	RiskdURL     string        // RISKD_URL
	RiskTimeout  time.Duration // GATEWAY_RISK_TIMEOUT
	RiskFailOpen bool          // GATEWAY_RISK_FAIL_MODE=open|closed (default closed)

	RelayURL   string // RELAY_URL
	RelayToken string // RELAY_API_TOKEN

	PaymentsURL           string // PAYMENTS_URL
	PaymentsWebhookSecret string // PAYMENTS_WEBHOOK_SECRET (payments-service WEBHOOK_SECRET)

	// The merchant that receives Pix charges and webhook notifications.
	MerchantID            string // MERCHANT_ID
	MerchantName          string // MERCHANT_NAME
	MerchantPixKey        string // MERCHANT_PIX_KEY: must equal payments-service PIX_KEY
	MerchantWebhookURL    string // MERCHANT_WEBHOOK_URL
	MerchantWebhookSecret string // MERCHANT_WEBHOOK_SECRET (whsec_...)

	// SimHeaders forwards X-Sim-* chaos headers to the simulator and honours
	// X-Sim-Time as the transaction time sent to riskd. Sandbox only
	// (GATEWAY_SIM_HEADERS=true).
	SimHeaders bool

	StepUpTTL       time.Duration // GATEWAY_STEP_UP_TTL
	PollInterval    time.Duration // GATEWAY_POLL_INTERVAL: settlement reconciliation poll
	PollAfter       time.Duration // GATEWAY_POLL_AFTER: a submitted transfer older than this is polled
	DefaultDayLimit int64         // GATEWAY_DEFAULT_DAY_LIMIT_CENTS
	DefaultNight    int64         // GATEWAY_DEFAULT_NIGHT_LIMIT_CENTS
}

// LoadConfig reads the environment through lookup (os.LookupEnv shape).
func LoadConfig(lookup func(string) (string, bool)) (Config, error) {
	var errs []error
	str := func(k, def string) string {
		if v, ok := lookup(k); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	dur := func(k string, def time.Duration) time.Duration {
		v := str(k, "")
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s: invalid duration %q", k, v))
			return def
		}
		return d
	}
	i64 := func(k string, def int64) int64 {
		v := str(k, "")
		if v == "" {
			return def
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			errs = append(errs, fmt.Errorf("%s: invalid amount %q", k, v))
			return def
		}
		return n
	}
	c := Config{
		Addr:                  str("GATEWAY_ADDR", ":8080"),
		LogLevel:              str("GATEWAY_LOG_LEVEL", "info"),
		ISPB:                  str("GATEWAY_ISPB", "13370001"),
		SPIAccountID:          str("GATEWAY_SPI_ACCOUNT_ID", "pib"),
		PixSimURL:             str("PIXSIM_URL", "http://localhost:8090"),
		PixSimWebhookSecret:   str("PIXSIM_WEBHOOK_SECRET", ""),
		RiskdURL:              str("RISKD_URL", "http://localhost:8083"),
		RiskTimeout:           dur("GATEWAY_RISK_TIMEOUT", 500*time.Millisecond),
		RelayURL:              str("RELAY_URL", "http://localhost:8084"),
		RelayToken:            str("RELAY_API_TOKEN", ""),
		PaymentsURL:           str("PAYMENTS_URL", "http://localhost:8082"),
		PaymentsWebhookSecret: str("PAYMENTS_WEBHOOK_SECRET", ""),
		MerchantID:            str("MERCHANT_ID", "loja"),
		MerchantName:          str("MERCHANT_NAME", "Loja Exemplo"),
		MerchantPixKey:        str("MERCHANT_PIX_KEY", ""),
		MerchantWebhookURL:    str("MERCHANT_WEBHOOK_URL", ""),
		MerchantWebhookSecret: str("MERCHANT_WEBHOOK_SECRET", ""),
		SimHeaders:            str("GATEWAY_SIM_HEADERS", "") == "true",
		StepUpTTL:             dur("GATEWAY_STEP_UP_TTL", 5*time.Minute),
		PollInterval:          dur("GATEWAY_POLL_INTERVAL", 5*time.Second),
		PollAfter:             dur("GATEWAY_POLL_AFTER", 10*time.Second),
		DefaultDayLimit:       i64("GATEWAY_DEFAULT_DAY_LIMIT_CENTS", 5_000_00),
		DefaultNight:          i64("GATEWAY_DEFAULT_NIGHT_LIMIT_CENTS", 1_000_00),
	}
	switch m := str("GATEWAY_RISK_FAIL_MODE", "closed"); m {
	case "closed":
	case "open":
		c.RiskFailOpen = true
	default:
		errs = append(errs, fmt.Errorf("GATEWAY_RISK_FAIL_MODE: want open or closed, got %q", m))
	}
	if len(c.ISPB) != 8 || strings.Trim(c.ISPB, "0123456789") != "" {
		errs = append(errs, fmt.Errorf("GATEWAY_ISPB: want 8 digits, got %q", c.ISPB))
	}
	if c.PixSimWebhookSecret == "" {
		errs = append(errs, errors.New("PIXSIM_WEBHOOK_SECRET is required: SPI webhooks are always verified"))
	}
	if c.MerchantPixKey == "" {
		errs = append(errs, errors.New("MERCHANT_PIX_KEY is required"))
	}
	return c, errors.Join(errs...)
}

// LoadConfigFromEnv reads os.Environ.
func LoadConfigFromEnv() (Config, error) { return LoadConfig(os.LookupEnv) }
