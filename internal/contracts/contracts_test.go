// Package contracts checks, against the component sources, every
// integration assumption this repository makes: the routes the gateway
// calls, the environment variables docker-compose.yml sets, the JSON
// fields on the wire, the metric names the Grafana dashboard queries, and
// the secrets and identifiers that must agree across services.
//
// The component sources are found through the Go workspace (go.work, see
// scripts/workspace.sh); payments-service-go is not a Go dependency and is
// looked up next to the other checkouts or in PAYMENTS_SERVICE_DIR. Without
// sources the checks skip, unless CONTRACTS_REQUIRED=1 (CI).
package contracts

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/victorcondackdev/fraud-rules-go/engine"
	"github.com/victorcondackdev/fraud-rules-go/velocity"
	"github.com/victorcondackdev/webhook-relay-go/signature"
)

const root = "../.."

// sources maps a module path's last element (e.g. "ledger-go") to its
// checkout directory.
func sources(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	b, err := os.ReadFile(filepath.Join(root, "go.work"))
	if err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			dir := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "use"))
			dir = strings.Trim(dir, "()")
			if dir == "" || strings.HasPrefix(dir, "go ") || dir == "." {
				continue
			}
			if !filepath.IsAbs(dir) {
				dir = filepath.Join(root, dir)
			}
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				out[filepath.Base(dir)] = dir
			}
		}
	}
	if d := os.Getenv("PAYMENTS_SERVICE_DIR"); d != "" {
		out["payments-service-go"] = d
	} else {
		for _, d := range out {
			cand := filepath.Join(filepath.Dir(d), "payments-service-go")
			if _, err := os.Stat(filepath.Join(cand, "go.mod")); err == nil {
				out["payments-service-go"] = cand
				break
			}
		}
	}
	return out
}

func need(t *testing.T, src map[string]string, name string) string {
	t.Helper()
	d, ok := src[name]
	if !ok {
		if os.Getenv("CONTRACTS_REQUIRED") == "1" {
			t.Fatalf("source of %s not found (run scripts/workspace.sh; CONTRACTS=1 also clones payments-service-go)", name)
		}
		t.Skipf("source of %s not found", name)
	}
	return d
}

func read(t *testing.T, path ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(path...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func contains(t *testing.T, what, text string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if !strings.Contains(text, n) {
			t.Errorf("%s: %q not found", what, n)
		}
	}
}

// --- docker-compose.yml, parsed with just enough YAML for this file -------

type composeService struct {
	env   map[string]string
	lines []string
}

func compose(t *testing.T) map[string]*composeService {
	t.Helper()
	f, err := os.Open(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]*composeService{}
	var cur *composeService
	inServices, inEnv := false, false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		switch {
		case indent == 0:
			inServices = trimmed == "services:"
			cur = nil
		case inServices && indent == 2 && strings.HasSuffix(trimmed, ":"):
			cur = &composeService{env: map[string]string{}}
			out[strings.TrimSuffix(trimmed, ":")] = cur
			inEnv = false
		case cur != nil && indent == 4:
			inEnv = trimmed == "environment:"
			cur.lines = append(cur.lines, trimmed)
		case cur != nil && inEnv && indent == 6:
			k, v, _ := strings.Cut(trimmed, ":")
			cur.env[k] = strings.Trim(strings.TrimSpace(v), `"`)
		case cur != nil:
			cur.lines = append(cur.lines, trimmed)
		}
	}
	if len(out) == 0 {
		t.Fatal("no services parsed from docker-compose.yml")
	}
	return out
}

func (s *composeService) has(fragment string) bool {
	for _, l := range s.lines {
		if strings.Contains(l, fragment) {
			return true
		}
	}
	return false
}

type scenario struct {
	Name     string `json:"name"`
	Accounts []struct {
		ID           string `json:"id"`
		ISPB         string `json:"ispb"`
		BalanceCents int64  `json:"initial_balance_cents"`
		Webhook      *struct {
			URL    string `json:"url"`
			Secret string `json:"secret"`
		} `json:"webhook"`
	} `json:"accounts"`
	Keys []struct {
		AccountID string `json:"account_id"`
		Key       string `json:"key"`
	} `json:"keys"`
}

// jsonKeys returns every object key in a JSON document, recursively.
func jsonKeys(t *testing.T, raw []byte) []string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	set := map[string]bool{}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				set[k] = true
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	walk(v)
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- the checks -------------------------------------------------------------

func TestSimulatorContract(t *testing.T) {
	d := need(t, sources(t), "pix-psp-simulator")
	server := read(t, d, "internal", "api", "server.go")
	contains(t, "simulator routes", server,
		`s.handle("GET", "/healthz"`, `s.handle("GET", "/accounts/{id}"`, `s.handle("POST", "/accounts"`,
		`s.handle("POST", "/dict/keys"`, `s.handle("GET", "/dict/keys/{key}"`,
		`s.handle("POST", "/pix/transfers"`, `s.handle("GET", "/pix/transfers/{e2eid}"`,
		`s.handle("POST", "/pix/qr-payments"`,
		`s.handle("POST", "/pix/transfers/{e2eid}/refunds"`, `s.handle("GET", "/pix/transfers/{e2eid}/refunds/{id}"`,
		`s.handle("POST", "/med/infractions"`, `s.handle("POST", "/med/infractions/{id}/{action}"`)
	contains(t, "simulator headers", read(t, d, "internal", "api", "handlers.go"),
		`PayerHeader = "X-Payer-Id"`, `IdempotencyHeader = "Idempotency-Key"`)
	contains(t, "simulator chaos headers", read(t, d, "internal", "chaos", "chaos.go"),
		`"X-Sim-Fail"`, `"X-Sim-Latency"`, `"X-Sim-Webhook-Duplicates"`)
	contains(t, "simulator transfer JSON", read(t, d, "internal", "sim", "transfer.go"),
		`json:"from_account_id"`, `json:"key,omitempty"`, `json:"amount_cents"`, `json:"end_to_end_id,omitempty"`,
		`json:"end_to_end_id"`, `json:"txid,omitempty"`, `json:"reason,omitempty"`, `json:"payer"`, `json:"payee"`,
		`json:"account_id,omitempty"`, `"duplicate_end_to_end_id"`, `webhook.EventPixSettled`, `webhook.EventPixReceived`, `webhook.EventPixRejected`)
	contains(t, "simulator refund and MED JSON", read(t, d, "internal", "sim", "refund.go"),
		`json:"return_reason"`, `json:"refund_id,omitempty"`, `json:"held_cents"`, `MEDBlock   InfractionEvent = "block"`,
		`MEDAnalyze InfractionEvent = "analyze"`, `MEDAccept  InfractionEvent = "accept"`, `json:"infraction_id,omitempty"`)
	contains(t, "simulator DICT lookup JSON", read(t, d, "internal", "dict", "dict.go"),
		`json:"owner_type"`, `json:"account_number"`, `json:"created_at"`, `json:"owner_tax_id_masked"`)
	contains(t, "simulator account JSON", read(t, d, "internal", "ledger", "ledger.go"), `json:"balance_cents"`)
	contains(t, "ISO statuses", read(t, d, "internal", "iso", "iso.go"), `StatusPending  = "ACSP"`, `StatusSettled  = "ACSC"`, `StatusRejected = "RJCT"`, `AM04 = "AM04"`)

	// Environment: PIXSIM_<FLAG> for every variable compose sets.
	main := read(t, d, "cmd", "pixsim", "main.go")
	contains(t, "pixsim flags", main, `"healthcheck"`)
	svc := compose(t)["pixsim"]
	for k := range svc.env {
		flag := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(k, "PIXSIM_")), "_", "-")
		if !strings.HasPrefix(k, "PIXSIM_") || !strings.Contains(main, `"`+flag+`"`) {
			t.Errorf("compose pixsim env %s does not map to a pixsim flag", k)
		}
	}
	contains(t, "pixsim Dockerfile", read(t, d, "Dockerfile"), "AS pixsim")
	if !svc.has("target: pixsim") {
		t.Error("compose must build the pixsim target")
	}

	// The scenario file only uses fields the simulator knows (it rejects
	// unknown fields at start-up).
	raw := []byte(read(t, root, "deploy", "pixsim", "pix-in-a-box.json"))
	known := read(t, d, "internal", "sim", "scenario.go") + read(t, d, "internal", "sim", "sim.go") + read(t, d, "internal", "dict", "ratelimit.go")
	for _, k := range jsonKeys(t, raw) {
		if !strings.Contains(known, `json:"`+k+`"`) && !strings.Contains(known, `json:"`+k+`,`) {
			t.Errorf("scenario field %q is not a simulator scenario field", k)
		}
	}
}

func TestRiskdContract(t *testing.T) {
	d := need(t, sources(t), "fraud-rules-go")
	contains(t, "riskd routes", read(t, d, "internal", "server", "server.go"),
		`route("POST /v1/evaluate"`, `route("POST /v1/events"`, `"GET /healthz"`, `"GET /metrics"`, "dec.DisallowUnknownFields()")
	contains(t, "riskd flags", read(t, d, "cmd", "riskd", "main.go"), `"ruleset"`, `"budget"`, `"addr"`)
	contains(t, "riskd image", read(t, d, "Dockerfile"), `ENTRYPOINT ["/riskd"`, "EXPOSE 8080")
	svc := compose(t)["riskd"]
	if !svc.has(`command: ["-ruleset", "/etc/riskd/pix-in-a-box.json"`) || !svc.has("./deploy/riskd/pix-in-a-box.json:/etc/riskd/pix-in-a-box.json:ro") {
		t.Error("compose riskd must mount and select deploy/riskd/pix-in-a-box.json")
	}

	// The mounted ruleset compiles in the real engine (riskd refuses to start otherwise).
	ours := []byte(read(t, root, "deploy", "riskd", "pix-in-a-box.json"))
	store, _ := velocity.New(velocity.Config{})
	if _, err := engine.New(store, engine.Options{}).LoadJSON(ours); err != nil {
		t.Fatalf("deploy/riskd/pix-in-a-box.json does not compile: %v", err)
	}
	// Drift from upstream pix-default is reported, not fatal: the PSP owns
	// its ruleset, but a silent divergence should be a decision.
	var a, b struct {
		Rules json.RawMessage `json:"rules"`
	}
	_ = json.Unmarshal(ours, &a)
	_ = json.Unmarshal([]byte(read(t, d, "rulesets", "pix-default.json")), &b)
	var ra, rb any
	_ = json.Unmarshal(a.Rules, &ra)
	_ = json.Unmarshal(b.Rules, &rb)
	ja, _ := json.Marshal(ra)
	jb, _ := json.Marshal(rb)
	if string(ja) != string(jb) {
		t.Logf("NOTE: deploy/riskd/pix-in-a-box.json rules differ from fraud-rules-go rulesets/pix-default.json")
	}
	// Rules the demo and the tests rely on.
	contains(t, "pix-in-a-box ruleset", string(ours), `"ato.new_device.night_drain"`, `"beneficiary.fraud_marks.single"`, `"beneficiary.new_key"`)
}

func TestRelayContract(t *testing.T) {
	d := need(t, sources(t), "webhook-relay-go")
	contains(t, "relayd routes", read(t, d, "api", "api.go"),
		`v1("POST /v1/endpoints"`, `v1("GET /v1/endpoints"`, `v1("POST /v1/messages"`, `v1("GET /v1/messages/{id}/attempts"`,
		`"GET /healthz"`, `"GET /metrics"`, `r.Header.Get("Idempotency-Key")`, `r.Header.Get("traceparent")`, `json:"event_type"`, `json:"payload"`)
	main := read(t, d, "cmd", "relayd", "main.go")
	for k := range compose(t)["relayd"].env {
		if !strings.Contains(main, `"`+k+`"`) {
			t.Errorf("compose relayd env %s is not read by relayd", k)
		}
	}
	contains(t, "relayd image", read(t, d, "Dockerfile"), `VOLUME ["/var/lib/webhook-relay"]`, "EXPOSE 8080")
	contains(t, "Standard Webhooks headers", read(t, d, "signature", "signature.go"), `HeaderID        = "webhook-id"`)
}

func TestPaymentsServiceContract(t *testing.T) {
	d := need(t, sources(t), "payments-service-go")
	server := read(t, d, "internal", "httpapi", "server.go")
	contains(t, "payments routes", server,
		`"POST /v1/charges"`, `"GET /v1/charges/{id}"`, `"POST /v1/webhooks/pix"`, `"GET /readyz"`, `"GET /metrics"`,
		`WebhookSignatureHeader = "X-Webhook-Signature"`, `"sha256=" + hex.EncodeToString`)
	// pixWebhookRequest is decoded strictly: exactly these fields.
	re := regexp.MustCompile(`(?s)type pixWebhookRequest struct \{(.*?)\}`)
	m := re.FindStringSubmatch(server)
	if m == nil {
		t.Fatal("pixWebhookRequest not found")
	}
	tags := regexp.MustCompile(`json:"([a-z_]+)"`).FindAllStringSubmatch(m[1], -1)
	var got []string
	for _, tg := range tags {
		got = append(got, tg[1])
	}
	if strings.Join(got, ",") != "txid,end_to_end_id,amount_cents" {
		t.Errorf("pixWebhookRequest fields %v; the gateway sends txid, end_to_end_id, amount_cents", got)
	}
	contains(t, "payments events", read(t, d, "internal", "domain", "event.go"), `"payments.charge.paid.v1"`, `json:"charge_id"`)
	contains(t, "CloudEvents envelope", read(t, d, "internal", "outbox", "envelope.go"), `json:"id"`, `json:"type"`, `json:"data"`, `json:"subject"`)
	cfg := read(t, d, "internal", "platform", "config", "config.go") + read(t, d, "adapters", "cmd", "server", "main.go")
	env := compose(t)["payments"].env
	for k := range env {
		if !strings.Contains(cfg, `"`+k+`"`) {
			t.Errorf("compose payments env %s is not read by payments-service", k)
		}
	}
	if n := len(env["MERCHANT_NAME"]); n > 25 {
		t.Errorf("MERCHANT_NAME is %d characters; payments-service allows 25", n)
	}
	if n := len(env["MERCHANT_CITY"]); n > 15 {
		t.Errorf("MERCHANT_CITY is %d characters; payments-service allows 15", n)
	}
	contains(t, "payments image", read(t, d, "Dockerfile"), "EXPOSE 8080", "./cmd/server")
}

// TestComposeWiring checks that values which must agree across services do.
func TestComposeWiring(t *testing.T) {
	c := compose(t)
	for _, s := range []string{"postgres", "redpanda", "payments", "pixsim", "riskd", "relayd", "gateway", "notifier", "merchant", "prometheus", "grafana"} {
		if c[s] == nil {
			t.Fatalf("service %s missing", s)
		}
	}
	gw, pay, mer, rel, nt := c["gateway"].env, c["payments"].env, c["merchant"].env, c["relayd"].env, c["notifier"].env

	// Every gateway variable is read by the gateway.
	cfg := read(t, root, "internal", "gateway", "config.go")
	for k := range gw {
		if !strings.Contains(cfg, `"`+k+`"`) {
			t.Errorf("compose gateway env %s is not read by internal/gateway/config.go", k)
		}
	}
	var sc scenario
	if err := json.Unmarshal([]byte(read(t, root, "deploy", "pixsim", "pix-in-a-box.json")), &sc); err != nil {
		t.Fatal(err)
	}
	var psp bool
	for _, a := range sc.Accounts {
		if a.ID != gw["GATEWAY_SPI_ACCOUNT_ID"] {
			continue
		}
		psp = true
		if a.ISPB != gw["GATEWAY_ISPB"] {
			t.Errorf("scenario ISPB %s != GATEWAY_ISPB %s", a.ISPB, gw["GATEWAY_ISPB"])
		}
		if a.Webhook == nil || a.Webhook.URL != "http://gateway:8080/v1/webhooks/spi" || a.Webhook.Secret != gw["PIXSIM_WEBHOOK_SECRET"] {
			t.Errorf("scenario webhook of %s must point at the gateway with PIXSIM_WEBHOOK_SECRET", a.ID)
		}
		if a.BalanceCents <= 0 {
			t.Error("the PSP settlement account needs an opening balance")
		}
	}
	if !psp {
		t.Errorf("scenario has no account %q", gw["GATEWAY_SPI_ACCOUNT_ID"])
	}
	var keyOK bool
	for _, k := range sc.Keys {
		keyOK = keyOK || (k.AccountID == gw["GATEWAY_SPI_ACCOUNT_ID"] && k.Key == gw["MERCHANT_PIX_KEY"])
	}
	if !keyOK {
		t.Errorf("MERCHANT_PIX_KEY %s must be registered to the PSP account in the scenario", gw["MERCHANT_PIX_KEY"])
	}
	eq := func(what, a, b string) {
		if a == "" || a != b {
			t.Errorf("%s: %q != %q", what, a, b)
		}
	}
	eq("merchant Pix key (gateway, payments-service PIX_KEY)", gw["MERCHANT_PIX_KEY"], pay["PIX_KEY"])
	eq("payments webhook secret", gw["PAYMENTS_WEBHOOK_SECRET"], pay["WEBHOOK_SECRET"])
	eq("merchant webhook secret (gateway, merchant)", gw["MERCHANT_WEBHOOK_SECRET"], mer["MERCHANT_WEBHOOK_SECRET"])
	eq("relay token (relayd, gateway)", rel["RELAY_API_TOKEN"], gw["RELAY_API_TOKEN"])
	eq("relay token (relayd, notifier)", rel["RELAY_API_TOKEN"], nt["RELAY_API_TOKEN"])
	eq("events topic (payments, notifier)", pay["EVENT_TOPIC"], nt["NOTIFIER_TOPIC"])
	eq("merchant webhook URL", gw["MERCHANT_WEBHOOK_URL"], "http://merchant:8080/webhooks")
	if _, err := signature.ParseSecret(gw["MERCHANT_WEBHOOK_SECRET"]); err != nil {
		t.Errorf("MERCHANT_WEBHOOK_SECRET: %v", err)
	}
	compText := read(t, root, "docker-compose.yml")
	if !strings.Contains(compText, "subnet: "+rel["RELAY_ALLOW_CIDRS"]) {
		t.Errorf("RELAY_ALLOW_CIDRS %s must be the compose network subnet", rel["RELAY_ALLOW_CIDRS"])
	}
	demo := read(t, root, "scripts", "demo.sh")
	contains(t, "demo.sh defaults", demo, "RELAY_TOKEN:-"+rel["RELAY_API_TOKEN"], "GATEWAY:-http://localhost:8080", "PIXSIM:-http://localhost:8090",
		"MERCHANT:-http://localhost:8070", "RELAY:-http://localhost:8084", "RISKD:-http://localhost:8083")
	for svc, port := range map[string]string{"gateway": "8080:8080", "pixsim": "8090:8080", "merchant": "8070:8080", "relayd": "8084:8080", "riskd": "8083:8080"} {
		if !c[svc].has(`"` + port + `"`) {
			t.Errorf("%s must publish %s (demo.sh default)", svc, port)
		}
	}
	// The notifier reads exactly what compose sets.
	nmain := read(t, root, "services", "notifier", "main.go")
	for k := range nt {
		if !strings.Contains(nmain, `"`+k+`"`) {
			t.Errorf("compose notifier env %s is not read by the notifier", k)
		}
	}
	prom := read(t, root, "deploy", "prometheus", "prometheus.yml")
	for _, target := range []string{"gateway:8080", "riskd:8080", "relayd:8080", "payments:8080", "notifier:8080", "merchant:8080"} {
		contains(t, "prometheus targets", prom, target)
	}
}

// TestDashboardMetricsExist resolves every metric the dashboard queries to
// a name defined in some component's exporter.
func TestDashboardMetricsExist(t *testing.T) {
	src := sources(t)
	defined := read(t, root, "internal", "gateway", "metrics.go") + read(t, root, "internal", "gateway", "service.go") +
		read(t, root, "internal", "platform", "httpx", "client.go") + read(t, root, "internal", "platform", "httpx", "server.go") +
		read(t, root, "internal", "merchant", "merchant.go") + read(t, root, "services", "notifier", "main.go")
	prefixes := map[string]string{"riskd_": "fraud-rules-go", "webhook_": "webhook-relay-go", "outbox_": "payments-service-go"}
	files := map[string][]string{
		"fraud-rules-go":      {"internal/server/server.go"},
		"webhook-relay-go":    {"relay/metrics.go"},
		"payments-service-go": {"internal/platform/metrics/metrics.go"},
	}
	var dash struct {
		Panels []struct {
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal([]byte(read(t, root, "deploy", "grafana", "dashboards", "pix.json")), &dash); err != nil {
		t.Fatal(err)
	}
	nameRe := regexp.MustCompile(`\b([a-z][a-z0-9_]*_(?:total|seconds|depth|requests|pending|events|state|revision|keys|ready))(?:_bucket)?\b`)
	seen := map[string]bool{}
	for _, p := range dash.Panels {
		for _, tg := range p.Targets {
			for _, m := range nameRe.FindAllStringSubmatch(tg.Expr, -1) {
				seen[m[1]] = true
			}
		}
	}
	if len(seen) < 15 {
		t.Fatalf("only %d metric names parsed from the dashboard", len(seen))
	}
	for name := range seen {
		text := defined
		for pfx, repo := range prefixes {
			if strings.HasPrefix(name, pfx) {
				dir, ok := src[repo]
				if !ok {
					if os.Getenv("CONTRACTS_REQUIRED") == "1" {
						t.Fatalf("source of %s not found", repo)
					}
					t.Logf("skipping %s: no %s source", name, repo)
					text = ""
					break
				}
				text = ""
				for _, f := range files[repo] {
					text += read(t, dir, f)
				}
			}
		}
		if text == "" {
			continue
		}
		// Defined either literally or as prefix + suffix (httpx helpers).
		base := name
		for _, pfx := range []string{"gateway_", "merchant_"} {
			if strings.HasPrefix(name, pfx) && (strings.Contains(name, "_http_") || strings.Contains(name, "_downstream_")) {
				base = strings.TrimPrefix(name, strings.TrimSuffix(pfx, "_"))
			}
		}
		if !strings.Contains(text, `"`+name+`"`) && !strings.Contains(text, `"`+base+`"`) {
			t.Errorf("dashboard metric %s is not defined by any exporter", name)
		}
	}
}
