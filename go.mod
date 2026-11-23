module github.com/victorcondackdev/pix-in-a-box

go 1.22

// fraud-rules-go is a test dependency: the gateway's tests run its real
// engine and the pix-default ruleset in-process instead of a fake.
//
// The component repositories are consumed as a Go workspace (see
// scripts/workspace.sh): locally from sibling checkouts, in CI and Docker
// from shallow clones. The versions below are the release tags each
// component is published under; inside the workspace the checked-out
// source is used instead.
require (
	github.com/victorcondackdev/brpay v0.1.0
	github.com/victorcondackdev/fraud-rules-go v0.1.0
	github.com/victorcondackdev/ledger-go v0.1.0
	github.com/victorcondackdev/pix-psp-simulator v0.1.0
	github.com/victorcondackdev/webhook-relay-go v0.1.0
)
