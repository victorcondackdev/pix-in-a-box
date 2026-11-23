module github.com/victorcondackdev/pix-in-a-box/services/notifier

go 1.22

// franz-go is the only dependency, which is why the notifier is its own
// module: the gateway module stays buildable from the component
// repositories alone. go.sum (and the indirect requirements) are produced
// by `go mod tidy` on first build; see the Dockerfile and ci.yml.
require github.com/twmb/franz-go v1.18.0
