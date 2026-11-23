module github.com/victorcondackdev/pix-in-a-box/services/notifier

go 1.26.0

// franz-go is the only dependency, which is why the notifier is its own
// module: the gateway module stays buildable from the component
// repositories alone. go.sum (and the indirect requirements) are produced
// by `go mod tidy` on first build; see the Dockerfile and ci.yml.
require github.com/twmb/franz-go v1.22.3

require (
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
)
