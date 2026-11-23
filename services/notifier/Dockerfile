# syntax=docker/dockerfile:1.7
#
# The notifier is its own module (franz-go is its only dependency). go.sum
# is produced by `go mod tidy` when it is not committed yet.
FROM golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy \
 && go build -trimpath -ldflags="-s -w" -o /out/notifier .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/notifier /notifier
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 CMD ["/notifier", "-healthcheck"]
ENTRYPOINT ["/notifier"]
