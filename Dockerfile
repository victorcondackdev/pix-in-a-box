# syntax=docker/dockerfile:1.7
#
# Builds the gateway and the example merchant from this repository plus the
# component repositories, assembled as a Go workspace (scripts/workspace.sh).
#
#   docker build --target gateway  -t pix-in-a-box/gateway .
#   docker build --target merchant -t pix-in-a-box/merchant .
#
# Component refs are build args, so a release can pin every component:
#   docker build --build-arg LEDGER_GO_REF=v0.1.0 ...

FROM golang:1.27-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
ARG BRPAY_REF=v0.1.0
ARG LEDGER_GO_REF=main
ARG PIXSIM_REF=main
ARG RELAY_REF=main
ARG FRAUD_RULES_REF=main
ENV CGO_ENABLED=0
COPY go.mod ./
COPY scripts/workspace.sh scripts/workspace.sh
RUN BRPAY_REF=$BRPAY_REF LEDGER_GO_REF=$LEDGER_GO_REF PIXSIM_REF=$PIXSIM_REF RELAY_REF=$RELAY_REF \
    FRAUD_RULES_REF=$FRAUD_RULES_REF sh scripts/workspace.sh /deps
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/gateway ./services/gateway \
 && go build -trimpath -ldflags="-s -w" -o /out/merchant ./services/merchant

FROM gcr.io/distroless/static-debian12:nonroot AS merchant
COPY --from=build /out/merchant /merchant
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 CMD ["/merchant", "-healthcheck"]
ENTRYPOINT ["/merchant"]

FROM gcr.io/distroless/static-debian12:nonroot AS gateway
COPY --from=build /out/gateway /gateway
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=24 CMD ["/gateway", "-healthcheck"]
ENTRYPOINT ["/gateway"]
