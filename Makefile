# Local development. The component repositories are expected as sibling
# checkouts (../ledger-go, ../pix-psp-simulator, ...); `make workspace
# DEPS=.deps-elsewhere` clones whatever is missing.
DEPS ?= ..

.PHONY: workspace test contracts lint up demo down logs

workspace:
	CONTRACTS=1 scripts/workspace.sh $(DEPS)

test:
	go test -race -count=1 ./...

contracts:
	scripts/check-contracts.sh

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

up:
	docker compose up -d --build --wait

demo:
	./scripts/demo.sh

down:
	docker compose down -v

logs:
	docker compose logs -f gateway notifier merchant
