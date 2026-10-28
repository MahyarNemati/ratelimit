GO ?= go

.PHONY: all test race bench lint fmt vet cover tidy demo clean

all: lint test

## test: unit + conformance suites with the race detector
test:
	$(GO) test -race -count=1 ./...

## bench: benchmarks for both backends
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./memory/... ./redislimit/...

## cover: coverage report
cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

## lint: formatting and vet
lint: fmt vet

fmt:
	@unformatted=$$(gofmt -l . | grep -v '^$$' || true); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed:"; echo "$$unformatted"; exit 1; \
	fi

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy
	@git diff --exit-code go.mod go.sum

## demo: run the example server
demo:
	$(GO) run ./examples/httpserver -rate 3 -period 10s

clean:
	rm -f coverage.out
