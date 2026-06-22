GOFILES = $(shell find . -name \*.go)

TESTFLAGS_RACE=-race=false
ifdef ENABLE_RACE
	TESTFLAGS_RACE=-race=true
endif

TESTFLAGS_CPU=
ifdef CPU
	TESTFLAGS_CPU=-cpu=$(CPU)
endif
TESTFLAGS = $(TESTFLAGS_RACE) $(TESTFLAGS_CPU) $(EXTRA_TESTFLAGS)

TESTFLAGS_TIMEOUT=30m
ifdef TIMEOUT
	TESTFLAGS_TIMEOUT=$(TIMEOUT)
endif

TESTFLAGS_ENABLE_STRICT_MODE=false
ifdef ENABLE_STRICT_MODE
	TESTFLAGS_ENABLE_STRICT_MODE=$(ENABLE_STRICT_MODE)
endif

.EXPORT_ALL_VARIABLES:
TEST_ENABLE_STRICT_MODE=${TESTFLAGS_ENABLE_STRICT_MODE}

.PHONY: fmt
fmt:
	@echo "Verifying gofmt, failures can be fixed with ./scripts/fix.sh"
	@!(gofmt -l -s -d ${GOFILES} | grep '[a-z]')

	@echo "Verifying goimports, failures can be fixed with ./scripts/fix.sh"
	@!(go run golang.org/x/tools/cmd/goimports@latest -l -d ${GOFILES} | grep '[a-z]')

# golangci-lint is built with an older Go than a newer system Go may provide;
# pin the run to .go-version so the linter can typecheck the standard library
# (a go1.24-built linter cannot parse a //go:build go1.26 stdlib). Prefer a
# PATH-resolved golangci-lint (CI); fall back to ./bin/golangci-lint locally.
GOLANGCI_LINT ?= $(shell command -v golangci-lint 2>/dev/null || echo ./bin/golangci-lint)

.PHONY: lint
lint:
	GOTOOLCHAIN=go$$(cat .go-version) $(GOLANGCI_LINT) run ./...

.PHONY: test
test:
	go test -v ${TESTFLAGS} -timeout ${TESTFLAGS_TIMEOUT} ./...

.PHONY: coverage
coverage:
	go test -v -timeout ${TESTFLAGS_TIMEOUT} ./... \
		-coverprofile cover.out -covermode atomic

.PHONY: test-benchmark-compare
# Runs benchmark tests on the current git ref and the given REF, and compares
# the two.
test-benchmark-compare: install-benchstat
	@git fetch
	./scripts/compare_benchmarks.sh $(REF)

.PHONY: install-benchstat
install-benchstat:
	go install golang.org/x/perf/cmd/benchstat@latest
