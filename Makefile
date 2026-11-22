.PHONY: help build demo ui test unit integration race crash chaos vet todo clean

help:
	@echo "make build        build ./smologdb"
	@echo "make demo         start a local cluster, feed it logs, query them back"
	@echo "make ui           run and search a cluster from the browser, http://127.0.0.1:3000"
	@echo "make unit         package tests (fast)"
	@echo "make integration  end-to-end tests across storage, router and agent"
	@echo "make test         both"
	@echo "make race         everything under the race detector"
	@echo "make crash        kill -9 the storage 200 times and check recovery"
	@echo "make chaos        kill storage nodes during ingest and check what survives"
	@echo "make todo         tests for unfinished work in TODO.md (they fail until it is done)"

build:
	go build -o smologdb ./cmd/smologdb

demo:
	./scripts/demo.sh

ui:
	cd ui && npm install && npm run dev

unit:
	go test $$(go list ./... | grep -v /integration)

integration:
	go test ./integration -timeout 60m

test: unit integration

race:
	go test -race ./... -timeout 90m

crash:
	go run ./cmd/crashtest -iters 200

chaos:
	go run ./cmd/chaostest -iters 200 -nodes 4

vet:
	go vet ./...

# Runs only the tests in files that still start with //go:build todo.
TODO_FILES = $(shell grep -rl --exclude-dir=node_modules --exclude-dir=.git --include='*_test.go' '^//go:build todo' .)
TODO_TESTS = $(shell grep -ho '^func \(Test\|Fuzz\)[A-Za-z0-9_]*' $(TODO_FILES) | sed 's/^func //' | paste -sd'|' -)

todo:
	@if [ -z "$(TODO_FILES)" ]; then echo "nothing left in TODO.md"; else \
		go test -tags todo -count=1 -run '^($(TODO_TESTS))$$' $(sort $(dir $(TODO_FILES))); fi

clean:
	rm -rf smologdb demo-data
	go clean -testcache
