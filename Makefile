.PHONY: help build demo ui test unit integration race crash chaos vet clean

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

clean:
	rm -rf smologdb demo-data
	go clean -testcache
