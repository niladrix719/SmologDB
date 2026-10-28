.PHONY: help stage1 stage2 stage3 stage4 stage6 stage7 stage8 stage9 stage10 stage11 stage12 test race crash chaos vet clean

help:
	@echo "minilog -- a 1500-line VictoriaLogs, built by you"
	@echo ""
	@echo "  make stage1   encoding + block + part round-trip, compression ratio"
	@echo "  make stage2   bloom filter FP rate vs theory, block skip ratio"
	@echo "  make stage3   merge correctness, LSM write-amplification table"
	@echo "  make stage4   partition pruning, retention, sharding sweep"
	@echo ""
	@echo "  cluster (see CLUSTER.md)"
	@echo "  make stage6   wire codec round-trip, seam transparency"
	@echo "  make stage7   routing balance vs balls-in-bins, tradeoff table"
	@echo "  make stage8   cluster vs brute force, scaling curve"
	@echo "  make stage9   reroute on node loss, partial-response marking"
	@echo "  make stage10  limit pushdown: wire bytes and peak RSS vs N"
	@echo "  make stage11  agent: durable queue, outage, restart, duplicates"
	@echo "  make stage12  agent inputs: JSON receiver, file tailer, end to end"
	@echo ""
	@echo "  make test     all stages"
	@echo "  make race     all stages under the race detector"
	@echo "  make crash    200 kill -9 / recover iterations"
	@echo "  make chaos    200 storage-node kills across a 4-node cluster"
	@echo ""
	@echo "Every stage fails loudly until you implement it. That is the point."

stage1:
	go test ./harness -run '^TestStage1[A-Za-z]' -v

stage2:
	go test ./harness -run TestStage2 -v

stage3:
	go test ./harness -run TestStage3 -v -timeout 30m

stage4:
	go test ./harness -run TestStage4 -v -timeout 30m

stage6:
	go test ./harness -run TestStage6 -v -timeout 30m

stage7:
	go test ./harness -run TestStage7 -v -timeout 30m

stage8:
	go test ./harness -run TestStage8 -v -timeout 30m

stage9:
	go test ./harness -run TestStage9 -v -timeout 30m

stage10:
	go test ./harness -run '^TestStage10' -v -timeout 30m

stage11:
	go test ./harness -run '^TestStage11' -v -timeout 30m

stage12:
	go test ./agent ./harness -run '^TestStage12' -v -timeout 30m

test:
	go test ./harness -v -timeout 60m

race:
	go test ./harness -race -timeout 90m

crash:
	go run ./cmd/crashtest -iters 200

chaos:
	go run ./cmd/chaostest -iters 200 -nodes 4

vet:
	go vet ./...

clean:
	go clean -testcache
