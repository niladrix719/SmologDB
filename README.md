# SmologDB

A small log database I built to understand how time series databases works on the inside. It stores logs in compressed columns with a bloom filter per block, merges parts in the background like an LSM tree, splits data across storage nodes, and ships with an agent that tails files or accepts JSON over HTTP and keeps a disk queue so nothing is lost while the cluster is down.

It is about 7,600 lines of Go and 5,500 lines of tests. The only dependency is `xxhash`. The code and the binary are called `minilog`, which was the working name.

## Try it

```sh
make demo
```

That builds `minilogd`, starts two storage nodes, an insert/select node and an agent on localhost, writes ten fake log lines a second into a file the agent is tailing, pushes a few JSON lines over HTTP, and then queries everything back:

```
== five ERROR lines from the tailed file
-- 5 rows, nodes queried 2, failed 0
2026-11-15 14:10:58.448  2026-11-15T14:10:58Z ERROR #5 db query slow user=29  [file=.../demo-data/app.log]
2026-11-15 14:10:58.948  2026-11-15T14:10:58Z ERROR #8 user logged in user=28  [file=.../demo-data/app.log]
...
```

It keeps running until you hit Ctrl+C, so you can query it from another terminal in the meantime. Data lives in `./demo-data` and is wiped on the next run.

## How it fits together

```
 app.log ──tail──┐
                 ├──> agent :9429 ──> insert/select :9000 ──┬──> storage :9001
 curl ──HTTP─────┘    (disk queue)    (routes by stream)    └──> storage :9002
                                            ▲
                       query ───────────────┘  (fans out, merges)
```

**Storage node** (`minilog/`). Rows are grouped by stream, sorted by time, and written as immutable *parts*: one file per column, delta-encoded timestamps, and a bloom filter over the tokens in each block. A query checks the block's time range and its bloom filter before it decompresses anything, so a search for a rare word skips most of the data. New rows sit in memory and get written to a part every second, or sooner if 64k rows pile up. Once a partition has eight parts they are merged into one. Data is split into one partition per day, so dropping old data is deleting a directory.

**Insert/select node** (`netinsert/`, `netselect/`). Stateless. Inserts are routed to a storage node by stream ID and batched per node (flushed at 2 MB or every second). Queries go to every storage node in parallel and the sorted results are merged, with `Limit` pushed down so each node only sends what could make the final cut. If a node is down, inserts are rerouted to the others and queries can return a partial result that is marked as partial.

**Agent** (`agent/`). Takes rows from two places: files it tails (`-tail`) and newline-delimited JSON posted to `/insert/jsonline`. It batches them into blocks and writes each block to an on-disk queue per destination before sending, so a dead insert node just means the queue grows. It retries with backoff until the block is accepted, and only gives up on a block the server rejects as malformed. Give it two destinations and every block goes to both. The tailer saves how far it has read in each file, but only after the rows are on disk in the queue, so a crash can cause duplicates but never gaps.

## Running it yourself

```sh
go build -o minilogd ./cmd/minilogd

./minilogd -storageDataPath=./data-1 -httpListenAddr=127.0.0.1:9001
./minilogd -storageDataPath=./data-2 -httpListenAddr=127.0.0.1:9002
./minilogd -storageNode=127.0.0.1:9001,127.0.0.1:9002 -httpListenAddr=127.0.0.1:9000
./minilogd -remoteWrite.addrs=127.0.0.1:9000 -tmpDataPath=./agent-data \
           -httpListenAddr=127.0.0.1:9429 -tail=/tmp/app.log
```

Which role a process plays depends on its flags: `-storageDataPath` makes a storage node, `-storageNode` makes an insert/select node, `-remoteWrite.addrs` makes an agent.

### Sending logs

Tail a file (lines become `_msg`, the file path becomes the stream):

```sh
./scripts/loggen.sh /tmp/app.log      # Ctrl+C to stop
```

Or push JSON:

```sh
curl -X POST --data-binary @app.jsonl \
  'http://127.0.0.1:9429/insert/jsonline?_stream_fields=service,host&_time_field=ts'
```

Nested objects are flattened to dotted names (`{"http":{"status":500}}` becomes `http.status=500`). The time field can be RFC 3339 or a unix timestamp in seconds, milliseconds, microseconds or nanoseconds. If one line in a request is bad, the whole request is rejected with a 400 that names the line, so a retry never stores half a batch twice.

### Querying

```sh
./scripts/query.sh ERROR 20                # rows whose message contains the word ERROR
COL=service ./scripts/query.sh checkout    # match a word in another field
```

Under the hood that is a JSON query sent to the insert/select node:

```sh
curl -X GET -d '{"MinTimestamp":0,"MaxTimestamp":9000000000000000000,
                 "Filters":[{"Column":"_msg","Token":"ERROR"}],"Limit":20}' \
  http://127.0.0.1:9000/
```

Filters match whole words, case-sensitively, and are ANDed together. That is the entire query language.

## Numbers

All of these come from the test suite (`make stageN` prints them), on an M2 Mac mini.

STAGE_NUMBERS_TABLE

| | |
|---|---|
| Agent throughput, 100 rows per block | 5.9k rows/s |
| Agent throughput, 10,000 rows per block | 412k rows/s |
| Rows stored when a node dies after storing but before replying (5,000 sent) | 10,000 |
| Lines re-sent after a tailer restart, with / without a checkpoint | 0 / 150 |
| End-to-end delay from a line in a file to a query result | 2–4 s |

The batching row is the reason the agent buffers at all: writing a block to the queue costs about 17 ms whatever its size, so small blocks waste almost all of it. The duplicates row is the price of at-least-once delivery, and it is the same price VictoriaLogs pays.

## What it does not do

- **No write-ahead log.** Rows in a storage node's memory (up to a second's worth) are lost if the process is killed with `kill -9`. A clean shutdown flushes them. `make crash` kills the process 200 times mid-write and checks that it always comes back with no corruption and nothing missing that was already on disk.
- **At-least-once, not exactly-once.** Retries can store a row twice. There is no dedup.
- **No real query language.** Time range, AND of word filters, limit. No regex, no aggregation, no pipes.
- **No replication inside the cluster, no consensus, no rebalancing.** The node list is a flag. For two copies, point the agent at two clusters. VictoriaLogs works the same way.
- **The merge policy is the simplest one that works.** Eight parts become one, always the whole partition. With a flush every second that means the current day gets rewritten every eight seconds, which is fine for a demo and gets more expensive as the day fills up. A real LSM merges parts of similar size instead.
- **No automatic retention.** `MustDropPartitionsBefore` drops old days, but nothing calls it on a schedule yet, and the `RetentionDays` setting is not wired up.
- **The tailer follows files by path and notices truncation by size.** If a file is rotated and grows past the old offset before the next poll, it will miss that.
- **No auth, TLS, tenants or compression on the wire.**
