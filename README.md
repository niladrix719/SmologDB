<img width="2172" height="724" alt="SmologDB Time-Series Log Database" src="https://github.com/user-attachments/assets/331d7fd3-0a6c-41c2-bb07-82c31231fd56" />

A small log database I built to understand how time series databases work on the inside. It stores logs in compressed columns with a bloom filter per block, merges parts as they pile up like an LSM tree, splits data across storage nodes, and ships with an agent that tails files or accepts JSON over HTTP and keeps a disk queue so nothing is lost while the cluster is down.

About 4,700 lines of Go plus 3,200 lines of tests. The only dependency is `xxhash`.

## Try it

```sh
make demo
```

That builds `smologdb`, starts two storage nodes, a router and an agent on localhost, writes ten fake log lines a second into a file the agent is tailing, pushes a few JSON lines over HTTP, and then queries everything back:

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
                 ├──> agent :9429 ──> router :9000 ──┬──> storage :9001
 curl ──HTTP─────┘    (disk queue)    (by stream)    └──> storage :9002
                                         ▲
                    query ───────────────┘  (fans out, merges)
```

**Storage node** (`storage/`). Rows are grouped by stream, sorted by time, and written as immutable *parts*: one file per column, delta-encoded timestamps, and a bloom filter over the tokens in each block. A query checks a block's time range and its bloom filter before it decompresses anything, so a search for a rare word skips most of the data. New rows sit in memory and get written to a part every second, or sooner if 64k rows pile up. Once a partition has eight parts they are merged into one. Data is split into one partition per day, so dropping old data is deleting a directory.

**Router** (`netinsert/`, `netselect/`). Stateless. Inserts go to a storage node picked by stream and are batched per node (sent at 2 MB or every second). Queries go to every storage node in parallel and the sorted results are merged, with the limit pushed down so each node only sends what could make the final cut. If a node is down, inserts are rerouted to the others and queries can return a partial result that is marked as partial.

**Agent** (`agent/`). Takes rows from two places: files it tails and newline-delimited JSON posted to `/ingest`. It batches them into blocks and writes each block to an on-disk queue per destination before sending, so a dead router just means the queue grows. It retries with backoff until the block is accepted, and only gives up on a block the server rejects as malformed. Give it two destinations and every block goes to both. The tailer saves how far it has read in each file, but only after the rows are on disk in the queue, so a crash can cause duplicates but never gaps.

## Running it yourself

```sh
make build

./smologdb storage -data ./data-1 -listen 127.0.0.1:9001
./smologdb storage -data ./data-2 -listen 127.0.0.1:9002
./smologdb router  -nodes 127.0.0.1:9001,127.0.0.1:9002 -listen 127.0.0.1:9000
./smologdb agent   -send-to 127.0.0.1:9000 -dir ./agent-data -listen 127.0.0.1:9429 -tail /tmp/app.log
```

A storage node keeps everything unless you pass `-retention-days N`. It then keeps today and the N-1 days before it: older days are deleted when the node starts and then every hour, and rows older than that are not stored.

### Sending logs

Tail a file. Each line becomes `_msg`, and the file path becomes the stream:

```sh
./scripts/loggen.sh /tmp/app.log      # Ctrl+C to stop
```

Or push JSON:

```sh
curl -X POST --data-binary @app.jsonl \
  'http://127.0.0.1:9429/ingest?stream=service,host&time_field=ts'
```

Nested objects are flattened to dotted names (`{"http":{"status":500}}` becomes `http.status=500`). The time field can be RFC 3339 or a unix timestamp in seconds, milliseconds, microseconds or nanoseconds; without one, the row gets the time it arrived. `msg_field` picks which field becomes the message. If one line in a request is bad, the whole request is rejected with a 400 that names the line, so a retry never stores half a batch twice.

### Querying

```sh
./scripts/query.sh ERROR 20                # rows whose message contains the word ERROR
COL=service ./scripts/query.sh checkout    # match a word in another field
```

Or ask the router directly. `/select` returns the newest matching rows, one JSON object per line:

```sh
curl 'http://127.0.0.1:9000/select?q=ERROR%20-debug&since=15m&limit=20'
```

`q` is a list of terms, and a row must match all of them:

| term | matches |
|---|---|
| `ERROR` | the word `ERROR` in the message (`_msg`) |
| `level:error` | the word `error` in the `level` field |
| `level:=error` | `level` is exactly `error` |
| `"disk full"` | those words next to each other, in order |
| `pay*` | a word that starts with `pay` |
| `-debug` | rows without the word `debug`; `-` works in front of any term |

Words are case-sensitive and punctuation is ignored, so `hello!` searches for the word `hello`. `since` is a Go duration (`15m`, `24h`); leave it out to search all time. `scripts/query.sh` uses the older JSON query on `GET /`, which still works.

### The UI

```sh
make ui        # then open http://127.0.0.1:3000
```

One page with three tabs. **Cluster** starts and stops storage nodes and the router, adds or removes nodes, and shows each process's output. **Agent** sets which log files to collect and shows how far into each one it has read, and can run a fake log generator. **Search** takes the query syntax above and has a **Live** toggle that refreshes every 2 seconds. The histogram of matches over time, the total and the query stats need the router's `/hits` endpoint, which isn't written yet (item 14 in `TODO.md`); until then the page shows the rows without them. Processes started from the UI write their output to `cluster-data/logs/` and are listed in `cluster-data/procs.json`, so if the UI crashes they keep running and it re-attaches to them on its next start. Configuration is saved to `cluster-data/cluster.json`, and every node's data lives under `cluster-data/` too. The UI only accepts requests from this machine, since it can start processes and read files.

## Numbers

From the test suite on an M2 Mac mini (`go test -v` prints them).

| | |
|---|---|
| 200k synthetic log rows on disk vs raw | 8.7 MiB vs 64 MiB (7.4x) |
| Timestamps after delta + varint encoding | 3.6 bytes per row, down from 8 |
| Bloom filter false positives, 16 bits per token | 0.094% measured, 0.094% predicted |
| Blocks skipped when searching for a rare word | 375 of 400 (94%) |
| Partitions opened by a 1-hour query over 90 days of data | 1 of 91 |
| Lines re-sent after a tailer restart, with / without a checkpoint | 0 / 150 |
| End-to-end delay from a line in a file to a query result | 2–4 s |

## What it does not do

- **No write-ahead log.** Rows in a storage node's memory (up to a second's worth) are lost if the process is killed with `kill -9`. A clean shutdown flushes them. `make crash` kills the process 200 times mid-write and checks that it always comes back with no corruption and nothing missing that was already on disk.
- **At-least-once, not exactly-once.** Retries can store a row twice. There is no dedup.
- **A small query language.** Words, phrases, prefixes, exact values, fields and negation, all ANDed. No OR, no regex, no aggregation.
- **No replication inside the cluster, no consensus, no rebalancing.** The node list is a flag. For two copies, point the agent at two clusters.
- **The merge policy is the simplest one that works.** Eight parts become one, always the whole partition. With a flush every second the current day gets rewritten every eight seconds, which is fine for a demo and gets more expensive as the day fills up. Merging parts of similar size would fix it.
- **The tailer follows files by path and notices truncation by size.** If a file is rotated and grows past the old offset before the next poll, it misses that.
- **No auth, TLS or compression on the wire.**

## Layout

```
storage/       storage engine: encoding, blocks, bloom filters, parts, merges, partitions
internalapi/   wire format and the storage node's HTTP API
netinsert/     routing and batching for inserts
netselect/     fan-out, merge and limit pushdown for queries
netstorage/    pairs the two so a cluster looks like a single storage
agent/         disk queue, sender, JSON receiver, file tailer
cmd/smologdb/  the server binary (storage, router and agent)
cmd/crashtest/ kill -9 / recover torture test
cmd/chaostest/ kills storage nodes during ingest
integration/   end-to-end tests over real HTTP
scripts/       demo, log generator, query helper
ui/            Next.js search page
```

## Tests

```sh
make unit          # package tests
make integration   # storage + router + agent together
make race          # everything under the race detector
make crash         # 200 kill -9 / recover cycles
make chaos         # node kills across a 4-node cluster
```
