# minilog cluster

Stages 6-10. The same exercise, one level up: take the single-process storage
engine you built and spread it across machines the way VictoriaLogs does.

The README says cluster mode is out of scope. It was, for stages 1-4 -- adding
it earlier would have been a distraction from blocks and blooms. It is in scope
now, and the rule does not change:

> Every stage ships with a measurement that can **fail against an independent
> prediction** -- closed-form theory, or a brute-force scan that shares no code
> with the thing it checks.

The good news: **you already have the oracle.** `bruteForce` in
`harness/helpers_test.go` is a linear scan over `[]Row`. It does not know or
care whether the rows came from one process or eight. Every correctness
measurement in stages 6-10 is that same oracle, unchanged. That is not an
accident -- it falls out of getting the seam right in stage 6, and it is the
reason stage 6 comes first.

---

## The bet VictoriaLogs makes

Three roles, one binary:

- **vlinsert** takes logs, shards them across storage nodes.
- **vlstorage** stores its shard and answers queries about it.
- **vlselect** fans a query out to *all* storage nodes and merges the results.

`app/vlstorage/main.go` decides which one it is by whether `-storageNode` was
passed. If the flag is absent it is a storage node using `localStorage`. If
present it is an insert/select node using `netstorageInsert` / `netstorageSelect`.
Same executable, same call sites, different implementation behind them.

And then the deliberate omissions:

| Not there | Why they get away with it |
|---|---|
| Replication | Sharding only. Back up instead; run two independent clusters for HA. |
| Consensus / Raft / gossip | The node list is a command-line flag. A human edits it. |
| Consistent hashing | Queries hit every node, so routing may change freely. |
| Rebalancing | Same reason. Add a node; old data stays put and is still found. |
| Distributed transactions | A row lands on exactly one node. Nothing spans nodes. |

**Read that third row twice.** Because a query fans out to every node, the
routing function is not load-bearing for correctness -- only for performance.
You can change it between restarts, add nodes, remove nodes, and no row ever
becomes unfindable. Consistent hashing exists to keep keys on the same node
when the node set changes; if you never look up a key by node, you never need
it. This single choice deletes the hardest problem in the design, and it costs
you exactly one thing: a query always pays fan-out to N nodes, even when the
answer lives on one.

That cost is real and you will measure it in stage 8.

---

## Package layout

Mirrors the real repo so stage 5's "go read the original" still works.

```
minilog/logstorage.go   the LogStorage seam         COMPLETE
internalapi/wire.go     codec + protocol version    stage 6
internalapi/server.go   storage-node HTTP handler   stage 6   (= internalinsert + internalselect)
netinsert/router.go     routing policies            stage 7
netinsert/netinsert.go  batching + shipping         stage 7, 9   (= app/vlstorage/netinsert)
netselect/netselect.go  fan-out + merge             stage 8, 9, 10 (= app/vlstorage/netselect)
netstorage/             pairs the two halves        COMPLETE  (= app/vlstorage/main.go)
cmd/minilogd/           the one binary              stage 6
cmd/chaostest/          distributed crashtest       COMPLETE
```

`minilog/` does not import any of the others. The engine must not learn that a
cluster exists -- the only thing added to it is `LogStorage`, four methods wide,
plus two `Query` fields and three `SearchStats` counters the cluster needs.

The harness scaffolding lives in `harness/cluster_test.go`: N storage nodes,
each a real `minilog.Storage` behind a real listener, with two independent
failure injectors -- `kill(i)` stops the transport (no HTTP response ever
arrives) and `fault(i)` returns 500 (the node is up and broken). Keeping those
two distinct is the whole of stage 9.

---

## Stages

| Stage | Files you write | Verified by |
|---|---|---|
| **6** -- the seam & the wire | `internalapi/wire.go`, `internalapi/server.go`, `cmd/minilogd/` | `make stage6` |
| **7** -- insert fan-out & routing | `netinsert/` | `make stage7` |
| **8** -- select fan-out & merge | `netselect/` | `make stage8` |
| **9** -- failure | reroute, partial responses | `make stage9`, `make chaos` |
| **10** -- pushdown | `Query.Limit`, per-node limits | `make stage10` |

---

### Stage 6 -- the seam and the wire

Two pieces of plumbing, no distributed logic yet. Get these right and stages
7-10 are testable against the oracle you already have; get them wrong and
every later stage needs its own bespoke test.

**The interface**, already written for you in `minilog/logstorage.go`:

```go
type LogStorage interface {
	MustAddRows(rows []Row)
	Search(q *Query) ([]Row, *SearchStats)
	MustForceFlush()
	MustClose()
}
```

`*Storage` already satisfies it, and `harness/helpers_test.go`'s `ingest` now
takes the interface, so the same helper drives a local engine and a cluster
with no stage 1-4 call site changing.

Four methods, and the omissions are the interesting part. `MustForceMerge`,
`MustDropPartitionsBefore` and `PartitionDays` are all deliberately outside it:
each one is a *local* storage concept with no cluster-wide meaning, and
inventing one would mean inventing distributed semantics for it. VictoriaLogs
makes the identical cut -- `processForceMerge` in `app/vlstorage/main.go`
returns "unhandled" when `localStorage == nil`, because a select node genuinely
cannot answer it.

**The wire codec.** Do not reach for JSON. You wrote `MarshalVarUint64`,
`MarshalString` and `MarshalTimestamps` in `minilog/encoding.go` and they are
exactly the right tools:

```
row  := varuint(streamID) varuint(zigzag(timestamp)) varuint(len(fields))
        { string(name) string(value) } * len(fields)
batch := varuint(len(rows)) row*
```

Mirror `InsertRow.Marshal` / `UnmarshalInplace` in `lib/logstorage/log_rows.go`.
Note that the real one ships `StreamTagsCanonical` and lets the storage node
derive the id; you ship the `uint64` StreamID directly, because your
`StreamIDForLabels` is already a pure function of the labels and the router
needs the id anyway. Write down why you chose that -- it is a real fork in the
road and the tradeoff (wire bytes vs. trusting the client's hash) is the point.

**The protocol version.** A const in `internalapi`:

```go
const ProtocolVersion = "v1"
```

Sent as a query arg on every internal request; the server rejects a mismatch
with a loud error. This is the entire mechanism that makes rolling upgrades
safe in the real cluster -- a node running old code fails visibly instead of
decoding v2 bytes as v1 garbage. Bump it whenever the row encoding changes.

**The daemon.** `cmd/minilogd`, one binary, two modes:

```sh
./minilogd -storageDataPath=./data-1 -httpListenAddr=:9001   # storage node
./minilogd -storageNode=:9001,:9002,:9003 -httpListenAddr=:9000  # insert+select
```

Storage-node endpoints, matching the real names:

```
POST /internal/insert         body = MarshalRowBatch        -> 204
POST /internal/select/query   body = MarshalQuery           -> MarshalQueryResponse
GET  /internal/force_flush    no body; without it every test sleeps
```

Status codes are load-bearing, because stage 9 has to tell "this node is down"
from "this node is broken" and the response is its only signal. `400` means the
request was wrong (bad version, undecodable body) and must never be retried or
re-routed. `500` means the node broke doing valid work. Neither is
*unavailable* -- unavailability is a transport failure with no HTTP response at
all.

**Measures:**

- **Codec round-trip.** `go test -fuzz` over the row codec: marshal then
  unmarshal must reproduce the row exactly, including empty values and
  non-UTF8 bytes. Independent because the oracle is the input itself.
- **Wire bytes per row vs. JSON.** Same denominator as stage 1's compression
  ratio (`Row.LogicalSizeBytes`). Expect 3-5x. If you are near 1x you are
  probably marshaling field names on every row -- note what a per-batch name
  dictionary would save, and whether it is worth it.
- **The seam is transparent.** `harness/cluster_test.go` defines a `directNode`
  -- a `LogStorage` that speaks `internalapi` to exactly one storage node over
  HTTP, and nothing else. It exists so stage 6 can be measured before netinsert
  or netselect exist. Eight conformance queries covering every skipping
  mechanism you built must give byte-identical results from a local
  `minilog.Storage` and from a `directNode`, and both must equal `bruteForce`.
  If that passes, a disagreement in stage 8 is a fan-out or merge bug rather
  than a codec bug -- which is worth a great deal when you are staring at one.

---

### Stage 7 -- insert fan-out and routing

`netinsert.Storage` implements `MustAddRows` by routing each row to a node,
buffering per node, and shipping batches over HTTP.

**Batching.** One `pendingData` buffer per node. Flush when it exceeds
`MaxInsertBlockSize` (2 MiB, same as the real one) or on a 1-second background
ticker, whichever comes first. Compress the body. Two knobs, opposite
directions: bigger batches amortize HTTP and compress better, smaller batches
make rows visible to queries sooner.

**Routing.** This is the stage. You already wrote the answer one level down --
`Storage.shardForStreamID` in `minilog/storage.go` is `nodeForStreamID` with a
different modulus. Implement three policies and make them switchable:

| Policy | Rule | Balance | Locality |
|---|---|---|---|
| `sticky` | `mix(streamID) % N` | bad under skew | perfect |
| `spray` | random per row | perfect | none |
| `hybrid` | sticky for a stream's first `StickyRowsPerStream` rows, then spray | good | good where it matters |

`hybrid` is what VictoriaLogs does (`streamRowsTracker.getNodeIdx`, threshold
1000). The reasoning is worth stating in your own words before you read theirs:
a stream with 40 rows is a rounding error, so keeping it whole on one node
costs nothing and makes stream-filtered queries cheap. A stream with 40 million
rows is the query you actually care about making fast, and you want all N nodes
grinding on it in parallel. The threshold is where one turns into the other.

**Measures:**

- **Balance vs. balls-in-bins.** With `sticky`, S uniform streams over N nodes
  is exactly balls-in-bins: `max/mean ~= 1 + sqrt(2 ln N / (S/N))`. At S/N = 10
  that predicts about 1.5x; at S/N = 1000, about 1.05x. Balance therefore
  improves as the *square root* of streams-per-node, which is why "we have lots
  of streams, it will even out" is only half true. If your measured ratio is
  *better* than theory, your "random" is not random -- you are probably
  round-robining, which balances beautifully in a synthetic test and falls apart
  the moment the input is batched by stream, which real input always is.
- **The routing tradeoff table.** Sweep policy x stream skew (uniform through
  Zipf s=1.2), report (a) load balance, (b) nodes touched by a single-stream
  query, (c) wall-clock for that query. Three policies x four skews. **This is
  stage 7's equivalent of the stage 3 write-amplification table -- save it.**
  It is the argument for `hybrid` in numbers you measured, and the number that
  will surprise you is how bad `sticky` gets under Zipf.
- **Batch size knee.** Sweep `MaxInsertBlockSize` over 64 KiB .. 8 MiB. Plot
  ingest throughput and time-to-visible. There is a knee. Find it, and work out
  which side of it your 1-second ticker actually puts you on.

---

### Stage 8 -- select fan-out and merge

`netselect.Storage.Search` fans the query to every node concurrently, then
merges.

**The merge is code you already have.** Each node returns rows sorted by
`(StreamID, Timestamp)` -- your `Search` contract already guarantees that. N
sorted streams into one sorted stream is exactly what `minilog/merger.go` does
to parts. Reuse the heap. If it does not fit, the reason it does not fit is
worth understanding before you write a second one.

Stats merge too: `SearchStats.Merge` already exists and is already used for
cross-shard merging in `Storage.Search`. Same call, one level up.

**Measures:**

- **Cluster == single-node == brute force.** 1000 random queries (varying time
  ranges, stream filters, token filters) against the same dataset ingested into
  N = 1, 2, 4, 8 node clusters. Every result must be byte-identical to
  `bruteForce` over the generated rows. The oracle shares no code with the
  network path, which is the entire point. **Vary the routing policy too** -- a
  merge bug that only shows up when a stream is split across nodes is exactly
  the bug `spray` will find and `sticky` will hide.
- **Scaling curve, and where it stops.** Latency vs N for a broad query.
  Speedup will fall short of linear. Before you plot it, predict the serial
  fraction: measure your merge throughput in rows/sec standalone, measure a
  single node's scan rate, and compute the N at which merge time overtakes
  per-node scan time. Then check. Amdahl is the independent prediction here and
  it is falsifiable.
- **The fan-out tax.** A query filtered to *one* stream, under `sticky`
  routing, still opens N nodes. Measure total blocks scanned across the cluster
  vs. the single-node number. That gap is what fan-out-to-all costs you, and it
  is the price of never needing consistent hashing. Decide whether you think it
  was worth it, and write down why.

---

### Stage 9 -- failure

Everything so far assumed every node answers. Now they do not.

**Insert path -- stay up.** A node that fails a request gets
`disabledUntil = now + 10s` and its pending block is re-sent to any other
available node. Rows land somewhere. The invariant:

> Once `MustAddRows` returns, every row is durable on some node, as long as at
> least one node is reachable.

Note what this does *not* promise: it does not promise the row is on the node
the routing function chose. Rerouted rows are on the "wrong" node forever. They
are still findable -- because queries hit every node. The design pays for
itself again here.

**Select path -- fail loud, or fail marked.** Two modes, and the difference
matters more than anything else in this document:

- Default: any node unreachable -> the whole query errors. A complete answer or
  no answer.
- `AllowPartialResponse`: return what the reachable nodes gave you, and **mark
  the response partial**. `SearchStats` grows a `NodesQueried` / `NodesFailed`
  pair, and nothing may report success while `NodesFailed > 0`.

Study `getFirstError` in `app/vlstorage/netselect/netselect.go` before you
write this. It distinguishes "backend is down" (tolerable under partial) from
"backend answered with an error" (never tolerable, because it usually means
misconfiguration, and hiding it makes the cluster undebuggable).

**Measures:**

- **`make chaos`.** The stage 3 crash test, distributed: `cmd/chaostest` kills
  a storage node at 200 random moments during ingest, restarts it, and at the
  end reopens every data directory *directly* -- so the verification path shares
  no code with netselect. The hard invariant it asserts is narrower than
  crashtest's, and the narrowing is the lesson: rows acknowledged by a flush
  during which no node died must survive, but rows that reached a node and were
  still unflushed when it was killed are gone, because the client had already
  let go of them. chaostest prints that loss as a percentage. Distributing the
  store added a second loss window (rows buffered in the client) without
  shrinking the first.
- **Partial responses are never silently partial.** Kill a node mid-query and
  assert both modes: strict returns an error and no rows; partial returns rows
  that are a *strict subset* of the true answer, with `NodesFailed > 0`. Then
  write the test that would catch the failure you actually fear -- a partial
  result reported as complete. That is the most dangerous bug a distributed
  query system can have, because it looks exactly like a correct answer.
- **Reroute balance.** Kill one of four nodes for 30 seconds under steady
  ingest. The remaining three should absorb the load evenly. If one of them
  takes everything, your "pick any available node" is picking the first one.

---

### Stage 10 -- pushdown

Stages 6-9 shipped every matching row to the select node. That does not scale,
and the failure is predictable: a broad query over 8 nodes materializes 8x the
single-node result set in one process, and `Search` returning `[]Row` means it
is all resident at once.

Add the smallest thing that fixes it:

```go
type Query struct {
	...
	Limit int  // 0 means unlimited
}
```

Each storage node applies `Limit` to its own result. The select node merges and
applies `Limit` again. Correct because the merge is order-preserving and each
node's local top-K contains the global top-K's contribution from that node.

This is `splitQueryToRemoteAndLocal` in `lib/logstorage/net_query_runner.go`,
reduced to its smallest honest form. The real thing splits an entire pipe chain
into a remote half and a local half; `Limit` is the one pipe minilog needs to
make the point. Read theirs after you have written yours -- the shape will be
recognizable, and the thing you will notice is that *which* pipes can be pushed
down is not obvious (a `sort` can, a `uniq` cannot, and working out why is the
lesson).

**Measures:**

- **Bytes on the wire, with and without.** Same query, `Limit = 100`, N = 8.
  Without pushdown you ship every match; with it you ship at most 800 rows.
  Report the ratio.
- **Peak RSS vs N.** Without pushdown, memory on the select node grows linearly
  in N. With it, flat. Plot both lines -- the crossing point is the cluster size
  at which the un-pushed version would have taken the process down.
- **Correctness, again.** The same 1000-query oracle run with `Limit` set. A
  limit pushdown that returns the wrong 100 rows is a silent bug of exactly the
  kind stage 9 taught you to fear.

---

### Stage 11 -- the agent

Stage 7 wrote this down and then left it alone:

```
cluster: MustAddRows returns  ->  rows are in a client-side buffer, and
                                  will not survive process death at all
```

That window is why `vlagent` exists. It sits next to the application, not
inside the cluster, and it puts rows on disk before it promises anything:

```
app  ->  agent  ->  storage node(s)
              |
         queue on disk, one per destination
```

Package `agent/`, two files:

- `queue.go` -- a durable FIFO of byte blocks: chunk files with fixed-width
  length-prefixed blocks, a metainfo file for reader/writer offsets, recovery
  on reopen (including a torn trailing write), a disk bound with oldest-drop.
  Peek/ack rather than read, so at-least-once is a contract and not an
  accident. This is `lib/persistentqueue` with the fast path removed.
- `agent.go` -- `Agent.MustAddRows` batches into a block, `MustFlush` appends
  the block to *every* destination's queue, and one sender per destination
  does peek, send, ack -- retrying forever with backoff on no-response and
  5xx, dropping on 4xx. This is `app/vlagent/remotewrite`.

The agent speaks the internal insert protocol from stage 6. Nothing new
crosses the wire; what is new is what happens before it does.

**Decisions the stubs ask you to make and write down:**

- fsync per block, or periodic. Measure both.
- Full queue: drop oldest, or block the producer. The real one drops oldest,
  and the reason is that the newest logs are the ones about the outage.
- A 4xx at the head of the queue: retry it and stall everything behind it, or
  drop it and log why. There is only one answer, but see how the queue changed
  a stage 9 rule from "one failed request" to "the pipeline stops".

**Measures:**

- **Outage becomes latency.** Kill the destination mid-ingest. `MustAddRows`
  keeps returning; revive; every row arrives. Report the accept latency with
  the destination down and the drain time after.
- **Restart becomes nothing.** Ingest with the destination down, close the
  agent, reopen it on the same directory, revive. Every row arrives. Then
  corrupt the tail of a chunk file first and do it again.
- **A lost response becomes a duplicate.** The destination stores the block
  and answers 503. The agent retries. Count the duplicate rows -- that is the
  price of at-least-once in a store with no row identity, and VictoriaLogs
  pays it on purpose.
- **A dead replica costs the live one nothing.** Two destinations, one killed.
  Report how long the live one took to drain.
- **What a block costs.** Same rows at 100, 1k, 10k rows per block. The ratio
  is the fsync, or it is not -- and either way you now know which.

Note what this stage adds to the "out of scope" list below: replication is
still out of scope *for the cluster*. The agent writing the same block to two
clusters is how the real system gets HA without the cluster having to know
the word, and it is fifteen lines once the queue exists.

---

### Stage 12 -- the agent's inputs

Stage 11 built the second half of `vlagent`: a durable queue and a sender per
destination. Nothing could feed it except a Go call. This stage adds the first
half -- the part that makes it an *ingestion* component -- and wires the whole
path together:

```
curl / shipper -> receiver --\
                              +-> agent -> disk queue -> insert node -> storage nodes
log file -> tailer ----------/        (never routes)     (routes)
```

Files, all in `agent/` except the last:

- `receiver.go` -- `POST /insert/jsonline`: newline-delimited JSON in, rows out.
  `ParseJSONLine` is the interesting part. A log line is not a `Row`: it has no
  `StreamID`, no nanosecond timestamp, and its fields are not sorted. The rules
  that bite: a nanosecond timestamp does not survive `float64`; a time field is
  *consumed* and must not stay in the row; fields must come out sorted by name
  because the block writer assumes it; and a bad line must reject the *whole*
  request, or a client's retry stores the good half twice.
- `tailer.go` -- follows files like `tail -F` and checkpoints how far it got.
  It has state that outlives it, so it has the queue's problem again:
  the checkpoint must be right after a crash at any point. The rule is one line --
  **flush the sink, then save the checkpoint** -- and `TestStage12CheckpointNever
  RunsAheadOfFlush` watches for the window from outside. Along the way: a half-written
  line must not be consumed, and a truncated file must restart from zero.
- `cmd/minilogd/main.go` -- `-remoteWrite.addrs`, `-tmpDataPath`, `-tail`: the
  agent mode (`runAgent`), and the insert node learning to serve `InsertPath`
  so that `agent -> insert node` works in the binary and not only in the harness.

**Decisions the stubs ask you to make and write down:**

- Atomic or streaming request handling. The receiver parses everything, then
  hands the sink one batch. The real one streams and cannot. What does each cost?
- Where the stream labels come from. `app`? `host`? A request id? What does each
  choice do to stage 7's routing and to the Zipf problem?
- Poll or notify. The tailer polls; the real one does not. At what scale does
  that stop being a defensible shortcut?

**Measures:**

- **The agent does not route.** One agent address (the insert node), three
  storage nodes: rows must arrive sharded, each exactly once. Pointed at the
  storage nodes directly, the same code replicates every block to all of them.
- **What a checkpoint buys.** Restart a tailer: with its checkpoint, 0
  duplicates; without it, the whole file again. Report both.
- **The ack is not the end of the road.** The agent acks when the *insert node*
  buffers the rows; there is still a `FlushInterval` of memory between there and
  a storage node. Name the window, and say which component owns it.

---

## Deliberately out of scope

Same spirit as the README's list, and for the same reason -- each of these
doubles the work and teaches nothing new about what is already here.

- **Replication inside the cluster.** VictoriaLogs does not do it either. If
  you want the row twice, run two clusters and write to both -- which is
  exactly what the stage 11 agent does with two `Addrs`.
- **Consensus, leader election, gossip, service discovery.** The node list is a
  flag. If you find yourself wanting Raft, re-read why fan-out-to-all made it
  unnecessary.
- **Rebalancing / resharding.** Adding a node changes routing for new rows
  only. Old rows stay put and stay findable.
- **Multi-level clusters, tenants, auth, TLS.** All present in the real repo,
  none of them structural.

---

## Notes to keep

The stubs will ask you to record decisions in place, same as before. The ones
worth writing down while they are fresh:

- Why you ship `StreamID` on the wire instead of the labels.
- Where you put your `StickyRowsPerStream` threshold and what the table said.
- What `spray` routing did to your stream-filtered query latency, in numbers.
- What replication would have changed -- specifically, which of your stage 9
  invariants would need to become quorum reads.
- The moment in stage 8 where the scaling curve flattened, and whether your
  Amdahl prediction called it.
