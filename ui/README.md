# SmologDB UI

Runs a local SmologDB cluster and searches it.

```sh
npm install
npm run dev        # http://127.0.0.1:3000
```

- **Cluster**: start and stop storage nodes and the router, add or remove nodes.
- **Agent**: pick log files to collect, see how far each has been read, run a sample log generator.
- **Search**: find rows by words, field and time range.

It runs the `smologdb` binary from the repo root (building it with `go build` if it's missing) and keeps
config and data in `../cluster-data/`. Override with `SMOLOGDB_REPO`, `SMOLOGDB_BIN` or `SMOLOGDB_ROUTER`.

The API starts processes and chooses which files get read, so it answers only to requests from this
machine, and the dev server binds to 127.0.0.1.
