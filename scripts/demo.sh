#!/usr/bin/env bash
# Starts a small cluster on this machine, feeds it logs two ways, and queries
# them back. Everything stops on Ctrl+C. Data goes to ./demo-data (wiped first).
#
#   storage :9001  storage :9002  <-  insert/select :9000  <-  agent :9429
#                                                                ^      ^
#                                              tails demo-data/app.log  HTTP push
set -euo pipefail
cd "$(dirname "$0")/.."

for port in 9000 9001 9002 9429; do
  if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is already in use -- stop the other minilogd processes first" >&2
    exit 1
  fi
done

go build -o minilogd ./cmd/minilogd
rm -rf demo-data && mkdir -p demo-data
# Stop in reverse start order, so the agent and insert node drain into storage
# nodes that are still up.
pids=()
stop() {
  echo; echo "stopping..."
  for ((i = ${#pids[@]} - 1; i >= 0; i--)); do
    kill "${pids[i]}" 2>/dev/null && wait "${pids[i]}" 2>/dev/null || true
  done
}
trap stop EXIT
trap 'exit 130' INT TERM

./minilogd -storageDataPath=demo-data/s1 -httpListenAddr=127.0.0.1:9001 >demo-data/s1.out 2>&1 & pids+=($!)
./minilogd -storageDataPath=demo-data/s2 -httpListenAddr=127.0.0.1:9002 >demo-data/s2.out 2>&1 & pids+=($!)
sleep 0.5
./minilogd -storageNode=127.0.0.1:9001,127.0.0.1:9002 -httpListenAddr=127.0.0.1:9000 >demo-data/insert.out 2>&1 & pids+=($!)
sleep 0.5
./minilogd -remoteWrite.addrs=127.0.0.1:9000 -tmpDataPath=demo-data/agent \
  -httpListenAddr=127.0.0.1:9429 -tail="$PWD/demo-data/app.log" >demo-data/agent.out 2>&1 & pids+=($!)
sleep 0.5

echo "cluster up. writing to demo-data/app.log (10 lines/s) and pushing JSON over HTTP..."
./scripts/loggen.sh demo-data/app.log 0.1 & pids+=($!)
for i in 1 2 3; do
  curl -s -X POST --data-binary "{\"_msg\":\"order $i placed\",\"service\":\"checkout\",\"level\":\"info\"}" \
    'http://127.0.0.1:9429/insert/jsonline?_stream_fields=service'
done

# A line takes 2-4s to become searchable: tailer poll, agent flush,
# insert-node flush, storage flush. Give it a little more than that.
sleep 5

echo
echo "== the three rows pushed over HTTP"
COL=service ./scripts/query.sh checkout
echo
echo "== five ERROR lines from the tailed file"
./scripts/query.sh ERROR 5
echo
echo "still running. in another terminal try:"
echo "  ./scripts/query.sh ERROR 20"
echo "  ./scripts/query.sh \"\" 50"
echo "Ctrl+C to stop."
wait
