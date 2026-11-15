#!/usr/bin/env bash
# Queries the insert/select node and prints rows as readable lines, newest last.
#
#   ./scripts/query.sh                 # every row
#   ./scripts/query.sh ERROR           # rows whose _msg contains the WORD ERROR
#   ./scripts/query.sh ERROR 20        # ...at most 20 of them
#   ./scripts/query.sh "" 20           # any row, at most 20
#   COL=service ./scripts/query.sh checkout   # match the word in another field
#   ADDR=127.0.0.1:9000 ./scripts/query.sh
addr=${ADDR:-127.0.0.1:9000}
col=${COL:-_msg}
token=${1:-}
limit=${2:-0}
filters='[]'
[ -n "$token" ] && filters="[{\"Column\":\"$col\",\"Token\":\"$token\"}]"
curl -s -m 30 -X GET \
  -d "{\"MinTimestamp\":0,\"MaxTimestamp\":9000000000000000000,\"Filters\":$filters,\"Limit\":$limit}" \
  "http://$addr/" | python3 -c '
import json, sys
from datetime import datetime, timezone
d = json.load(sys.stdin)
rows = sorted(d["rows"] or [], key=lambda r: r["Timestamp"])
for r in rows:
    t = datetime.fromtimestamp(r["Timestamp"] / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    f = {x["Name"]: x["Value"] for x in r["Fields"]}
    msg = f.pop("_msg", "")
    rest = " ".join(f"{k}={v}" for k, v in sorted(f.items()))
    print(f"{t}  {msg}  [{rest}]" if rest else f"{t}  {msg}")
s = d["stats"]
nq, nf = s["NodesQueried"], s["NodesFailed"]
print(f"-- {len(rows)} rows, nodes queried {nq}, failed {nf}", file=sys.stderr)
'
