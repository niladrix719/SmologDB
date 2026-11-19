import http from "node:http";
import { guard, loadConfig } from "@/lib/cluster";

const NS_PER_MS = BigInt(1_000_000);
const NS_PER_S = BigInt(1_000_000_000);
const FAR_FUTURE = BigInt("9000000000000000000");
const ZERO = BigInt(0);

const RANGES: Record<string, number> = {
  "5m": 5 * 60,
  "15m": 15 * 60,
  "1h": 60 * 60,
  "6h": 6 * 60 * 60,
  "24h": 24 * 60 * 60,
  "7d": 7 * 24 * 60 * 60,
};

export type Field = { Name: string; Value: string };
export type Row = { ts: string; ms: number; stream: string; fields: Field[] };
export type Histogram = { start: number; step: number; counts: number[] };
// The router's SearchStats, summed over every storage node. Rows held in memory aren't counted.
export type Stats = {
  PartitionsTotal: number;
  PartitionsOpened: number;
  PartsTotal: number;
  PartsOpened: number;
  BlocksTotal: number;
  BlocksSkippedByTime: number;
  BlocksSkippedByBloom: number;
  BlocksScanned: number;
  RowsScanned: number;
  RowsMatched: number;
  BytesReadFromDisk: number;
};
export type QueryResult = {
  rows: Row[];
  matched: number;
  hist: Histogram;
  stats: Stats | null;
  nodesQueried: number;
  nodesFailed: number;
  tookMs: number;
};

// Same rule as the storage tokenizer: runs of letters and digits.
function tokenize(s: string): string[] {
  return s.split(/[^\p{L}\p{N}]+/u).filter(Boolean);
}

const STEPS_S = [1, 5, 10, 30, 60, 300, 600, 1800, 3600, 3 * 3600, 6 * 3600, 12 * 3600, 86400];

// Counts rows in at most ~60 buckets over [fromMs, toMs]. Buckets line up with the viewer's
// clock (shiftMs = local time minus UTC), so a 3-hour bucket starts at 03:00 local, not UTC.
function histogram(rows: Row[], fromMs: number, toMs: number, shiftMs: number): Histogram {
  const span = Math.max(toMs - fromMs, 1000);
  const step = (STEPS_S.find((s) => span / (s * 1000) <= 60) ?? Math.ceil(span / 60_000)) * 1000;
  const start = Math.floor((fromMs + shiftMs) / step) * step - shiftMs;
  const counts = new Array<number>(Math.floor((toMs - start) / step) + 1).fill(0);
  for (const r of rows) {
    const i = Math.floor((r.ms - start) / step);
    if (i >= 0 && i < counts.length) counts[i]++;
  }
  return { start, step, counts };
}

// The router takes its query as a JSON body on GET, which fetch refuses to send.
function getWithBody(url: string, body: string): Promise<{ status: number; text: string }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      url,
      {
        method: "GET",
        timeout: 30_000,
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
      },
      (res) => {
        let text = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => (text += chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, text }));
      },
    );
    req.on("timeout", () => req.destroy(new Error("router timed out")));
    req.on("error", reject);
    req.end(body);
  });
}

export async function POST(request: Request) {
  const denied = guard(request, true);
  if (denied) return denied;
  const ROUTER = process.env.SMOLOGDB_ROUTER ?? `http://${loadConfig().router.listen}`;
  const input = await request.json().catch(() => null);
  if (!input || typeof input !== "object") {
    return Response.json({ error: "bad request body" }, { status: 400 });
  }
  // TODO(#10): once the router has GET /select, send it the query string and drop the
  // GET-with-body request and the sort below; this whole handler becomes a plain fetch.
  const words = typeof input.words === "string" ? input.words : "";
  const column = typeof input.column === "string" && input.column.trim() ? input.column.trim() : "_msg";
  const range = typeof input.range === "string" ? input.range : "15m";
  const limit = Math.min(Math.max(Number(input.limit) || 200, 1), 5000);
  // Minutes ahead of UTC, the negative of the browser's getTimezoneOffset().
  const tz = Number(input.tz);
  const shiftMs = Number.isFinite(tz) && Math.abs(tz) <= 14 * 60 ? tz * 60_000 : 0;

  const nowMs = Date.now();
  const now = BigInt(nowMs) * NS_PER_MS;
  const secs = RANGES[range];
  const min = secs ? now - BigInt(secs) * NS_PER_S : ZERO;
  const max = secs ? now : FAR_FUTURE;
  const filters = tokenize(words).map((Token) => ({ Column: column, Token }));
  // BigInts don't survive JSON.stringify, so the timestamps are written in by hand.
  const body = `{"MinTimestamp":${min},"MaxTimestamp":${max},"Filters":${JSON.stringify(filters)},"Limit":0}`;

  const started = Date.now();
  let res;
  try {
    res = await getWithBody(`${ROUTER}/`, body);
  } catch (err) {
    return Response.json({ error: `can't reach the router at ${ROUTER}: ${(err as Error).message}` }, { status: 502 });
  }
  if (res.status !== 200) {
    return Response.json({ error: `router answered ${res.status}: ${res.text.trim()}` }, { status: 502 });
  }

  // Timestamps and stream IDs are 64-bit; quote them before JSON.parse rounds them.
  const raw = JSON.parse(res.text.replace(/"(Timestamp|StreamID)":(\d+)/g, '"$1":"$2"'));
  const all: Row[] = (raw.rows ?? []).map((r: { Timestamp: string; StreamID: string; Fields: Field[] }) => ({
    ts: r.Timestamp,
    ms: Number(BigInt(r.Timestamp) / NS_PER_MS),
    stream: r.StreamID,
    fields: r.Fields ?? [],
  }));
  // TODO(#14): the router can return just the newest N now (Query.Newest), but the "matched"
  // count and the histogram below need every match. Once a count-only /hits endpoint exists,
  // send Newest and Limit and drop this sort. Fine until a range holds a few hundred thousand rows.
  all.sort((a, b) => (BigInt(b.ts) > BigInt(a.ts) ? 1 : BigInt(b.ts) < BigInt(a.ts) ? -1 : 0));

  // Buckets every match, which this handler only has because it fetches them all (see above).
  const hist = secs
    ? histogram(all, nowMs - secs * 1000, nowMs, shiftMs)
    : histogram(all, all.at(-1)?.ms ?? nowMs, all[0]?.ms ?? nowMs, shiftMs);

  const result: QueryResult = {
    rows: all.slice(0, limit),
    matched: all.length,
    hist,
    stats: raw.stats ?? null,
    nodesQueried: raw.stats?.NodesQueried ?? 0,
    nodesFailed: raw.stats?.NodesFailed ?? 0,
    tookMs: Date.now() - started,
  };
  return Response.json(result);
}
