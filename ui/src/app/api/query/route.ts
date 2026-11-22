import { guard, loadConfig } from "@/lib/cluster";

export type Field = { Name: string; Value: string };
export type Row = { ts: string; ms: number; fields: Field[] };
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
  NodesQueried: number;
  NodesFailed: number;
};
// matched, hist, stats and nodesQueried come from the router's /hits. They're null when it
// doesn't answer (it isn't written yet: TODO #14), and the rows are shown without them.
export type QueryResult = {
  rows: Row[];
  matched: number | null;
  hist: Histogram | null;
  stats: Stats | null;
  nodesQueried: number | null;
  nodesFailed: number;
  tookMs: number;
};

// The range picker's values as the router's `since`, a Go duration. Go has no "d", and
// "all" sends no since at all.
const SINCE: Record<string, string> = { "5m": "5m", "15m": "15m", "1h": "1h", "6h": "6h", "24h": "24h", "7d": "168h", all: "" };

const NS_PER_MS = BigInt(1_000_000);

// "2026-11-22T14:10:58.448123456Z" as Unix milliseconds, plus the exact nanoseconds as a string
// (Date keeps only milliseconds).
function parseTime(s: string): [number, string] {
  const m = /^(.*?)(?:\.(\d{1,9}))?Z$/.exec(s);
  const secMs = Date.parse(`${m?.[1] ?? s}Z`);
  if (!Number.isFinite(secMs)) return [0, "0"];
  const frac = (m?.[2] ?? "").padEnd(9, "0");
  return [secMs + Math.floor(Number(frac) / 1e6), (BigInt(secMs) * NS_PER_MS + BigInt(frac)).toString()];
}

// /select answers one JSON object per row: "_time" plus every field.
function parseRows(ndjson: string): Row[] {
  return ndjson
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const obj = JSON.parse(line) as Record<string, string>;
      const [ms, ts] = parseTime(obj._time ?? "");
      const fields = Object.entries(obj)
        .filter(([name]) => name !== "_time")
        .map(([Name, Value]) => ({ Name, Value }));
      return { ts, ms, fields };
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
  const q = typeof input.q === "string" ? input.q : "";
  const since = SINCE[input.range] ?? SINCE["15m"];
  const limit = Math.min(Math.max(Number(input.limit) || 200, 1), 5000);
  // Minutes ahead of UTC (the negative of the browser's getTimezoneOffset), so histogram
  // buckets start on the viewer's local hours.
  const tz = Number(input.tz);
  const offset = Number.isFinite(tz) && Math.abs(tz) <= 14 * 60 ? `${Math.round(tz)}m` : "0m";

  const url = (path: string, extra: Record<string, string>) =>
    `${ROUTER}${path}?${new URLSearchParams({ q, ...(since && { since }), ...extra })}`;
  const started = Date.now();
  const hitsPending = fetch(url("/hits", { offset }), { signal: AbortSignal.timeout(30_000) }).catch(() => null);
  let rowsRes: Response;
  try {
    rowsRes = await fetch(url("/select", { limit: String(limit) }), { signal: AbortSignal.timeout(30_000) });
  } catch (err) {
    return Response.json({ error: `can't reach the router at ${ROUTER}: ${(err as Error).message}` }, { status: 502 });
  }
  if (!rowsRes.ok) {
    const text = (await rowsRes.text()).trim();
    // A 400 is the query's fault (say, an unclosed quote); anything else is the cluster's.
    return rowsRes.status === 400
      ? Response.json({ error: text }, { status: 400 })
      : Response.json({ error: `router answered ${rowsRes.status}: ${text}` }, { status: 502 });
  }
  const rows = parseRows(await rowsRes.text());

  // /hits is optional: without it the rows still show. A router with no /hits route hands the
  // request to its GET / handler, which answers 400 rather than 404, so any failure counts.
  // Expected answer: {"start": unix ms, "step": ms, "counts": [...], "stats": SearchStats}.
  let hits: (Histogram & { stats: Stats | null }) | null = null;
  const hitsRes = await hitsPending;
  if (hitsRes?.ok) {
    const body = await hitsRes.json().catch(() => null);
    if (Array.isArray(body?.counts)) hits = body;
  }

  const result: QueryResult = {
    rows,
    matched: hits ? hits.counts.reduce((sum, n) => sum + n, 0) : null,
    hist: hits && { start: hits.start, step: hits.step, counts: hits.counts },
    stats: hits?.stats ?? null,
    nodesQueried: hits?.stats?.NodesQueried ?? null,
    nodesFailed: hits?.stats?.NodesFailed ?? (Number(rowsRes.headers.get("x-nodes-failed")) || 0),
    tookMs: Date.now() - started,
  };
  return Response.json(result);
}
