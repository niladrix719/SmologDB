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
export type QueryResult = {
  rows: Row[];
  matched: number;
  nodesQueried: number;
  nodesFailed: number;
  tookMs: number;
};

// Same rule as the storage tokenizer: runs of letters and digits.
function tokenize(s: string): string[] {
  return s.split(/[^\p{L}\p{N}]+/u).filter(Boolean);
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
  const words = typeof input.words === "string" ? input.words : "";
  const column = typeof input.column === "string" && input.column.trim() ? input.column.trim() : "_msg";
  const range = typeof input.range === "string" ? input.range : "15m";
  const limit = Math.min(Math.max(Number(input.limit) || 200, 1), 5000);

  const now = BigInt(Date.now()) * NS_PER_MS;
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
  // ponytail: sorts every match in the range here because the router's Limit keeps the first N
  // by stream, not the newest. Fine for a few hundred thousand rows; a server-side "newest N" is the fix.
  all.sort((a, b) => (BigInt(b.ts) > BigInt(a.ts) ? 1 : BigInt(b.ts) < BigInt(a.ts) ? -1 : 0));

  const result: QueryResult = {
    rows: all.slice(0, limit),
    matched: all.length,
    nodesQueried: raw.stats?.NodesQueried ?? 0,
    nodesFailed: raw.stats?.NodesFailed ?? 0,
    tookMs: Date.now() - started,
  };
  return Response.json(result);
}
