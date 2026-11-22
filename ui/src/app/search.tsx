"use client";

import { useEffect, useRef, useState } from "react";
import type { QueryResult, Row } from "./api/query/route";
import { Histogram, QueryPlan } from "./charts";

const RANGES = [
  ["5m", "Last 5 minutes"],
  ["15m", "Last 15 minutes"],
  ["1h", "Last hour"],
  ["6h", "Last 6 hours"],
  ["24h", "Last 24 hours"],
  ["7d", "Last 7 days"],
  ["all", "All time"],
];
const LIMITS = [50, 200, 1000, 5000];
const LIVE_EVERY_MS = 2000;

type Query = { q: string; range: string; limit: number };

function tokens(s: string): Set<string> {
  return new Set(s.split(/[^\p{L}\p{N}]+/u).filter(Boolean));
}

// Words to highlight in each message: the search's plain terms on the default column. A term
// with "-" or "field:" in front says nothing about which words the message contains.
function highlightWords(q: string): Set<string> {
  const words = new Set<string>();
  for (const term of q.match(/-?(?:[^\s":]+:=?)?(?:"[^"]*"|\S+)/g) ?? []) {
    if (term.startsWith("-") || /^[^\s":]+:/.test(term)) continue;
    tokens(term.replace(/\*$/, "")).forEach((w) => words.add(w));
  }
  return words;
}

// The search term for "this field has exactly this value". The syntax has no way to escape a
// quote, so a value containing one is searched as a phrase of its words.
function fieldTerm(name: string, value: string) {
  return value.includes('"') ? `${name}:"${[...tokens(value)].join(" ")}"` : `${name}:="${value}"`;
}

function Highlighted({ text, match }: { text: string; match: Set<string> }) {
  if (match.size === 0) return <>{text}</>;
  return (
    <>
      {text.split(/([\p{L}\p{N}]+)/u).map((part, i) =>
        match.has(part) ? (
          <mark key={i} className="rounded-sm bg-amber-200 px-0.5 text-inherit dark:bg-amber-500/40">
            {part}
          </mark>
        ) : (
          part
        ),
      )}
    </>
  );
}

function formatTime(ms: number) {
  const d = new Date(ms);
  const pad = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

const INITIAL: Query = { q: "", range: "15m", limit: 200 };

async function fetchQuery(q: Query): Promise<QueryResult> {
  const res = await fetch("/api/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...q, tz: -new Date().getTimezoneOffset() }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error ?? `HTTP ${res.status}`);
  return data;
}

const control = "rounded-md border border-zinc-300 bg-white py-2 text-sm dark:border-zinc-700 dark:bg-zinc-900";

export function Search() {
  const [query, setQuery] = useState<Query>(INITIAL);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [ran, setRan] = useState<Query>(INITIAL);
  const [live, setLive] = useState(false);
  // Read by the live-tail timer: the last search that ran, and whether a request is still out.
  const lastRan = useRef<Query>(INITIAL);
  const inFlight = useRef(false);

  // quiet is a live-tail refresh: the current results stay on screen, undimmed, until it lands.
  function show(q: Query, pending: Promise<QueryResult>, quiet = false) {
    inFlight.current = true;
    pending
      .then(
        (data) => {
          setResult(data);
          setRan(q);
          lastRan.current = q;
          setError("");
        },
        (err: Error) => {
          setError(err.message);
          if (!quiet) setResult(null);
        },
      )
      .finally(() => {
        inFlight.current = false;
        if (!quiet) setLoading(false);
      });
  }

  function run(q: Query) {
    setLoading(true);
    show(q, fetchQuery(q));
  }

  useEffect(() => {
    show(INITIAL, fetchQuery(INITIAL));
  }, []);

  // Live tail: re-run the last search every couple of seconds. Rows come newest first, so new
  // lines appear on top. A tick is skipped while the tab is hidden or a request is still out.
  useEffect(() => {
    if (!live) return;
    const timer = setInterval(() => {
      if (document.hidden || inFlight.current) return;
      show(lastRan.current, fetchQuery(lastRan.current), true);
    }, LIVE_EVERY_MS);
    return () => clearInterval(timer);
  }, [live]);

  // Clicking a field adds "this field is exactly this value" to the search.
  function filterBy(name: string, value: string) {
    const term = fieldTerm(name, value);
    const q = { ...query, q: query.q.includes(term) ? query.q : `${query.q} ${term}`.trim() };
    setQuery(q);
    run(q);
  }

  const msgMatch = highlightWords(ran.q);

  return (
    <div>
      <form
        className="mb-1.5 grid gap-2 sm:grid-cols-[1fr_10rem_6rem_auto_auto]"
        onSubmit={(e) => {
          e.preventDefault();
          run(query);
        }}
      >
        <input
          autoFocus
          aria-label="Search"
          spellCheck={false}
          value={query.q}
          onChange={(e) => setQuery({ ...query, q: e.target.value })}
          placeholder='ERROR -debug service:checkout "disk full" pay*'
          className={`${control} px-3 font-mono outline-none focus:border-zinc-500`}
        />
        <select value={query.range} onChange={(e) => setQuery({ ...query, range: e.target.value })} className={`${control} px-2`}>
          {RANGES.map(([v, label]) => (
            <option key={v} value={v}>
              {label}
            </option>
          ))}
        </select>
        <select
          value={query.limit}
          onChange={(e) => setQuery({ ...query, limit: Number(e.target.value) })}
          title="Show at most"
          className={`${control} px-2`}
        >
          {LIMITS.map((n) => (
            <option key={n} value={n}>
              {n} rows
            </option>
          ))}
        </select>
        <button
          type="button"
          aria-pressed={live}
          onClick={() => setLive(!live)}
          title={`Re-run the search every ${LIVE_EVERY_MS / 1000} seconds`}
          className={`${control} inline-flex items-center justify-center gap-1.5 px-3 ${live ? "border-emerald-500 text-emerald-700 dark:border-emerald-600 dark:text-emerald-400" : "text-zinc-600 dark:text-zinc-400"}`}
        >
          <span className={`h-2 w-2 rounded-full ${live ? "animate-pulse bg-emerald-500" : "bg-zinc-300 dark:bg-zinc-600"}`} />
          Live
        </button>
        <button
          type="submit"
          disabled={loading}
          className="rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
        >
          {loading ? "Searching…" : "Search"}
        </button>
      </form>
      <p className="mb-4 font-mono text-[11px] text-zinc-500">
        word · field:word · field:=exact · &quot;a phrase&quot; · prefix* · -anything to exclude · all terms must match
      </p>

      {error && (
        <p className="mb-4 rounded-md border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">
          {error}
        </p>
      )}

      {result && (
        <div className={loading ? "opacity-50 transition-opacity" : undefined}>
          <p className="mb-2 text-xs text-zinc-500">
            {result.matched === null
              ? `newest ${result.rows.length} rows`
              : `${result.matched.toLocaleString()} matched${result.matched > result.rows.length ? `, showing newest ${result.rows.length}` : ""}`}
            {result.nodesQueried !== null && ` · ${result.nodesQueried} nodes`} · {result.tookMs} ms
            {result.nodesFailed > 0 && (
              <span className="ml-2 rounded bg-amber-100 px-1.5 py-0.5 font-medium text-amber-900 dark:bg-amber-900/50 dark:text-amber-200">
                partial: {result.nodesFailed} node{result.nodesFailed > 1 ? "s" : ""} down
              </span>
            )}
          </p>

          {result.hist && result.matched ? <Histogram hist={result.hist} /> : null}
          {!result.hist && (
            <p className="mb-3 text-xs text-zinc-500">
              No histogram or total: the router doesn&apos;t answer <code>/hits</code> yet (TODO #14).
            </p>
          )}
          {result.stats && <QueryPlan stats={result.stats} />}

          {result.rows.length === 0 ? (
            <p className="rounded-md border border-dashed border-zinc-300 px-3 py-10 text-center text-sm text-zinc-500 dark:border-zinc-700">
              No rows. Try a wider time range, or check the words match exactly.
            </p>
          ) : (
            <ul className="divide-y divide-zinc-200 rounded-md border border-zinc-200 font-mono text-[13px] dark:divide-zinc-800 dark:border-zinc-800">
              {result.rows.map((row, i) => (
                <LogLine key={i} row={row} msgMatch={msgMatch} onField={filterBy} />
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}

function LogLine({ row, msgMatch, onField }: { row: Row; msgMatch: Set<string>; onField: (n: string, v: string) => void }) {
  const msg = row.fields.find((f) => f.Name === "_msg")?.Value ?? "";
  const rest = row.fields.filter((f) => f.Name !== "_msg");
  return (
    <li className="flex flex-col gap-1 px-3 py-1.5 hover:bg-zinc-50 sm:flex-row sm:gap-4 dark:hover:bg-zinc-900">
      <time className="shrink-0 text-zinc-500" title={`${row.ts} ns`}>
        {formatTime(row.ms)}
      </time>
      <div className="min-w-0 flex-1">
        <span className="break-words whitespace-pre-wrap">
          <Highlighted text={msg} match={msgMatch} />
        </span>
        {rest.length > 0 && (
          <span className="ml-2 inline-flex flex-wrap gap-1 align-middle">
            {rest.map((f) => (
              <button
                key={f.Name}
                onClick={() => onField(f.Name, f.Value)}
                title={`Search ${f.Name} for ${f.Value}`}
                className="rounded bg-zinc-100 px-1.5 text-xs text-zinc-600 hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-700"
              >
                {f.Name}={f.Value}
              </button>
            ))}
          </span>
        )}
      </div>
    </li>
  );
}
