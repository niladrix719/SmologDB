"use client";

import { useEffect, useState } from "react";
import type { QueryResult, Row } from "./api/query/route";

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

type Query = { words: string; column: string; range: string; limit: number };

function tokens(s: string): Set<string> {
  return new Set(s.split(/[^\p{L}\p{N}]+/u).filter(Boolean));
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

const INITIAL: Query = { words: "", column: "_msg", range: "15m", limit: 200 };

async function fetchQuery(q: Query): Promise<QueryResult> {
  const res = await fetch("/api/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(q),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error ?? `HTTP ${res.status}`);
  return data;
}

export function Search() {
  const [query, setQuery] = useState<Query>(INITIAL);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [ran, setRan] = useState<Query>(INITIAL);

  function show(q: Query, pending: Promise<QueryResult>) {
    pending
      .then(
        (data) => {
          setResult(data);
          setRan(q);
          setError("");
        },
        (err: Error) => {
          setError(err.message);
          setResult(null);
        },
      )
      .finally(() => setLoading(false));
  }

  function run(q: Query) {
    setLoading(true);
    show(q, fetchQuery(q));
  }

  useEffect(() => {
    show(INITIAL, fetchQuery(INITIAL));
  }, []);

  // Clicking a field searches for its value in that field.
  function filterBy(name: string, value: string) {
    const q = { ...query, column: name, words: value };
    setQuery(q);
    run(q);
  }

  const seenFields = new Set<string>(["_msg"]);
  result?.rows.forEach((r) => r.fields.forEach((f) => seenFields.add(f.Name)));
  const msgMatch = ran.column === "_msg" ? tokens(ran.words) : new Set<string>();

  return (
    <div>

      <form
        className="mb-4 grid gap-2 sm:grid-cols-[1fr_9rem_10rem_6rem_auto]"
        onSubmit={(e) => {
          e.preventDefault();
          run(query);
        }}
      >
        <input
          autoFocus
          value={query.words}
          onChange={(e) => setQuery({ ...query, words: e.target.value })}
          placeholder="Words to find (whole words, case-sensitive), e.g. ERROR payment"
          className="rounded-md border border-zinc-300 bg-white px-3 py-2 font-mono text-sm outline-none focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-900"
        />
        <input
          list="fields"
          value={query.column}
          onChange={(e) => setQuery({ ...query, column: e.target.value })}
          title="Field to search in"
          className="rounded-md border border-zinc-300 bg-white px-3 py-2 font-mono text-sm outline-none focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-900"
        />
        <datalist id="fields">
          {[...seenFields].map((f) => (
            <option key={f} value={f} />
          ))}
        </datalist>
        <select
          value={query.range}
          onChange={(e) => setQuery({ ...query, range: e.target.value })}
          className="rounded-md border border-zinc-300 bg-white px-2 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-900"
        >
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
          className="rounded-md border border-zinc-300 bg-white px-2 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-900"
        >
          {LIMITS.map((n) => (
            <option key={n} value={n}>
              {n} rows
            </option>
          ))}
        </select>
        <button
          type="submit"
          disabled={loading}
          className="rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
        >
          {loading ? "Searching…" : "Search"}
        </button>
      </form>

      {error && (
        <p className="mb-4 rounded-md border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">
          {error}
        </p>
      )}

      {result && (
        <>
          <p className="mb-2 text-xs text-zinc-500">
            {result.matched.toLocaleString()} matched
            {result.matched > result.rows.length && `, showing newest ${result.rows.length}`} · {result.nodesQueried}{" "}
            nodes · {result.tookMs} ms
            {result.nodesFailed > 0 && (
              <span className="ml-2 rounded bg-amber-100 px-1.5 py-0.5 font-medium text-amber-900 dark:bg-amber-900/50 dark:text-amber-200">
                partial: {result.nodesFailed} node{result.nodesFailed > 1 ? "s" : ""} down
              </span>
            )}
          </p>

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
        </>
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
