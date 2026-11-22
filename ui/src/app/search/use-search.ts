"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { QueryError, QueryResult } from "../api/query/route";

export const RANGES = [
  ["5m", "Last 5 minutes"],
  ["15m", "Last 15 minutes"],
  ["1h", "Last hour"],
  ["6h", "Last 6 hours"],
  ["24h", "Last 24 hours"],
  ["7d", "Last 7 days"],
  ["all", "All time"],
] as const;
export const LIMITS = [50, 200, 1000, 5000] as const;
export const LIVE_EVERY_MS = 2000;

export type Query = { q: string; range: string; limit: number };
export type Failure = { message: string; kind: NonNullable<QueryError["kind"]> | "network"; router?: string };

const DEFAULT: Query = { q: "", range: "15m", limit: 200 };

// The search lives in the URL (?q=&range=&limit=), so a link reproduces it and Back works.
function fromURL(): Query {
  const p = new URLSearchParams(window.location.search);
  const range = p.get("range");
  const limit = Number(p.get("limit"));
  return {
    q: p.get("q") ?? "",
    range: RANGES.some(([v]) => v === range) ? range! : DEFAULT.range,
    limit: (LIMITS as readonly number[]).includes(limit) ? limit : DEFAULT.limit,
  };
}

function toURL(q: Query): string {
  const p = new URLSearchParams();
  if (q.q) p.set("q", q.q);
  if (q.range !== DEFAULT.range) p.set("range", q.range);
  if (q.limit !== DEFAULT.limit) p.set("limit", String(q.limit));
  const s = p.toString();
  return `${window.location.pathname}${s ? `?${s}` : ""}`;
}

async function fetchQuery(q: Query, signal: AbortSignal): Promise<QueryResult> {
  const res = await fetch("/api/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...q, tz: -new Date().getTimezoneOffset() }),
    signal,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const e = data as QueryError;
    throw Object.assign(new Error(e.error ?? `HTTP ${res.status}`), { kind: e.kind, router: e.router });
  }
  return data;
}

export function useSearch() {
  const [draft, setDraft] = useState<Query>(DEFAULT); // what the controls show
  const [ran, setRan] = useState<Query>(DEFAULT); // what the results belong to
  const [result, setResult] = useState<QueryResult | null>(null);
  const [failure, setFailure] = useState<Failure | null>(null);
  const [loading, setLoading] = useState(true);
  const [live, setLive] = useState(false);
  const inFlight = useRef<AbortController | null>(null);
  const lastRan = useRef<Query>(DEFAULT);

  // Each search aborts the one before it, so an older answer can never land on top of a newer
  // search. quiet is a live-tail refresh: the current results stay on screen, undimmed.
  const run = useCallback((q: Query, opts: { quiet?: boolean; push?: boolean } = {}) => {
    inFlight.current?.abort();
    const ctl = new AbortController();
    inFlight.current = ctl;
    if (opts.push && toURL(q) !== `${window.location.pathname}${window.location.search}`) {
      window.history.pushState(null, "", toURL(q));
    }
    if (!opts.quiet) setLoading(true);
    fetchQuery(q, ctl.signal)
      .then(
        (data) => {
          if (ctl.signal.aborted) return;
          setResult(data);
          setRan(q);
          lastRan.current = q;
          setFailure(null);
        },
        (err: Error & { kind?: Failure["kind"]; router?: string }) => {
          if (ctl.signal.aborted) return;
          const kind = err.kind ?? "network";
          setFailure({ message: err.message, kind, router: err.router });
          // A typo keeps the last good results on screen; a broken router replaces them.
          if (!opts.quiet && kind !== "bad-query") setResult(null);
        },
      )
      .finally(() => {
        if (inFlight.current !== ctl) return;
        inFlight.current = null;
        setLoading(false);
      });
  }, []);

  // Load the search from the URL, and follow Back and Forward.
  useEffect(() => {
    const load = () => {
      const q = fromURL();
      setDraft(q);
      run(q);
    };
    load();
    window.addEventListener("popstate", load);
    return () => window.removeEventListener("popstate", load);
  }, [run]);

  // Live tail: re-run the last search every couple of seconds. Rows come newest first, so new
  // lines appear on top. A tick is skipped while the tab is hidden or a search is still out.
  useEffect(() => {
    if (!live) return;
    const timer = setInterval(() => {
      if (!document.hidden && !inFlight.current) run(lastRan.current, { quiet: true });
    }, LIVE_EVERY_MS);
    return () => clearInterval(timer);
  }, [live, run]);

  const search = useCallback(
    (q: Query) => {
      setDraft(q);
      run(q, { push: true });
    },
    [run],
  );

  return { draft, setDraft, ran, result, failure, loading, live, setLive, search };
}
