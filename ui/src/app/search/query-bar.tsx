"use client";

import { useEffect, useRef } from "react";
import { LIMITS, LIVE_EVERY_MS, RANGES, type Query } from "./use-search";

const control =
  "h-8 rounded border border-line bg-canvas text-[13px] text-fg outline-none focus-visible:border-accent focus-visible:ring-1 focus-visible:ring-accent";

function Select({ label, value, onChange, children }: { label: string; value: string | number; onChange: (v: string) => void; children: React.ReactNode }) {
  return (
    <label className="relative order-2 sm:order-none">
      <span className="sr-only">{label}</span>
      <select value={value} onChange={(e) => onChange(e.target.value)} className={`${control} appearance-none pr-7 pl-2.5`}>
        {children}
      </select>
      <svg aria-hidden viewBox="0 0 12 12" className="pointer-events-none absolute top-1/2 right-2 h-3 w-3 -translate-y-1/2 text-muted">
        <path d="M3 4.5 6 7.5 9 4.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </label>
  );
}

type Props = {
  draft: Query;
  onDraft: (q: Query) => void;
  onSearch: (q: Query) => void;
  live: boolean;
  onLive: (on: boolean) => void;
  loading: boolean;
};

export function QueryBar({ draft, onDraft, onSearch, live, onLive, loading }: Props) {
  const input = useRef<HTMLInputElement>(null);

  // "/" jumps to the search box from anywhere on the page, as in most log tools.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const typing = e.target instanceof HTMLElement && e.target.closest("input, textarea, select");
      if (e.key === "/" && !typing && !e.metaKey && !e.ctrlKey && !e.altKey) {
        e.preventDefault();
        input.current?.focus();
        input.current?.select();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  return (
    <header className="sticky top-0 z-20 border-b border-line bg-surface">
      <form
        role="search"
        className="flex flex-wrap items-center gap-2 px-4 py-2"
        onSubmit={(e) => {
          e.preventDefault();
          onSearch(draft);
        }}
      >
        <span className="mr-2 font-mono text-[13px] font-semibold tracking-tight text-fg">smologdb</span>
        <div className="relative order-1 w-full sm:order-none sm:w-auto sm:flex-1">
          <input
            ref={input}
            aria-label="Search logs"
            spellCheck={false}
            autoComplete="off"
            value={draft.q}
            onChange={(e) => onDraft({ ...draft, q: e.target.value })}
            onKeyDown={(e) => e.key === "Escape" && e.currentTarget.blur()}
            placeholder='ERROR -debug service:checkout "disk full" pay*'
            className={`${control} w-full pr-8 pl-2.5 font-mono placeholder:text-muted`}
          />
          <kbd className="pointer-events-none absolute top-1/2 right-2 -translate-y-1/2 rounded border border-line px-1 font-mono text-[11px] leading-4 text-muted">
            /
          </kbd>
        </div>
        <Select label="Time range" value={draft.range} onChange={(range) => onDraft({ ...draft, range })}>
          {RANGES.map(([v, label]) => (
            <option key={v} value={v}>
              {label}
            </option>
          ))}
        </Select>
        <Select label="Rows to show" value={draft.limit} onChange={(v) => onDraft({ ...draft, limit: Number(v) })}>
          {LIMITS.map((n) => (
            <option key={n} value={n}>
              {n} rows
            </option>
          ))}
        </Select>
        <button
          type="button"
          aria-pressed={live}
          onClick={() => onLive(!live)}
          title={`Re-run the search every ${LIVE_EVERY_MS / 1000} seconds`}
          className={`${control} order-2 inline-flex items-center gap-1.5 px-2.5 sm:order-none ${live ? "border-accent text-accent" : "text-muted hover:text-fg"}`}
        >
          <span aria-hidden className={`h-1.5 w-1.5 rounded-full ${live ? "bg-accent motion-safe:animate-pulse" : "bg-faint"}`} />
          Live
        </button>
        <button type="submit" className="order-2 h-8 rounded sm:order-none bg-fg px-3 text-[13px] font-medium text-canvas hover:opacity-90 focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-1 focus-visible:outline-none">
          Search
        </button>
      </form>
      <div aria-hidden className={`absolute inset-x-0 bottom-[-1px] h-0.5 bg-accent transition-opacity ${loading ? "opacity-100 motion-safe:animate-pulse" : "opacity-0"}`} />
    </header>
  );
}
