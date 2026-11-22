"use client";

import type { Row } from "../api/query/route";

// More distinct values than this (ids, trace ids) and a top-5 list says nothing useful.
const MAX_LISTED = 20;
const TOP = 5;

type FieldStat = { name: string; seen: number; distinct: number; top: [string, number][] };

// Counts are over the rows on screen, not every match: the page says so.
function summarize(rows: Row[]): FieldStat[] {
  const byField = new Map<string, Map<string, number>>();
  for (const row of rows) {
    for (const f of row.fields) {
      if (f.Name === "_msg") continue;
      let values = byField.get(f.Name);
      if (!values) byField.set(f.Name, (values = new Map()));
      values.set(f.Value, (values.get(f.Value) ?? 0) + 1);
    }
  }
  return [...byField]
    .map(([name, values]) => ({
      name,
      seen: [...values.values()].reduce((a, b) => a + b, 0),
      distinct: values.size,
      top: values.size > MAX_LISTED ? [] : [...values].sort((a, b) => b[1] - a[1]).slice(0, TOP),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

type Props = { rows: Row[]; onRefine: (name: string, value: string, exclude: boolean) => void; className?: string };

export function FieldsSidebar({ rows, onRefine, className = "" }: Props) {
  const stats = summarize(rows);
  return (
    <aside aria-label="Fields" className={className}>
      <h2 className="px-4 pt-3 pb-2 text-[11px] text-muted">Fields in these {rows.length.toLocaleString()} rows</h2>
      {stats.length === 0 && <p className="px-4 pb-3 text-[12px] text-muted">These rows have only a message.</p>}
      {stats.map((s) => (
        <section key={s.name} className="border-t border-line px-4 py-2.5">
          <h3 className="flex items-baseline justify-between gap-2 font-mono text-[12px] font-semibold text-fg">
            <span className="truncate">{s.name}</span>
            <span className="shrink-0 font-sans text-[11px] font-normal text-muted">
              {s.distinct} {s.distinct === 1 ? "value" : "values"}
            </span>
          </h3>
          {s.top.length === 0 ? (
            <p className="mt-1 text-[11px] text-muted">Too many to list</p>
          ) : (
            <ul className="mt-1.5 space-y-px">
              {s.top.map(([value, n]) => (
                <li key={value} className="group relative flex items-center gap-1 text-[12px]">
                  <span aria-hidden className="absolute inset-y-0 left-0 rounded-sm bg-accent/10" style={{ width: `${(n / s.seen) * 100}%` }} />
                  <button
                    type="button"
                    onClick={() => onRefine(s.name, value, false)}
                    title={`Show only rows where ${s.name} is ${value}`}
                    className="relative min-w-0 flex-1 truncate rounded-sm px-1 py-px text-left font-mono text-fg outline-none hover:underline focus-visible:ring-1 focus-visible:ring-accent"
                  >
                    {value}
                  </button>
                  <span className="relative text-muted tabular-nums">{n}</span>
                  <button
                    type="button"
                    aria-label={`Hide rows where ${s.name} is ${value}`}
                    title={`Hide rows where ${s.name} is ${value}`}
                    onClick={() => onRefine(s.name, value, true)}
                    className="relative h-5 w-5 rounded-sm font-mono text-muted opacity-0 outline-none group-hover:opacity-100 hover:bg-line hover:text-fg focus-visible:opacity-100 focus-visible:ring-1 focus-visible:ring-accent"
                  >
                    −
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
      ))}
    </aside>
  );
}
