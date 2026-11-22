"use client";

import { useState } from "react";
import type { Row } from "../api/query/route";
import { levelOf, type Level } from "./query";
import { RowDetails } from "./row-details";

const pad = (n: number, w = 2) => String(n).padStart(w, "0");
const clock = (ms: number) => {
  const d = new Date(ms);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
};
const day = (ms: number) => new Date(ms).toLocaleDateString(undefined, { weekday: "short", day: "numeric", month: "short", year: "numeric" });

// The level is always written out; the stripe only adds emphasis for errors and warnings.
const LEVEL: Record<Level, { label: string; text: string; stripe: string }> = {
  error: { label: "ERROR", text: "text-error", stripe: "border-l-error-line" },
  warn: { label: "WARN", text: "text-warn", stripe: "border-l-warn-line" },
  info: { label: "INFO", text: "text-muted", stripe: "border-l-transparent" },
  debug: { label: "DEBUG", text: "text-muted", stripe: "border-l-transparent" },
};

function Highlighted({ text, match }: { text: string; match: Set<string> }) {
  if (match.size === 0) return <>{text}</>;
  return (
    <>
      {text.split(/([\p{L}\p{N}]+)/u).map((part, i) =>
        match.has(part) ? (
          <mark key={i} className="rounded-[2px] bg-mark text-inherit">
            {part}
          </mark>
        ) : (
          part
        ),
      )}
    </>
  );
}

type Props = { rows: Row[]; highlight: Set<string>; onRefine: (name: string, value: string, exclude: boolean) => void };

export function LogList({ rows, highlight, onRefine }: Props) {
  // Keyed by time and message rather than position, so an open row stays open while Live
  // pushes new rows in above it.
  const [open, setOpen] = useState<Set<string>>(() => new Set());
  const levels = rows.map((r) => levelOf(r.fields));
  const showLevel = levels.some(Boolean);
  const toggle = (key: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (!next.delete(key)) next.add(key);
      return next;
    });

  return (
    <ol aria-label="Log lines" className="font-mono text-[13px] leading-5">
      {rows.map((row, i) => {
        const msg = row.fields.find((f) => f.Name === "_msg")?.Value ?? "";
        const lv = levels[i];
        const style = lv ? LEVEL[lv.level] : null;
        const rest = row.fields.filter((f) => f.Name !== "_msg" && f.Name !== lv?.fromField);
        const key = `${row.ts} ${msg}`;
        const isOpen = open.has(key);
        const newDay = i === 0 || day(rows[i - 1].ms) !== day(row.ms);
        return (
          <li key={`${i} ${key}`}>
            {newDay && <div className="border-b border-line bg-surface px-4 py-1 font-sans text-[11px] font-medium text-muted">{day(row.ms)}</div>}
            <div
              role="button"
              tabIndex={0}
              aria-expanded={isOpen}
              onClick={() => toggle(key)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  toggle(key);
                }
              }}
              className={`grid cursor-pointer gap-x-3 border-b border-l-2 border-b-line/60 py-1 pr-4 pl-3.5 outline-none hover:bg-raised focus-visible:bg-raised focus-visible:ring-1 focus-visible:ring-accent focus-visible:ring-inset ${
                showLevel ? "grid-cols-[auto_3rem_1fr]" : "grid-cols-[auto_1fr]"
              } ${style?.stripe ?? "border-l-transparent"} ${isOpen ? "bg-raised" : ""}`}
            >
              <time dateTime={new Date(row.ms).toISOString()} title={`${row.ts} ns`} className="text-muted tabular-nums">
                {clock(row.ms)}
              </time>
              {showLevel && <span className={`text-[11px] font-semibold ${style?.text ?? ""}`}>{style?.label ?? ""}</span>}
              <span className={`col-span-full min-w-0 break-words whitespace-pre-wrap text-fg sm:col-span-1 ${isOpen ? "" : "line-clamp-2"}`}>
                <Highlighted text={msg} match={highlight} />
                {rest.length > 0 && <span className="ml-3 text-muted">{rest.map((f) => `${f.Name}=${f.Value}`).join("  ")}</span>}
              </span>
            </div>
            {isOpen && <RowDetails row={row} onRefine={onRefine} />}
          </li>
        );
      })}
    </ol>
  );
}
