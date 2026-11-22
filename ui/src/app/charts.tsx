"use client";

import { useState } from "react";
import type { Histogram as Hist, Stats } from "./api/query/route";

const nf = new Intl.NumberFormat();

function bytes(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

// The smallest of 1, 2, 5 x 10^k that is at least n, so the top of the scale is a round number.
function niceCeil(n: number) {
  const p = 10 ** Math.floor(Math.log10(n));
  return [1, 2, 5, 10].map((m) => m * p).find((v) => v >= n)!;
}

function per(stepMs: number) {
  const s = stepMs / 1000;
  const [n, unit] =
    s % 86400 === 0 ? [s / 86400, "day"] : s % 3600 === 0 ? [s / 3600, "hour"] : s % 60 === 0 ? [s / 60, "minute"] : [s, "second"];
  return n === 1 ? unit : `${n} ${unit}s`;
}

function at(ms: number, stepMs: number) {
  const d = new Date(ms);
  if (stepMs >= 30 * 60_000) {
    return d.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  }
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: stepMs < 60_000 ? "2-digit" : undefined });
}

// Matches over time as one strip of bars. Hover a bar for its slot and count; HitsTable has the same numbers.
export function Histogram({ hist }: { hist: Hist }) {
  const [hover, setHover] = useState<number | null>(null);
  const { start, step, counts } = hist;
  const top = niceCeil(Math.max(...counts, 1));
  const end = start + counts.length * step;
  const left = hover === null ? 0 : ((hover + 0.5) / counts.length) * 100;

  return (
    <figure aria-label={`Matches per ${per(step)} from ${at(start, step)} to ${at(end, step)}`}>
      <div className="relative">
        <div aria-hidden onPointerLeave={() => setHover(null)} className="flex h-16 items-end gap-[2px] border-b border-line">
          {counts.map((c, i) => (
            <div key={i} onPointerEnter={() => setHover(i)} className="flex h-full min-w-0 flex-1 items-end justify-center">
              {c > 0 && (
                <div
                  className={`w-full max-w-6 rounded-t-[4px] bg-(--chart-1) ${hover === i ? "opacity-70" : ""}`}
                  style={{ height: `max(2px, ${(c / top) * 100}%)` }}
                />
              )}
            </div>
          ))}
        </div>
        <span aria-hidden className="pointer-events-none absolute top-0 left-0 bg-canvas pr-1 text-[10px] leading-none text-muted tabular-nums">
          {nf.format(top)}
        </span>
        {hover !== null && (
          <div
            className="pointer-events-none absolute bottom-full z-10 mb-1 rounded border border-line bg-canvas px-2 py-1 text-[12px] whitespace-nowrap"
            style={left < 50 ? { left: `${left}%` } : { right: `${100 - left}%` }}
          >
            <span className="font-semibold text-fg tabular-nums">{nf.format(counts[hover])}</span>{" "}
            <span className="text-muted">
              {at(start + hover * step, step)} – {at(start + (hover + 1) * step, step)}
            </span>
          </div>
        )}
      </div>
      <figcaption className="mt-1 flex justify-between text-[11px] text-muted tabular-nums">
        <span>{at(start, step)}</span>
        <span>per {per(step)}</span>
        <span>{at(end, step)}</span>
      </figcaption>
    </figure>
  );
}

export function HitsTable({ hist }: { hist: Hist }) {
  const { start, step, counts } = hist;
  return (
    <div className="max-h-56 overflow-auto">
      <table className="text-[12px] tabular-nums">
        <caption className="sr-only">Matches per {per(step)}</caption>
        <thead>
          <tr className="text-left text-muted">
            <th className="pr-8 font-normal">From</th>
            <th className="text-right font-normal">Matches</th>
          </tr>
        </thead>
        <tbody>
          {counts.map(
            (c, i) =>
              c > 0 && (
                <tr key={i}>
                  <td className="pr-8 text-muted">{at(start + i * step, step)}</td>
                  <td className="text-right text-fg">{nf.format(c)}</td>
                </tr>
              ),
          )}
        </tbody>
      </table>
    </div>
  );
}

// Where each block on disk went, in the order the storage node checks them.
const BLOCKS = [
  ["BlocksSkippedByTime", "Skipped by time range", "bg-(--chart-1)"],
  ["BlocksSkippedByBloom", "Skipped by bloom filter", "bg-(--chart-2)"],
  ["BlocksScanned", "Scanned", "bg-(--chart-3)"],
] as const;

export function QueryPlan({ stats: s }: { stats: Stats }) {
  const total = s.BlocksTotal;
  const pct = (n: number) => Math.round((n / total) * 100);
  const tiles = [
    ["Blocks scanned", total > 0 ? `${nf.format(s.BlocksScanned)} of ${nf.format(total)}` : "none on disk"],
    ["Partitions searched", `${s.PartitionsOpened} of ${s.PartitionsTotal}`],
    ["Parts searched", `${s.PartsOpened} of ${s.PartsTotal}`],
    ["Rows scanned on disk", nf.format(s.RowsScanned)],
    ["Rows matched on disk", nf.format(s.RowsMatched)],
    ["Read from disk", bytes(s.BytesReadFromDisk)],
  ];

  return (
    <div className="text-[12px] text-muted">
      {total > 0 && (
        <>
          <div
            role="img"
            aria-label={BLOCKS.map(([k, name]) => `${name}: ${s[k]} blocks`).join(", ")}
            className="flex h-6 max-w-3xl gap-[2px] text-[11px] font-medium text-white"
          >
            {BLOCKS.map(
              ([k, name, bg]) =>
                s[k] > 0 && (
                  <div key={k} title={`${name}: ${nf.format(s[k])} blocks (${pct(s[k])}%)`} className={`flex min-w-0 items-center rounded-[3px] px-1.5 ${bg}`} style={{ flex: `${s[k]} 1 0` }}>
                    {pct(s[k]) >= 12 && `${pct(s[k])}%`}
                  </div>
                ),
            )}
          </div>
          <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1">
            {BLOCKS.map(([k, name, bg]) => (
              <li key={k} className="flex items-center gap-1.5">
                <span aria-hidden className={`size-2.5 rounded-sm ${bg}`} />
                {name}
                <span className="text-fg tabular-nums">{nf.format(s[k])}</span>
              </li>
            ))}
          </ul>
        </>
      )}
      <dl className="mt-3 grid max-w-3xl grid-cols-2 gap-x-6 gap-y-2 sm:grid-cols-3">
        {tiles.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd className="text-[13px] font-medium text-fg tabular-nums">{value}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
