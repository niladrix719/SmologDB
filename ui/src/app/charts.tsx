"use client";

import { useState } from "react";
import type { Histogram as Hist, Stats } from "./api/query/route";
import { bytes } from "./cluster";

const nf = new Intl.NumberFormat();

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

export function Histogram({ hist }: { hist: Hist }) {
  const [hover, setHover] = useState<number | null>(null);
  const { start, step, counts } = hist;
  const top = niceCeil(Math.max(...counts, 1));
  const left = hover === null ? 0 : ((hover + 0.5) / counts.length) * 100;

  return (
    <figure className="mb-3">
      <figcaption className="flex justify-between text-xs text-zinc-500">
        <span>Matches per {per(step)}</span>
        <span className="tabular-nums">{nf.format(top)}</span>
      </figcaption>
      <div className="relative">
        <div
          aria-hidden
          onPointerLeave={() => setHover(null)}
          className="flex h-24 items-end gap-[2px] border-y border-zinc-200 dark:border-zinc-800"
        >
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
        {hover !== null && (
          <div
            className="pointer-events-none absolute bottom-full mb-1 rounded-md border border-zinc-200 bg-white px-2 py-1 text-xs shadow-sm dark:border-zinc-700 dark:bg-zinc-900"
            style={left < 50 ? { left: `${left}%` } : { right: `${100 - left}%` }}
          >
            <strong className="font-semibold tabular-nums">{nf.format(counts[hover])}</strong>{" "}
            <span className="text-zinc-500">
              {at(start + hover * step, step)} – {at(start + (hover + 1) * step, step)}
            </span>
          </div>
        )}
      </div>
      <div className="mt-1 flex justify-between text-[11px] text-zinc-500 tabular-nums">
        <span>{at(start, step)}</span>
        <span>{at(start + counts.length * step, step)}</span>
      </div>
      <details className="text-xs text-zinc-500">
        <summary className="cursor-pointer select-none">Table</summary>
        <table className="mt-1 tabular-nums">
          <tbody>
            {counts.map(
              (c, i) =>
                c > 0 && (
                  <tr key={i}>
                    <td className="pr-6">{at(start + i * step, step)}</td>
                    <td className="text-right text-zinc-800 dark:text-zinc-200">{nf.format(c)}</td>
                  </tr>
                ),
            )}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

// Where each block on disk went: in the order the storage node checks them.
const BLOCKS = [
  ["BlocksSkippedByTime", "Skipped by time range", "bg-(--chart-1)"],
  ["BlocksSkippedByBloom", "Skipped by bloom filter", "bg-(--chart-2)"],
  ["BlocksScanned", "Scanned", "bg-(--chart-3)"],
] as const;

export function QueryPlan({ stats: s }: { stats: Stats }) {
  const total = s.BlocksTotal;
  const pct = (n: number) => Math.round((n / total) * 100);
  const tiles = [
    ["Partitions searched", `${s.PartitionsOpened} of ${s.PartitionsTotal}`],
    ["Parts searched", `${s.PartsOpened} of ${s.PartsTotal}`],
    ["Rows scanned on disk", nf.format(s.RowsScanned)],
    ["Rows matched on disk", nf.format(s.RowsMatched)],
    ["Read from disk", bytes(s.BytesReadFromDisk)],
  ];

  return (
    <details className="mb-3 text-xs text-zinc-500">
      <summary className="cursor-pointer select-none">
        How it ran:{" "}
        {total > 0
          ? `scanned ${nf.format(s.BlocksScanned)} of ${nf.format(total)} blocks on disk (${pct(s.BlocksScanned)}%)`
          : "no blocks on disk in this range"}
        , {bytes(s.BytesReadFromDisk)} read
      </summary>
      <div className="mt-2 rounded-md border border-zinc-200 p-3 dark:border-zinc-800">
        {total > 0 && (
          <>
            <div
              role="img"
              aria-label={BLOCKS.map(([k, name]) => `${name}: ${s[k]} blocks`).join(", ")}
              className="flex h-7 gap-[2px] text-[11px] font-medium text-white"
            >
              {BLOCKS.map(
                ([k, name, bg]) =>
                  s[k] > 0 && (
                    <div
                      key={k}
                      title={`${name}: ${nf.format(s[k])} blocks (${pct(s[k])}%)`}
                      className={`flex min-w-0 items-center rounded-[4px] px-1.5 ${bg}`}
                      style={{ flex: `${s[k]} 1 0` }}
                    >
                      {pct(s[k]) >= 12 && `${pct(s[k])}%`}
                    </div>
                  ),
              )}
            </div>
            <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1">
              {BLOCKS.map(([k, name, bg]) => (
                <li key={k} className="flex items-center gap-1.5">
                  <span className={`size-2.5 rounded-sm ${bg}`} />
                  {name}
                  <span className="text-zinc-800 tabular-nums dark:text-zinc-200">{nf.format(s[k])}</span>
                </li>
              ))}
            </ul>
          </>
        )}
        <dl className={`grid grid-cols-2 gap-3 sm:grid-cols-5 ${total > 0 ? "mt-3" : ""}`}>
          {tiles.map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">{value}</dd>
            </div>
          ))}
        </dl>
      </div>
    </details>
  );
}
