"use client";

import { Fragment, useState } from "react";
import type { Row } from "../api/query/route";

// RFC 3339 with all nine digits, the way /select writes "_time".
function isoNs(ts: string, ms: number) {
  const frac = ts.padStart(10, "0").slice(-9);
  return new Date(Math.floor(ms / 1000) * 1000).toISOString().replace(/\.\d{3}Z$/, `.${frac}Z`);
}

function Action({ label, onClick, children }: { label: string; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      onClick={onClick}
      className="h-5 w-5 rounded font-mono text-[13px] leading-none text-muted outline-none hover:bg-line hover:text-fg focus-visible:ring-1 focus-visible:ring-accent"
    >
      {children}
    </button>
  );
}

type Props = { row: Row; onRefine: (name: string, value: string, exclude: boolean) => void };

export function RowDetails({ row, onRefine }: Props) {
  const [copy, setCopy] = useState<"idle" | "done" | "manual">("idle");
  const time = isoNs(row.ts, row.ms);
  const json = JSON.stringify({ _time: time, ...Object.fromEntries(row.fields.map((f) => [f.Name, f.Value])) }, null, 2);

  async function onCopy() {
    try {
      await navigator.clipboard.writeText(json);
      setCopy("done");
      setTimeout(() => setCopy("idle"), 1500);
    } catch {
      setCopy("manual"); // no clipboard outside HTTPS or localhost: show the JSON to copy by hand
    }
  }

  return (
    <div className="border-b border-line bg-raised py-2 pr-4 pl-4">
      <dl className="grid grid-cols-[minmax(5rem,max-content)_1fr_auto] items-start gap-x-4 gap-y-0.5 font-mono text-[12.5px] leading-5">
        <dt className="text-muted">_time</dt>
        <dd className="text-fg">{time}</dd>
        <dd />
        {row.fields.map((f) => (
          <Fragment key={f.Name}>
            <dt className="text-muted">{f.Name}</dt>
            <dd className="min-w-0 break-all whitespace-pre-wrap text-fg">{f.Value}</dd>
            <dd className="flex gap-0.5">
              <Action label={`Show only rows where ${f.Name} is this value`} onClick={() => onRefine(f.Name, f.Value, false)}>
                +
              </Action>
              <Action label={`Hide rows where ${f.Name} is this value`} onClick={() => onRefine(f.Name, f.Value, true)}>
                −
              </Action>
            </dd>
          </Fragment>
        ))}
      </dl>
      <button type="button" onClick={onCopy} className="mt-2 text-[12px] text-accent outline-none hover:underline focus-visible:underline">
        {copy === "done" ? "Copied" : "Copy as JSON"}
      </button>
      {copy === "manual" && (
        <pre className="mt-2 max-h-64 overflow-auto rounded border border-line bg-canvas p-2 font-mono text-[12px] text-fg">{json}</pre>
      )}
    </div>
  );
}
