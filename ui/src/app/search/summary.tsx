"use client";

import { useState } from "react";
import type { QueryResult } from "../api/query/route";
import { Histogram, HitsTable, QueryPlan } from "../charts";

function Toggle({ on, onClick, className = "", children }: { on: boolean; onClick: () => void; className?: string; children: React.ReactNode }) {
  return (
    <button
      type="button"
      aria-expanded={on}
      onClick={onClick}
      className={`rounded px-1.5 py-0.5 outline-none hover:bg-raised hover:text-fg focus-visible:ring-1 focus-visible:ring-accent ${on ? "bg-raised text-fg" : ""} ${className}`}
    >
      {children}
    </button>
  );
}

type Props = { result: QueryResult; fieldsOpen: boolean; onFields: () => void };

export function Summary({ result, fieldsOpen, onFields }: Props) {
  const [panel, setPanel] = useState<"plan" | "table" | null>(null);
  const { matched, rows, hist, stats, nodesQueried, nodesFailed, tookMs } = result;
  const toggle = (p: "plan" | "table") => setPanel(panel === p ? null : p);

  return (
    <section aria-label="Overview" className="border-b border-line px-4 pt-2 pb-3">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[12px] text-muted">
        <span>
          <span className="font-medium text-fg tabular-nums">{(matched ?? rows.length).toLocaleString()}</span> {matched === null ? "newest rows" : "matched"}
        </span>
        {matched !== null && matched > rows.length && <span>showing the newest {rows.length.toLocaleString()}</span>}
        {nodesQueried !== null && (
          <span>
            {nodesQueried} {nodesQueried === 1 ? "node" : "nodes"}
          </span>
        )}
        <span className="tabular-nums">{tookMs} ms</span>
        {nodesFailed > 0 && (
          <span role="status" className="font-medium text-warn">
            Partial: {nodesFailed} {nodesFailed === 1 ? "node" : "nodes"} didn&apos;t answer
          </span>
        )}
        <span className="ml-auto flex gap-1">
          {rows.length > 0 && (
            <Toggle className="lg:hidden" on={fieldsOpen} onClick={onFields}>
              Fields
            </Toggle>
          )}
          {stats && (
            <Toggle on={panel === "plan"} onClick={() => toggle("plan")}>
              How it ran
            </Toggle>
          )}
          {hist && matched ? (
            <Toggle on={panel === "table"} onClick={() => toggle("table")}>
              Table
            </Toggle>
          ) : null}
        </span>
      </div>
      {hist && matched ? (
        <div className="mt-3">
          <Histogram hist={hist} />
        </div>
      ) : null}
      {panel === "plan" && stats && (
        <div className="mt-3">
          <QueryPlan stats={stats} />
        </div>
      )}
      {panel === "table" && hist && (
        <div className="mt-3">
          <HitsTable hist={hist} />
        </div>
      )}
    </section>
  );
}
