"use client";

import { useState } from "react";
import { FieldsSidebar } from "./fields-sidebar";
import { LogList } from "./log-list";
import { fieldTerm, highlightWords, withTerm } from "./query";
import { QueryBar } from "./query-bar";
import { Failed, NoData, NoMatches, Skeleton } from "./states";
import { Summary } from "./summary";
import { useSearch } from "./use-search";

export function Search() {
  const s = useSearch();
  const [fieldsOpen, setFieldsOpen] = useState(false);
  const { result, failure } = s;
  // A typo keeps the old results on screen and shows the message under the bar; anything
  // else replaces the results with an explanation.
  const blocking = failure && failure.kind !== "bad-query" ? failure : null;

  const refine = (name: string, value: string, exclude: boolean) =>
    s.search({ ...s.draft, q: withTerm(s.draft.q, fieldTerm(name, value), exclude) });

  return (
    <>
      <h1 className="sr-only">Search logs</h1>
      <QueryBar draft={s.draft} onDraft={s.setDraft} onSearch={s.search} live={s.live} onLive={s.setLive} loading={s.loading} />
      {failure?.kind === "bad-query" && (
        <p role="alert" className="border-b border-line bg-surface px-4 py-1.5 font-mono text-[12px] text-error">
          {failure.message}
        </p>
      )}
      <div className="flex flex-1">
        {result && result.rows.length > 0 && !blocking && (
          <FieldsSidebar
            rows={result.rows}
            onRefine={refine}
            className="sticky top-[49px] hidden h-[calc(100vh-49px)] w-60 shrink-0 overflow-y-auto border-r border-line bg-surface lg:block"
          />
        )}
        <main className={`min-w-0 flex-1 transition-opacity ${s.loading && result ? "opacity-60" : ""}`}>
          {blocking ? (
            <Failed failure={blocking} onRetry={() => s.search(s.draft)} />
          ) : !result ? (
            <Skeleton />
          ) : (
            <>
              <Summary result={result} fieldsOpen={fieldsOpen} onFields={() => setFieldsOpen(!fieldsOpen)} />
              {fieldsOpen && result.rows.length > 0 && (
                <FieldsSidebar rows={result.rows} onRefine={refine} className="border-b border-line bg-surface lg:hidden" />
              )}
              {result.rows.length > 0 ? (
                <LogList rows={result.rows} highlight={highlightWords(s.ran.q)} onRefine={refine} />
              ) : !s.ran.q && s.ran.range === "all" ? (
                <NoData router={result.router} onRetry={() => s.search(s.ran)} />
              ) : (
                <NoMatches ran={s.ran} onSearch={s.search} />
              )}
            </>
          )}
        </main>
      </div>
    </>
  );
}
