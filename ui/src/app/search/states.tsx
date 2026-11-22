"use client";

import { RANGES, type Failure, type Query } from "./use-search";

const rangeLabel = (range: string) => RANGES.find(([v]) => v === range)?.[1].toLowerCase() ?? range;

function Message({ title, children, alert }: { title: string; children: React.ReactNode; alert?: boolean }) {
  return (
    <section role={alert ? "alert" : "status"} className="max-w-2xl px-4 py-10 text-[13px] leading-6 text-muted">
      <h2 className="mb-1 text-[15px] font-semibold text-fg">{title}</h2>
      {children}
    </section>
  );
}

function Code({ children }: { children: string }) {
  return <pre className="mt-1.5 mb-4 overflow-x-auto rounded border border-line bg-surface px-3 py-2 font-mono text-[12px] leading-5 text-fg">{children}</pre>;
}

const linkButton = "rounded border border-line bg-canvas px-2.5 py-1 text-[12px] text-fg outline-none hover:bg-raised focus-visible:ring-1 focus-visible:ring-accent";

export function NoMatches({ ran, onSearch }: { ran: Query; onSearch: (q: Query) => void }) {
  const wider = (["24h", "all"] as const).filter((r) => r !== ran.range && !(ran.range === "7d" && r === "24h"));
  return (
    <Message title="Nothing matched">
      <p>
        No log lines {ran.q ? <>match <code className="font-mono text-fg">{ran.q}</code></> : "arrived"} in the {rangeLabel(ran.range)}.
      </p>
      <div className="mt-3 flex flex-wrap gap-2">
        {wider.map((range) => (
          <button key={range} type="button" className={linkButton} onClick={() => onSearch({ ...ran, range })}>
            Search {rangeLabel(range)}
          </button>
        ))}
        {ran.q && (
          <button type="button" className={linkButton} onClick={() => onSearch({ ...ran, q: "" })}>
            Clear the search
          </button>
        )}
      </div>
    </Message>
  );
}

export function NoData({ router, onRetry }: { router: string; onRetry: () => void }) {
  const hostPort = router.replace(/^https?:\/\//, "").replace(/\/$/, "");
  return (
    <Message title="No logs yet">
      <p>The router at {hostPort} has nothing stored. Send it a first line either way, then search again.</p>
      <p className="mt-4 text-fg">Follow a log file with the agent:</p>
      <Code>{`smologdb agent -send-to ${hostPort} \\\n  -dir ./agent-data -listen 127.0.0.1:9429 \\\n  -tail /var/log/app.log`}</Code>
      <p className="text-fg">Or, with the agent running, push a JSON line:</p>
      <Code>{`echo '{"_msg":"hello from curl","service":"demo"}' | \\\n  curl -X POST --data-binary @- 'http://127.0.0.1:9429/ingest?stream=service'`}</Code>
      <button type="button" className={linkButton} onClick={onRetry}>
        Search again
      </button>
    </Message>
  );
}

export function Failed({ failure, onRetry }: { failure: Failure; onRetry: () => void }) {
  const down = failure.kind === "router-down";
  return (
    <Message alert title={down ? "Can't reach the router" : failure.kind === "network" ? "Can't reach this page's server" : "The router answered with an error"}>
      {down ? (
        <>
          <p>This page looks for the router at {failure.router ?? "127.0.0.1:9000"}, and nothing answered there.</p>
          <p className="mt-4 text-fg">Start a local cluster from the repository:</p>
          <Code>make demo</Code>
          <p className="text-fg">Or point this page at your router:</p>
          <Code>SMOLOGDB_ROUTER=http://your-router:9000 make ui</Code>
        </>
      ) : (
        <Code>{failure.message}</Code>
      )}
      <button type="button" className={linkButton} onClick={onRetry}>
        Try again
      </button>
    </Message>
  );
}

export function Skeleton() {
  return (
    <div aria-busy="true" aria-label="Loading log lines" className="space-y-2 px-4 py-4">
      {Array.from({ length: 12 }, (_, i) => (
        <div key={i} className="h-3.5 rounded-sm bg-raised motion-safe:animate-pulse" style={{ width: `${55 + ((i * 37) % 40)}%` }} />
      ))}
    </div>
  );
}
