"use client";

import { useEffect, useState } from "react";
import type { ClusterConfig, ClusterState, ProcStatus } from "@/lib/types";

const input =
  "rounded-md border border-zinc-300 bg-white px-2 py-1 font-mono text-[13px] outline-none focus:border-zinc-500 disabled:opacity-60 dark:border-zinc-700 dark:bg-zinc-900";
const button =
  "rounded-md border border-zinc-300 px-2.5 py-1 text-xs font-medium hover:bg-zinc-100 disabled:opacity-40 dark:border-zinc-700 dark:hover:bg-zinc-800";
const primary =
  "rounded-md bg-zinc-900 px-3 py-1.5 text-xs font-medium text-white hover:bg-zinc-700 disabled:opacity-40 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300";

type Cluster = ReturnType<typeof useCluster>;

export function useCluster() {
  const [state, setState] = useState<ClusterState | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");

  useEffect(() => {
    let alive = true;
    const load = () =>
      fetch("/api/cluster")
        .then((r) => r.json())
        .then(
          (d) => alive && (d.error ? setError(d.error) : setState(d)),
          (e: Error) => alive && setError(e.message),
        );
    load();
    const timer = setInterval(load, 2000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  async function send(method: string, body: unknown, label: string) {
    setBusy(label);
    setError("");
    try {
      const res = await fetch("/api/cluster", {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (data.state) setState(data.state);
      else if (!data.error) setState(data);
      if (data.error) setError(data.error);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy("");
    }
  }

  return {
    state,
    error,
    busy,
    act: (action: string, id?: string) => send("POST", { action, id }, `${action}:${id ?? ""}`),
    save: (config: ClusterConfig) => send("PUT", config, "save"),
  };
}

function Status({ p }: { p?: ProcStatus }) {
  let color = "bg-zinc-300 dark:bg-zinc-600";
  let text = "stopped";
  if (p?.running) {
    color = "bg-emerald-500";
    const mins = Math.floor((p.uptimeSec ?? 0) / 60);
    text = `running · pid ${p.pid} · ${mins < 1 ? "just now" : `${mins}m`}`;
  } else if (p?.exitCode != null && p.exitCode !== 0) {
    color = "bg-red-500";
    text = `exited ${p.exitCode}`;
  }
  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-zinc-500">
      <span className={`h-2 w-2 rounded-full ${color}`} />
      {text}
      {p?.stale && <span className="text-amber-600 dark:text-amber-400">· restart to apply changes</span>}
    </span>
  );
}

// Saves on blur or Enter; keyed on the saved value so polling doesn't clobber typing.
function Field({ label, value, onSave, width = "w-44" }: { label: string; value: string; onSave: (v: string) => void; width?: string }) {
  return (
    <label className="flex items-center gap-1.5 text-xs text-zinc-500">
      {label}
      <input
        key={value}
        defaultValue={value}
        className={`${input} ${width}`}
        onBlur={(e) => {
          const v = e.target.value.trim();
          if (v && v !== value) onSave(v);
          else e.target.value = value;
        }}
        onKeyDown={(e) => e.key === "Enter" && e.currentTarget.blur()}
      />
    </label>
  );
}

function Output({ lines }: { lines: string[] }) {
  if (lines.length === 0) return null;
  return (
    <details className="mt-1.5">
      <summary className="cursor-pointer text-xs text-zinc-500 select-none">output</summary>
      <pre className="mt-1 max-h-48 overflow-auto rounded bg-zinc-100 p-2 text-[11px] leading-relaxed text-zinc-700 dark:bg-zinc-900 dark:text-zinc-300">
        {lines.slice(-40).join("\n")}
      </pre>
    </details>
  );
}

function Proc({ c, id, name, children, extra }: { c: Cluster; id: string; name: string; children?: React.ReactNode; extra?: React.ReactNode }) {
  const p = c.state?.procs[id];
  const working = c.busy.endsWith(`:${id}`);
  return (
    <div className="border-b border-zinc-200 px-3 py-2.5 last:border-b-0 dark:border-zinc-800">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <span className="w-20 font-mono text-sm font-medium">{name}</span>
        {children}
        <div className="ml-auto flex items-center gap-2">
          <Status p={p} />
          {p?.running ? (
            <>
              {p.stale && (
                <button className={button} disabled={working} onClick={() => c.act("restart", id)}>
                  Restart
                </button>
              )}
              <button className={button} disabled={working} onClick={() => c.act("stop", id)}>
                {working ? "…" : "Stop"}
              </button>
            </>
          ) : (
            <button className={button} disabled={working} onClick={() => c.act("start", id)}>
              {working ? "…" : "Start"}
            </button>
          )}
          {extra}
        </div>
      </div>
      <Output lines={p?.output ?? []} />
    </div>
  );
}

function Section({ title, hint, children }: { title: string; hint?: React.ReactNode; children: React.ReactNode }) {
  return (
    <section className="mb-6">
      <div className="mb-2 flex items-baseline justify-between gap-4">
        <h2 className="text-sm font-semibold">{title}</h2>
        {hint && <span className="text-xs text-zinc-500">{hint}</span>}
      </div>
      <div className="rounded-md border border-zinc-200 dark:border-zinc-800">{children}</div>
    </section>
  );
}

function ErrorBar({ c }: { c: Cluster }) {
  if (!c.error) return null;
  return (
    <p className="mb-4 rounded-md border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">
      {c.error}
    </p>
  );
}

export function ClusterTab({ c }: { c: Cluster }) {
  if (!c.state) return <ErrorBar c={c} />;
  const { config, binary } = c.state;
  const set = (next: ClusterConfig) => c.save(next);

  function addNode() {
    const last = config.storage[config.storage.length - 1];
    const n = Math.max(0, ...config.storage.map((s) => Number(s.id.replace(/\D/g, "")) || 0)) + 1;
    const port = Number(last?.listen.split(":").pop() ?? 9000) + 1;
    set({
      ...config,
      storage: [...config.storage, { id: `storage-${n}`, listen: `127.0.0.1:${port}`, data: `cluster-data/storage-${n}` }],
    });
  }

  return (
    <div>
      <ErrorBar c={c} />
      <div className="mb-5 flex flex-wrap items-center gap-3">
        <span className="text-xs text-zinc-500">
          binary <code className="font-mono">{binary.path}</code> {binary.exists ? "✓" : "— not built yet"}
        </span>
        <button className={button} disabled={!!c.busy} onClick={() => c.act("build")}>
          {c.busy === "build:" ? "Building…" : binary.exists ? "Rebuild" : "Build"}
        </button>
        <div className="ml-auto flex gap-2">
          <button className={primary} disabled={!!c.busy} onClick={() => c.act("startAll")}>
            {c.busy === "startAll:" ? "Starting…" : "Start all"}
          </button>
          <button className={button} disabled={!!c.busy} onClick={() => c.act("stopAll")}>
            {c.busy === "stopAll:" ? "Stopping…" : "Stop all"}
          </button>
        </div>
      </div>

      <Section title="Storage nodes" hint="each keeps its own share of the data">
        {config.storage.map((node, i) => {
          const update = (patch: Partial<typeof node>) =>
            set({ ...config, storage: config.storage.map((n, j) => (j === i ? { ...n, ...patch } : n)) });
          const running = c.state?.procs[node.id]?.running;
          return (
            <Proc
              key={node.id}
              c={c}
              id={node.id}
              name={node.id}
              extra={
                <button
                  className={button}
                  disabled={running || config.storage.length === 1}
                  title={running ? "stop it first" : "remove this node"}
                  onClick={() => set({ ...config, storage: config.storage.filter((_, j) => j !== i) })}
                >
                  Remove
                </button>
              }
            >
              <Field label="listen" value={node.listen} width="w-36" onSave={(listen) => update({ listen })} />
              <Field label="data" value={node.data} onSave={(data) => update({ data })} />
            </Proc>
          );
        })}
        <div className="px-3 py-2">
          <button className={button} onClick={addNode}>
            + Add storage node
          </button>
        </div>
      </Section>

      <Section title="Router" hint={`spreads writes across ${config.storage.length} node${config.storage.length === 1 ? "" : "s"} and answers searches`}>
        <Proc c={c} id="router" name="router">
          <Field label="listen" value={config.router.listen} width="w-36" onSave={(listen) => set({ ...config, router: { listen } })} />
        </Proc>
      </Section>
    </div>
  );
}

function bytes(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

export function AgentTab({ c }: { c: Cluster }) {
  const [path, setPath] = useState("");
  if (!c.state) return <ErrorBar c={c} />;
  const { config, tail, sampleLog } = c.state;
  const agent = config.agent;
  const setAgent = (patch: Partial<typeof agent>) => c.save({ ...config, agent: { ...agent, ...patch } });
  const addPath = (p: string) => {
    const v = p.trim();
    if (v && !agent.tail.includes(v)) setAgent({ tail: [...agent.tail, v] });
    setPath("");
  };

  return (
    <div>
      <ErrorBar c={c} />
      <Section title="Agent" hint={<>sends everything to the router at <code className="font-mono">{config.router.listen}</code></>}>
        <Proc c={c} id="agent" name="agent">
          <Field label="listen" value={agent.listen} width="w-36" onSave={(listen) => setAgent({ listen })} />
          <Field label="queue" value={agent.dir} onSave={(dir) => setAgent({ dir })} />
        </Proc>
      </Section>

      <Section title="Log files to collect" hint="a file that doesn't exist yet is picked up when it appears">
        {tail.length === 0 && <p className="px-3 py-2.5 text-xs text-zinc-500">No files yet.</p>}
        {tail.map((t) => (
          <div key={t.path} className="flex items-center gap-3 border-b border-zinc-200 px-3 py-2 dark:border-zinc-800">
            <code className="min-w-0 flex-1 truncate font-mono text-[13px]" title={t.path}>
              {t.path}
            </code>
            <span className="text-xs whitespace-nowrap text-zinc-500">
              {t.size === null ? "not created yet" : `read ${bytes(t.read)} of ${bytes(t.size)}`}
            </span>
            <button className={button} onClick={() => setAgent({ tail: agent.tail.filter((p) => p !== t.path) })}>
              Remove
            </button>
          </div>
        ))}
        <form
          className="flex gap-2 px-3 py-2"
          onSubmit={(e) => {
            e.preventDefault();
            addPath(path);
          }}
        >
          <input
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="/var/log/app.log"
            className={`${input} min-w-0 flex-1`}
          />
          <button className={button} type="submit" disabled={!path.trim()}>
            Add
          </button>
        </form>
      </Section>

      <Section title="Sample logs" hint="fake app logs, five lines a second">
        <Proc
          c={c}
          id="loggen"
          name="loggen"
          extra={
            !agent.tail.includes(sampleLog) && (
              <button className={button} onClick={() => addPath(sampleLog)}>
                Collect it
              </button>
            )
          }
        >
          <span className="text-xs text-zinc-500">
            writes <code className="font-mono">{sampleLog}</code>
          </span>
        </Proc>
      </Section>

      <Section title="Send JSON over HTTP">
        <pre className="overflow-x-auto px-3 py-2.5 font-mono text-[12px] text-zinc-600 dark:text-zinc-400">
          {`curl -X POST --data-binary @app.jsonl \\\n  'http://${agent.listen}/ingest?stream=service'`}
        </pre>
      </Section>
    </div>
  );
}
