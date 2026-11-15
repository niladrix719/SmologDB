"use client";

import { useState } from "react";
import { AgentTab, ClusterTab, useCluster } from "./cluster";
import { Search } from "./search";

const TABS = ["Search", "Cluster", "Agent"] as const;

export default function Home() {
  const [tab, setTab] = useState<(typeof TABS)[number]>("Search");
  const cluster = useCluster();
  const procs = cluster.state?.procs ?? {};
  const up = Object.entries(procs).filter(([id, p]) => p.running && id !== "build").length;

  return (
    <div className="mx-auto w-full max-w-6xl flex-1 px-4 py-6 sm:px-6">
      <header className="mb-5 flex flex-wrap items-center gap-x-6 gap-y-3">
        <h1 className="text-lg font-semibold tracking-tight">SmologDB</h1>
        <nav className="flex gap-1">
          {TABS.map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`rounded-md px-3 py-1 text-sm ${
                tab === t
                  ? "bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900"
                  : "text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
        <span className="ml-auto text-xs text-zinc-500">
          {cluster.state ? `${up} process${up === 1 ? "" : "es"} running` : ""}
        </span>
      </header>
      {tab === "Search" && <Search />}
      {tab === "Cluster" && <ClusterTab c={cluster} />}
      {tab === "Agent" && <AgentTab c={cluster} />}
    </div>
  );
}
