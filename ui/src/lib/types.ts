export type StorageNode = { id: string; listen: string; data: string };

export type ClusterConfig = {
  storage: StorageNode[];
  router: { listen: string };
  agent: { listen: string; dir: string; tail: string[] };
};

export type ProcStatus = {
  running: boolean;
  // Started before the UI's last restart and picked up again from cluster-data/procs.json.
  adopted?: boolean;
  pid?: number;
  startedAt?: number;
  uptimeSec?: number;
  exitCode?: number | null;
  output: string[];
  // Running with arguments that no longer match the saved config.
  stale: boolean;
};

export type TailProgress = { path: string; read: number; size: number | null };

export type ClusterState = {
  config: ClusterConfig;
  procs: Record<string, ProcStatus>;
  binary: { path: string; exists: boolean };
  tail: TailProgress[];
  sampleLog: string;
};
