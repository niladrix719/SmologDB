import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import type { ClusterConfig, ClusterState, ProcStatus, TailProgress } from "./types";

export const REPO = process.env.SMOLOGDB_REPO ?? path.resolve(process.cwd(), "..");
const BIN = process.env.SMOLOGDB_BIN ?? path.join(REPO, "smologdb");
const DATA = path.join(REPO, "cluster-data");
const CONFIG_PATH = path.join(DATA, "cluster.json");
const SAMPLE_LOG = path.join(DATA, "sample.log");
const OUTPUT_LINES = 200;

const DEFAULTS: ClusterConfig = {
  storage: [
    { id: "storage-1", listen: "127.0.0.1:9001", data: "cluster-data/storage-1" },
    { id: "storage-2", listen: "127.0.0.1:9002", data: "cluster-data/storage-2" },
  ],
  router: { listen: "127.0.0.1:9000" },
  agent: { listen: "127.0.0.1:9429", dir: "cluster-data/agent", tail: [] },
};

type Proc = {
  child: ChildProcess | null;
  pid?: number;
  startedAt?: number;
  exitCode?: number | null;
  output: string[];
  args: string[];
};

// Survives module reloads in `next dev`, so running processes aren't forgotten.
const g = globalThis as unknown as { __smologdbProcs?: Map<string, Proc> };
const procs = (g.__smologdbProcs ??= new Map<string, Proc>());

const abs = (p: string) => (path.isAbsolute(p) ? p : path.join(REPO, p));

export function loadConfig(): ClusterConfig {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
  } catch {
    return structuredClone(DEFAULTS);
  }
}

const ADDR = /^[A-Za-z0-9.\-]+:\d{1,5}$/;

function validate(input: unknown): ClusterConfig {
  const c = input as ClusterConfig;
  const str = (v: unknown) => typeof v === "string" && v.trim() !== "";
  if (!c || !Array.isArray(c.storage) || c.storage.length === 0) throw new Error("need at least one storage node");
  const ids = new Set<string>();
  for (const n of c.storage) {
    if (!str(n?.id) || !/^[\w-]+$/.test(n.id) || ids.has(n.id)) throw new Error(`bad or duplicate node id ${n?.id}`);
    ids.add(n.id);
    if (!ADDR.test(n.listen)) throw new Error(`${n.id}: listen must look like 127.0.0.1:9001`);
    if (!str(n.data)) throw new Error(`${n.id}: data directory is required`);
  }
  if (!ADDR.test(c.router?.listen)) throw new Error("router: listen must look like 127.0.0.1:9000");
  if (!ADDR.test(c.agent?.listen)) throw new Error("agent: listen must look like 127.0.0.1:9429");
  if (!str(c.agent.dir)) throw new Error("agent: queue directory is required");
  if (!Array.isArray(c.agent.tail) || !c.agent.tail.every(str)) throw new Error("agent: tail paths must be strings");
  if (c.agent.tail.some((p) => p.includes(","))) throw new Error("agent: tail paths can't contain commas");
  const listens = [...c.storage.map((n) => n.listen), c.router.listen, c.agent.listen];
  if (new Set(listens).size !== listens.length) throw new Error("two processes share a listen address");
  return {
    storage: c.storage.map(({ id, listen, data }) => ({ id, listen, data: data.trim() })),
    router: { listen: c.router.listen },
    agent: { listen: c.agent.listen, dir: c.agent.dir.trim(), tail: c.agent.tail.map((p) => p.trim()) },
  };
}

export function saveConfig(input: unknown) {
  const cfg = validate(input);
  for (const [id, p] of procs) {
    if (p.child && id.startsWith("storage-") && !cfg.storage.some((n) => n.id === id)) {
      throw new Error(`stop ${id} before removing it`);
    }
  }
  fs.mkdirSync(DATA, { recursive: true });
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + "\n");
}

type Spec = { cmd: string; args: string[]; listen?: string };

function spec(id: string, cfg: ClusterConfig): Spec {
  if (id === "router") {
    return { cmd: BIN, args: ["router", "-nodes", cfg.storage.map((n) => n.listen).join(","), "-listen", cfg.router.listen], listen: cfg.router.listen };
  }
  if (id === "agent") {
    const a = cfg.agent;
    const tail = a.tail.length ? ["-tail", a.tail.map(abs).join(",")] : [];
    return { cmd: BIN, args: ["agent", "-send-to", cfg.router.listen, "-dir", abs(a.dir), "-listen", a.listen, ...tail], listen: a.listen };
  }
  if (id === "loggen") {
    return { cmd: "bash", args: [path.join(REPO, "scripts", "loggen.sh"), SAMPLE_LOG, "0.2"] };
  }
  const node = cfg.storage.find((n) => n.id === id);
  if (!node) throw new Error(`no process called ${id}`);
  return { cmd: BIN, args: ["storage", "-data", abs(node.data), "-listen", node.listen], listen: node.listen };
}

function isOpen(addr: string, timeoutMs = 300): Promise<boolean> {
  const i = addr.lastIndexOf(":");
  return new Promise((resolve) => {
    const sock = net.createConnection({ host: addr.slice(0, i), port: Number(addr.slice(i + 1)) });
    const done = (open: boolean) => {
      sock.destroy();
      resolve(open);
    };
    sock.setTimeout(timeoutMs, () => done(false));
    sock.once("connect", () => done(true));
    sock.once("error", () => done(false));
  });
}

function record(p: Proc, chunk: Buffer | string) {
  for (const line of chunk.toString().split("\n")) {
    if (line.trim()) p.output.push(line);
  }
  if (p.output.length > OUTPUT_LINES) p.output.splice(0, p.output.length - OUTPUT_LINES);
}

function run(id: string, cmd: string, args: string[]): Proc {
  const child = spawn(cmd, args, { cwd: REPO, stdio: ["ignore", "pipe", "pipe"] });
  const p: Proc = { child, pid: child.pid, startedAt: Date.now(), exitCode: null, output: [], args };
  procs.set(id, p);
  child.stdout?.on("data", (c) => record(p, c));
  child.stderr?.on("data", (c) => record(p, c));
  child.on("error", (err) => {
    record(p, `failed to start: ${err.message}`);
    p.child = null;
    p.exitCode = -1;
  });
  child.on("exit", (code, signal) => {
    p.child = null;
    p.exitCode = code ?? -1;
    record(p, `exited (${signal ?? code})`);
  });
  return p;
}

function exited(p: Proc): Promise<void> {
  return new Promise((resolve) => {
    if (!p.child) return resolve();
    p.child.once("exit", () => resolve());
  });
}

export async function build() {
  const p = run("build", "go", ["build", "-o", BIN, "./cmd/smologdb"]);
  await exited(p);
  if (p.exitCode !== 0) throw new Error(`go build failed: ${p.output.slice(-3).join(" ")}`);
}

export async function start(id: string) {
  if (procs.get(id)?.child) return;
  const cfg = loadConfig();
  const s = spec(id, cfg);
  if (s.listen && (await isOpen(s.listen))) {
    throw new Error(`${s.listen} is already in use (is make demo or another smologdb running?)`);
  }
  if (s.cmd === BIN && !fs.existsSync(/*turbopackIgnore: true*/ BIN)) await build();
  fs.mkdirSync(DATA, { recursive: true });
  const p = run(id, s.cmd, s.args);
  if (!s.listen) return;
  for (const deadline = Date.now() + 5000; Date.now() < deadline; ) {
    if (!p.child) throw new Error(`${id} exited: ${p.output.slice(-2).join(" ")}`);
    if (await isOpen(s.listen)) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`${id} did not start listening on ${s.listen}`);
}

export async function stop(id: string) {
  const p = procs.get(id);
  if (!p?.child) return;
  const child = p.child;
  child.kill("SIGTERM");
  // smologdb flushes on SIGTERM; only force it if that hangs.
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  await exited(p);
  clearTimeout(timer);
}

export async function startAll() {
  const cfg = loadConfig();
  await Promise.all(cfg.storage.map((n) => start(n.id)));
  await start("router");
  await start("agent");
}

// Reverse of start order, so the agent and router drain into nodes that are still up.
export async function stopAll() {
  const cfg = loadConfig();
  await stop("loggen");
  await stop("agent");
  await stop("router");
  await Promise.all(cfg.storage.map((n) => stop(n.id)));
}

function tailProgress(cfg: ClusterConfig): TailProgress[] {
  let checkpoints: Record<string, number> = {};
  try {
    checkpoints = JSON.parse(fs.readFileSync(path.join(abs(cfg.agent.dir), "checkpoints.json"), "utf8"));
  } catch {}
  return cfg.agent.tail.map((p) => {
    let size: number | null = null;
    try {
      size = fs.statSync(abs(p)).size;
    } catch {}
    return { path: p, read: checkpoints[abs(p)] ?? 0, size };
  });
}

export function state(): ClusterState {
  const cfg = loadConfig();
  const ids = [...cfg.storage.map((n) => n.id), "router", "agent", "loggen", "build"];
  const out: Record<string, ProcStatus> = {};
  for (const id of ids) {
    const p = procs.get(id);
    let stale = false;
    if (p?.child && id !== "build") {
      stale = spec(id, cfg).args.join("\0") !== p.args.join("\0");
    }
    out[id] = {
      running: !!p?.child,
      pid: p?.pid,
      startedAt: p?.startedAt,
      uptimeSec: p?.child && p.startedAt ? Math.floor((Date.now() - p.startedAt) / 1000) : undefined,
      exitCode: p?.exitCode,
      output: p?.output ?? [],
      stale,
    };
  }
  return {
    config: cfg,
    procs: out,
    binary: { path: BIN, exists: fs.existsSync(/*turbopackIgnore: true*/ BIN) },
    tail: tailProgress(cfg),
    sampleLog: path.relative(REPO, SAMPLE_LOG),
  };
}

// These routes start processes and choose which files get read, so they answer only
// to a browser on this machine: the Host check stops DNS rebinding, and requiring JSON
// plus a matching Origin stops other sites from posting to them.
export function guard(req: Request, mutating: boolean): Response | null {
  const host = req.headers.get("host") ?? "";
  if (!/^(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$/.test(host)) {
    return Response.json({ error: "only available from this machine" }, { status: 403 });
  }
  if (mutating) {
    if (!(req.headers.get("content-type") ?? "").startsWith("application/json")) {
      return Response.json({ error: "expected application/json" }, { status: 415 });
    }
    const origin = req.headers.get("origin");
    if (origin && new URL(origin).host !== host) {
      return Response.json({ error: "cross-origin request refused" }, { status: 403 });
    }
  }
  return null;
}
