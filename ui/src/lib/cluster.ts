import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import type { ClusterConfig, ClusterState, ProcStatus, TailProgress } from "./types";

export const REPO = process.env.SMOLOGDB_REPO ?? path.resolve(process.cwd(), "..");
const BIN = process.env.SMOLOGDB_BIN ?? path.join(REPO, "smologdb");
const DATA = path.join(REPO, "cluster-data");
const CONFIG_PATH = path.join(DATA, "cluster.json");
const SAMPLE_LOG = path.join(DATA, "sample.log");
// Each process writes its output here, and running ones are listed in PIDS_PATH, so they
// outlive this server and are picked up again when it restarts.
const LOGS = path.join(DATA, "logs");
const PIDS_PATH = path.join(DATA, "procs.json");
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
  // Started by an earlier run of this server and still alive: there's no child handle, only the pid.
  adopted?: boolean;
  pid?: number;
  startedAt?: number;
  exitCode?: number | null;
  output: string[]; // only for `go build`; the others read their log file
  log?: string;
  cmd: string;
  args: string[];
};

type SavedProc = { pid: number; startedAt: number; cmd: string; args: string[]; log?: string };

function alive(pid?: number): boolean {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

function running(p?: Proc): boolean {
  return !!p && (!!p.child || (!!p.adopted && alive(p.pid)));
}

function commandLine(pid: number): string {
  try {
    return execFileSync("ps", ["-ww", "-o", "command=", "-p", String(pid)], { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

// Processes an earlier run of this server started and that are still running, because it crashed
// or was killed: they show as running and can be stopped. A pid only counts if its command line
// is still exactly ours, so a pid the system has since given to something else is never touched.
function adopt(): Map<string, Proc> {
  const found = new Map<string, Proc>();
  let saved: Record<string, SavedProc> = {};
  try {
    saved = JSON.parse(fs.readFileSync(PIDS_PATH, "utf8"));
  } catch {
    return found;
  }
  for (const [id, s] of Object.entries(saved)) {
    if (!alive(s.pid) || commandLine(s.pid) !== [s.cmd, ...s.args].join(" ")) continue;
    found.set(id, { child: null, adopted: true, pid: s.pid, startedAt: s.startedAt, exitCode: null, output: [], log: s.log, cmd: s.cmd, args: s.args });
  }
  return found;
}

// Survives module reloads in `next dev`; adopt() covers a restart of the server itself.
const g = globalThis as unknown as { __smologdbProcs?: Map<string, Proc> };
const procs = (g.__smologdbProcs ??= adopt());

function savePids() {
  const out: Record<string, SavedProc> = {};
  for (const [id, p] of procs) {
    if (p.log && p.pid && running(p)) out[id] = { pid: p.pid, startedAt: p.startedAt ?? 0, cmd: p.cmd, args: p.args, log: p.log };
  }
  fs.mkdirSync(DATA, { recursive: true });
  fs.writeFileSync(PIDS_PATH, JSON.stringify(out, null, 2) + "\n");
}

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
    if (running(p) && id.startsWith("storage-") && !cfg.storage.some((n) => n.id === id)) {
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

// Adds a line of our own (like "exited") to what the process wrote.
function note(p: Proc, line: string) {
  if (!p.log) return record(p, line);
  try {
    fs.appendFileSync(p.log, line + "\n");
  } catch {}
}

function lastLines(file: string, n: number): string[] {
  try {
    const fd = fs.openSync(file, "r");
    try {
      const size = fs.fstatSync(fd).size;
      const buf = Buffer.alloc(Math.min(size, 64 * 1024));
      fs.readSync(fd, buf, 0, buf.length, size - buf.length);
      return buf.toString("utf8").split("\n").filter((l) => l.trim()).slice(-n);
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return [];
  }
}

// toFile: the process writes to cluster-data/logs/<id>.log rather than a pipe to this server.
// A pipe dies with the server, and a Go program exits on SIGPIPE the next time it logs, so a
// node started that way would be killed by a UI restart.
function run(id: string, cmd: string, args: string[], toFile = true): Proc {
  let log: string | undefined;
  let stdio: ["ignore", "pipe" | number, "pipe" | number] = ["ignore", "pipe", "pipe"];
  if (toFile) {
    fs.mkdirSync(LOGS, { recursive: true });
    log = path.join(LOGS, `${id}.log`);
    const fd = fs.openSync(log, "w");
    stdio = ["ignore", fd, fd];
  }
  const child = spawn(cmd, args, { cwd: REPO, stdio });
  if (typeof stdio[1] === "number") fs.closeSync(stdio[1]); // the child has its own copy
  const p: Proc = { child, pid: child.pid, startedAt: Date.now(), exitCode: null, output: [], log, cmd, args };
  procs.set(id, p);
  child.stdout?.on("data", (c) => record(p, c));
  child.stderr?.on("data", (c) => record(p, c));
  child.on("error", (err) => {
    note(p, `failed to start: ${err.message}`);
    p.child = null;
    p.exitCode = -1;
    savePids();
  });
  child.on("exit", (code, signal) => {
    p.child = null;
    p.exitCode = code ?? -1;
    note(p, `exited (${signal ?? code})`);
    savePids();
  });
  if (log) savePids();
  return p;
}

function exited(p: Proc): Promise<void> {
  return new Promise((resolve) => {
    if (p.child) return void p.child.once("exit", () => resolve());
    // An adopted process isn't our child, so there's no exit event: watch the pid instead.
    const timer = setInterval(() => {
      if (!alive(p.pid)) {
        clearInterval(timer);
        resolve();
      }
    }, 100);
  });
}

export async function build() {
  const p = run("build", "go", ["build", "-o", BIN, "./cmd/smologdb"], false);
  await exited(p);
  if (p.exitCode !== 0) throw new Error(`go build failed: ${p.output.slice(-3).join(" ")}`);
}

export async function start(id: string) {
  if (running(procs.get(id))) return;
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
  const pid = p?.pid;
  if (!p || !pid || !running(p)) return;
  const kill = (signal: NodeJS.Signals) => {
    try {
      process.kill(pid, signal);
    } catch {}
  };
  kill("SIGTERM");
  // smologdb flushes on SIGTERM; only force it if that hangs.
  const timer = setTimeout(() => kill("SIGKILL"), 10_000);
  await exited(p);
  clearTimeout(timer);
  if (p.adopted) {
    p.adopted = false;
    note(p, "stopped");
    savePids();
  }
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
    const up = running(p);
    if (p?.adopted && !up) {
      // An adopted process ended on its own; we can't know its exit code.
      p.adopted = false;
      savePids();
    }
    let stale = false;
    if (p && up && id !== "build") {
      stale = spec(id, cfg).args.join("\0") !== p.args.join("\0");
    }
    out[id] = {
      running: up,
      adopted: up && !!p?.adopted,
      pid: p?.pid,
      startedAt: p?.startedAt,
      uptimeSec: up && p?.startedAt ? Math.floor((Date.now() - p.startedAt) / 1000) : undefined,
      exitCode: p?.exitCode,
      output: p?.log ? lastLines(p.log, OUTPUT_LINES) : (p?.output ?? []),
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
