import type { Field } from "../api/query/route";

// The search syntax is storage.ParseFilters on the router; this is the little the UI needs to know.

export function tokens(s: string): Set<string> {
  return new Set(s.split(/[^\p{L}\p{N}]+/u).filter(Boolean));
}

// Words to highlight in each message: the plain terms on the default column. A term with "-" or
// "field:" in front says nothing about which words the message contains.
export function highlightWords(q: string): Set<string> {
  const words = new Set<string>();
  for (const term of q.match(/-?(?:[^\s":]+:=?)?(?:"[^"]*"|\S+)/g) ?? []) {
    if (term.startsWith("-") || /^[^\s":]+:/.test(term)) continue;
    tokens(term.replace(/\*$/, "")).forEach((w) => words.add(w));
  }
  return words;
}

// The term for "this field is exactly this value". The syntax can't escape a quote, so a value
// containing one is searched as a phrase of its words instead.
export function fieldTerm(name: string, value: string): string {
  return value.includes('"') ? `${name}:"${[...tokens(value)].join(" ")}"` : `${name}:="${value}"`;
}

const escapeRegExp = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// Puts a term, or its negation, into the query, replacing either form if it's already there, so
// clicking "exclude" after "filter" flips the term instead of adding a contradiction.
export function withTerm(q: string, term: string, exclude: boolean): string {
  const rest = q.replace(new RegExp(`(^|\\s)-?${escapeRegExp(term)}(?=\\s|$)`, "g"), " ").replace(/\s+/g, " ").trim();
  return `${rest} ${exclude ? "-" : ""}${term}`.trim();
}

export type Level = "error" | "warn" | "info" | "debug";

// A row's level: a level or severity field if it has one, else an upper-case level word near the
// start of the message, which is where tailed log lines usually carry it.
export function levelOf(fields: Field[]): { level: Level; fromField: string | null } | null {
  for (const f of fields) {
    if (!/^(level|severity|lvl)$/i.test(f.Name)) continue;
    const level = classify(f.Value);
    if (level) return { level, fromField: f.Name };
  }
  const msg = fields.find((f) => f.Name === "_msg")?.Value.slice(0, 80) ?? "";
  const word = /\b(FATAL|PANIC|CRITICAL|ERROR|ERR|WARNING|WARN|INFO|DEBUG|TRACE)\b/.exec(msg)?.[1];
  const level = word ? classify(word) : null;
  return level ? { level, fromField: null } : null;
}

function classify(value: string): Level | null {
  const v = value.toLowerCase();
  if (/^(fatal|panic|crit|critical|emerg|alert|err|error)/.test(v)) return "error";
  if (v.startsWith("warn")) return "warn";
  if (v === "info" || v === "notice") return "info";
  if (v === "debug" || v === "trace") return "debug";
  return null;
}
