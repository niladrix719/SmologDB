// The search route hands out logs, so it answers only to a browser on this machine: the Host
// check stops DNS rebinding, and requiring JSON plus a matching Origin stops other sites from
// posting to it.
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
