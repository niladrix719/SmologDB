import { build, guard, saveConfig, start, startAll, state, stop, stopAll } from "@/lib/cluster";

export async function GET(request: Request) {
  return guard(request, false) ?? Response.json(state());
}

export async function PUT(request: Request) {
  const denied = guard(request, true);
  if (denied) return denied;
  try {
    saveConfig(await request.json());
    return Response.json(state());
  } catch (err) {
    return Response.json({ error: (err as Error).message, state: state() }, { status: 400 });
  }
}

export async function POST(request: Request) {
  const denied = guard(request, true);
  if (denied) return denied;
  const { action, id } = await request.json().catch(() => ({}));
  try {
    switch (action) {
      case "start":
        await start(id);
        break;
      case "stop":
        await stop(id);
        break;
      case "restart":
        await stop(id);
        await start(id);
        break;
      case "startAll":
        await startAll();
        break;
      case "stopAll":
        await stopAll();
        break;
      case "build":
        await build();
        break;
      default:
        return Response.json({ error: `unknown action ${action}` }, { status: 400 });
    }
    return Response.json(state());
  } catch (err) {
    return Response.json({ error: (err as Error).message, state: state() }, { status: 400 });
  }
}
