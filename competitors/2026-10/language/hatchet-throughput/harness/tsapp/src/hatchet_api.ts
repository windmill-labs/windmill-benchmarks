/** Minimal REST helpers against the hatchet-lite API with the tenant API token. */
const SERVER = process.env.HATCHET_CLIENT_SERVER_URL!;
const TOKEN = process.env.HATCHET_CLIENT_TOKEN!;
const TENANT = process.env.HATCHET_TENANT_ID!;

export async function api(method: string, path: string, body?: unknown): Promise<any> {
  const res = await fetch(SERVER + path, {
    method,
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

export async function listWorkers(): Promise<any[]> {
  const r = await api('GET', `/api/v1/tenants/${TENANT}/worker`);
  return (r.rows ?? []).filter((w: any) => w.status === 'ACTIVE');
}

export async function setPaused(ids: string[], flag: boolean): Promise<void> {
  // flip every worker at once so the ramp is not serialised over N HTTP calls
  await Promise.all(ids.map((id) => api('PATCH', `/api/v1/workers/${id}`, { isPaused: flag })));
}
