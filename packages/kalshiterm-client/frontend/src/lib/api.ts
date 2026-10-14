import { getToken } from "./token";

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    detail: string,
  ) {
    super(detail);
  }
}

export async function api<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Authorization: `Bearer ${getToken() ?? ""}` };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const res = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data: unknown = await res.json().catch(() => ({}));
  if (!res.ok) {
    const d = (data ?? {}) as { error?: string; detail?: unknown };
    throw new ApiError(res.status, d.error ?? "error", typeof d.detail === "string" ? d.detail : res.statusText);
  }
  return data as T;
}
