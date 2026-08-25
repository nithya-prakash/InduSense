import type { ApiErrorBody, TokenPair } from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8080";

const ACCESS_KEY = "indusense_access_token";
const REFRESH_KEY = "indusense_refresh_token";

export function getTokens() {
  if (typeof window === "undefined") return { access: null, refresh: null };
  return {
    access: localStorage.getItem(ACCESS_KEY),
    refresh: localStorage.getItem(REFRESH_KEY),
  };
}

export function setTokens(pair: TokenPair) {
  localStorage.setItem(ACCESS_KEY, pair.access_token);
  localStorage.setItem(REFRESH_KEY, pair.refresh_token);
}

export function clearTokens() {
  localStorage.removeItem(ACCESS_KEY);
  localStorage.removeItem(REFRESH_KEY);
}

export class ApiError extends Error {
  code: string;
  status: number;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// A single in-flight refresh promise, shared across concurrent 401s, so a
// burst of simultaneous requests hitting an expired token doesn't each try
// to rotate the refresh token independently (which would race — only the
// first rotation succeeds, per the API's rotate-on-use design).
let refreshInFlight: Promise<boolean> | null = null;

async function doRefresh(): Promise<boolean> {
  const { refresh } = getTokens();
  if (!refresh) return false;
  try {
    const res = await fetch(`${API_BASE}/api/v1/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refresh }),
    });
    if (!res.ok) {
      clearTokens();
      return false;
    }
    const pair: TokenPair = await res.json();
    setTokens(pair);
    return true;
  } catch {
    clearTokens();
    return false;
  }
}

async function request<T>(path: string, init?: RequestInit, retried = false): Promise<T> {
  const { access } = getTokens();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(init?.headers as Record<string, string> | undefined),
  };
  if (access) headers["Authorization"] = `Bearer ${access}`;

  const res = await fetch(`${API_BASE}${path}`, { ...init, headers });

  if (res.status === 401 && !retried) {
    if (!refreshInFlight) refreshInFlight = doRefresh().finally(() => (refreshInFlight = null));
    const refreshed = await refreshInFlight;
    if (refreshed) return request<T>(path, init, true);
  }

  if (!res.ok) {
    let body: ApiErrorBody | null = null;
    try {
      body = await res.json();
    } catch {
      /* non-JSON error body, e.g. a proxy error page */
    }
    throw new ApiError(res.status, body?.error?.code ?? "UNKNOWN", body?.error?.message ?? res.statusText);
  }

  if (res.status === 204) return undefined as T;
  return res.json();
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined }),
};

export function wsUrl(path: string): string {
  const base = API_BASE.replace(/^http/, "ws");
  const { access } = getTokens();
  return `${base}${path}?token=${encodeURIComponent(access ?? "")}`;
}

export { API_BASE };
