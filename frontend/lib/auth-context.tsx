"use client";

import { createContext, useContext, useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { api, clearTokens, getTokens, setTokens } from "./api";
import type { Me, TokenPair } from "./types";

interface AuthContextValue {
  me: Me | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  hasPermission: (perm: string) => boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [me, setMe] = useState<Me | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  const loadMe = useCallback(async () => {
    const { access } = getTokens();
    if (!access) {
      setMe(null);
      setLoading(false);
      return;
    }
    try {
      const data = await api.get<Me>("/api/v1/auth/me");
      setMe(data);
    } catch {
      setMe(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadMe();
  }, [loadMe]);

  const login = useCallback(
    async (email: string, password: string) => {
      const pair = await api.post<TokenPair>("/api/v1/auth/login", { email, password });
      setTokens(pair);
      await loadMe();
      router.push("/");
    },
    [loadMe, router]
  );

  const logout = useCallback(async () => {
    const { refresh } = getTokens();
    try {
      if (refresh) await api.post("/api/v1/auth/logout", { refresh_token: refresh });
    } catch {
      /* logout is best-effort client-side regardless of server response */
    }
    clearTokens();
    setMe(null);
    router.push("/login");
  }, [router]);

  const hasPermission = useCallback((perm: string) => me?.permissions?.includes(perm) ?? false, [me]);

  return (
    <AuthContext.Provider value={{ me, loading, login, logout, hasPermission }}>{children}</AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
