import { useEffect, useState } from "react";
import { DockviewReact, type DockviewReadyEvent } from "dockview-react";
import "dockview-react/dist/styles/dockview.css";
import { CommandBar } from "./CommandBar";
import { Welcome } from "./panels/Welcome";
import { api } from "./lib/api";
import { connect } from "./lib/ws";
import { applyTheme, initialTheme, type Theme } from "./lib/theme";
import { COMMANDS } from "./lib/commands";

type Session = { mode: "read-only" | "trading"; environment: string };

const components = { welcome: Welcome };

export function App() {
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [session, setSession] = useState<Session | null>(null);
  const [live, setLive] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => applyTheme(theme), [theme]);

  useEffect(() => {
    api<Session>("GET", "/api/session").then(setSession, (e: Error) => setError(e.message));
    return connect(
      (m) => {
        if (m.type === "mode" || m.type === "hello") {
          setSession((s) => ({
            mode: m.mode as Session["mode"],
            environment: (m.environment as string | undefined) ?? s?.environment ?? "",
          }));
        }
      },
      setLive,
    );
  }, []);

  const production = session?.environment === "production";
  const trading = session?.mode === "trading";

  const run = (name: string): string | null => {
    if (name === "THEME") {
      setTheme((t) => (t === "dark" ? "light" : "dark"));
      return null;
    }
    if (name === "HELP") return COMMANDS.map((c) => `${c.name}: ${c.help}`).join("  |  ");
    return `unknown command ${name}`;
  };

  const onReady = (e: DockviewReadyEvent) => {
    e.api.addPanel({ id: "welcome", component: "welcome", title: "Welcome" });
  };

  return (
    <div className="shell">
      {production && <div className="banner">PRODUCTION: real money{trading ? " (trading on)" : " (read-only)"}</div>}
      <header className="bar">
        <strong>KalshiTerm</strong>
        <span className={`pill ${trading ? "on" : ""}`}>{session ? session.mode : "..."}</span>
        <span className="pill">{session?.environment ?? ""}</span>
        <span className={`dot ${live ? "live" : ""}`} title={live ? "connected" : "disconnected"} />
        {error && <span className="err">{error}</span>}
        <span className="spacer" />
        <button onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>{theme}</button>
      </header>
      <main className="dock">
        <DockviewReact
          className={theme === "dark" ? "dockview-theme-dark" : "dockview-theme-light"}
          components={components}
          onReady={onReady}
        />
      </main>
      <CommandBar run={run} />
    </div>
  );
}
