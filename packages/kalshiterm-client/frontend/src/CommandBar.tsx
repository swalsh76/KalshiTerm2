import { useEffect, useRef, useState } from "react";
import { COMMANDS, parseCommand } from "./lib/commands";

export function CommandBar({ run }: { run: (name: string, args: string[]) => string | null }) {
  const [open, setOpen] = useState(false);
  const [line, setLine] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen(true);
      } else if (e.key === "Escape") {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (open) input.current?.focus();
  }, [open]);

  if (!open) return null;
  return (
    <div className="cmd-backdrop" onClick={() => setOpen(false)}>
      <form
        className="cmd"
        onClick={(e) => e.stopPropagation()}
        onSubmit={(e) => {
          e.preventDefault();
          const c = parseCommand(line);
          if (!c) return;
          const out = run(c.name, c.args);
          setMsg(out);
          if (out === null) {
            setLine("");
            setOpen(false);
          }
        }}
      >
        <input
          ref={input}
          value={line}
          onChange={(e) => setLine(e.target.value)}
          placeholder="command (HELP lists them)"
          aria-label="command"
        />
        {msg && <div className="cmd-msg">{msg}</div>}
        <div className="cmd-help">{COMMANDS.map((c) => c.name).join("  ")}</div>
      </form>
    </div>
  );
}
