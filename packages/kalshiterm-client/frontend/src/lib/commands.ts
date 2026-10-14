// The command bar's vocabulary. Slice 7.5 ships the parser and the panels that exist so far;
// later slices add their mnemonics (M, CH, BK, OR, POS, BUY, KILL) to this table.
export type Command = { name: string; help: string };

export const COMMANDS: Command[] = [
  { name: "HELP", help: "list commands" },
  { name: "THEME", help: "switch light / dark" },
];

export function parseCommand(line: string): { name: string; args: string[] } | null {
  const parts = line.trim().split(/\s+/).filter(Boolean);
  const name = parts[0]?.toUpperCase();
  if (!name) return null;
  return { name, args: parts.slice(1) };
}
