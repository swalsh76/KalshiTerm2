import { getToken } from "./token";

export type ServerMessage = { type: string; [k: string]: unknown };

// One multiplexed WebSocket. Auth is the first message (the token is never in the URL).
export function connect(onMessage: (m: ServerMessage) => void, onState: (open: boolean) => void): () => void {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/api/ws`);
  ws.onopen = () => ws.send(JSON.stringify({ type: "auth", token: getToken() ?? "" }));
  ws.onmessage = (e) => {
    const m = JSON.parse(String(e.data)) as ServerMessage;
    if (m.type === "hello") onState(true);
    onMessage(m);
  };
  ws.onclose = () => onState(false);
  return () => ws.close();
}
