// The session token arrives in the URL fragment (never sent to any server). It is moved into
// memory and the fragment is erased, so it is in no history entry, no bookmark and no storage.
let token: string | null = null;

export function takeToken(loc: Location = window.location, hist: History = window.history): string | null {
  const m = /(?:^#|&)token=([A-Za-z0-9_-]+)/.exec(loc.hash);
  if (m) {
    token = m[1] ?? null;
    hist.replaceState(null, "", loc.pathname + loc.search);
  }
  return token;
}

export function getToken(): string | null {
  return token;
}
