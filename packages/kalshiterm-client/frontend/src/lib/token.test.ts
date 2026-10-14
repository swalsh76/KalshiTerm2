import { describe, expect, it } from "vitest";
import { getToken, takeToken } from "./token";

describe("takeToken", () => {
  it("reads the fragment token and erases it from the URL", () => {
    let url = "";
    const loc = { hash: "#token=abc_DEF-123", pathname: "/", search: "" } as Location;
    const hist = { replaceState: (_s: unknown, _t: string, u: string) => (url = u) } as unknown as History;
    expect(takeToken(loc, hist)).toBe("abc_DEF-123");
    expect(url).toBe("/");
    expect(getToken()).toBe("abc_DEF-123");
  });
  it("keeps the token it already has when the fragment is gone", () => {
    const loc = { hash: "", pathname: "/", search: "" } as Location;
    expect(takeToken(loc, {} as unknown as History)).toBe("abc_DEF-123");
  });
  it("ignores a malformed token", () => {
    const loc = { hash: "#token=<script>", pathname: "/", search: "" } as Location;
    expect(takeToken(loc, {} as unknown as History)).toBe("abc_DEF-123");
  });
});
