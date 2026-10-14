import { expect, it } from "vitest";
import { parseCommand } from "./commands";

it("parses a mnemonic and its arguments", () => {
  expect(parseCommand("  buy KXA-1  3 0.45 ")).toEqual({ name: "BUY", args: ["KXA-1", "3", "0.45"] });
});
it("returns null for an empty line", () => {
  expect(parseCommand("   ")).toBeNull();
});
