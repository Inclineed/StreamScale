import { describe, expect, it } from "vitest";
import { freshness, type Live } from "./api";
import { MODE_LABELS, TERMS } from "./analogy";
import { age, count, rate, seconds } from "./format";
import type { Point } from "./types";

function live(overrides: Partial<Live>): Live {
  return {
    state: null,
    points: [],
    sample: null,
    operations: [],
    journal: [],
    status: { agent: "connected", since: 0, error: null, last_ok: 0, forward_restarts: 0 },
    workflow: null,
    connection: { socket: "open", attempts: 0, lastMessageAt: 0, skew: 0 },
    now: 100,
    extras: {},
    ...overrides,
  };
}

const point = (received_at: number) => ({ received_at }) as Point;

describe("freshness", () => {
  it("is live only with an open stream, reachable agent and a recent sample", () => {
    expect(freshness(live({ points: [point(99)] })).verdict).toBe("live");
  });

  it("never reports live data when the stream is closed", () => {
    const value = live({ points: [point(99.5)], connection: { socket: "closed", attempts: 2, lastMessageAt: 0, skew: 0 } });
    expect(freshness(value).verdict).toBe("disconnected");
  });

  it("reports an unreachable agent even with recent cached points", () => {
    const value = live({
      points: [point(99.5)],
      status: { agent: "unreachable", since: 0, error: "x", last_ok: 0, forward_restarts: 1 },
    });
    expect(freshness(value).verdict).toBe("agent-unreachable");
  });

  it("turns stale when samples stop arriving", () => {
    expect(freshness(live({ points: [point(90)] })).verdict).toBe("stale");
    expect(freshness(live({ points: [] })).verdict).toBe("waiting");
  });
});

describe("analogy", () => {
  it("maps every store term to a system term with an explanation", () => {
    for (const term of Object.values(TERMS)) {
      expect(term.store.length).toBeGreaterThan(2);
      expect(term.system.length).toBeGreaterThan(2);
      expect(term.explain.length).toBeGreaterThan(20);
    }
    expect(Object.keys(MODE_LABELS).sort()).toEqual([
      "cpu-hpa",
      "fixed",
      "lag-keda",
      "manual",
      "ml-predictive",
      "ml-reactive",
    ]);
    for (const mode of Object.values(MODE_LABELS)) {
      expect(mode.explain.length).toBeGreaterThan(20);
    }
  });
});

describe("formatting", () => {
  it("shows unknown values as a dash instead of zero", () => {
    expect(count(null)).toBe("—");
    expect(rate(undefined)).toBe("—");
    expect(seconds(null)).toBe("—");
    expect(age(null)).toBe("—");
  });

  it("formats rates and durations for reading at a glance", () => {
    expect(rate(389.6)).toBe("390/s");
    expect(rate(52.04)).toBe("52.0/s");
    expect(seconds(0.042)).toBe("42 ms");
    expect(seconds(75)).toBe("1 min 15 s");
    expect(count(249048)).toBe("249,048");
  });
});
