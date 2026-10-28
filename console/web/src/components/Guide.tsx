import { useState } from "react";
import type { Live } from "../api";
import { TERMS } from "../analogy";
import { useLens } from "../lens";
import { useOperation } from "../operations";
import type { Point } from "../types";

type Step = {
  store: string;
  system: string;
  action?: { kind: string; params?: Record<string, unknown>; label: string };
  watch?: string;
  done?: (point: Point | undefined) => boolean;
};

type Demo = {
  id: string;
  title: string;
  store: string;
  system: string;
  steps: Step[];
};

const idle = (point: Point | undefined) =>
  !point?.sale || ["finished", "failed"].includes(point.sale.state);
const backlog = (size: number) => (point: Point | undefined) =>
  (point?.lag_total ?? 0) > size;
const members = (count: number) => (point: Point | undefined) =>
  point?.members === count;

export const DEMOS: Demo[] = [
  {
    id: "no-scaling",
    title: "No scaling",
    store: "One cashier faces a flash-sale rush.",
    system: "A fixed single consumer under a burst three times its capacity.",
    steps: [
      {
        store: "Keep one cashier on duty.",
        system: "Set the fixed mode (one replica).",
        action: {
          kind: "scaling.mode",
          params: { mode: "fixed" },
          label: "Fixed: one cashier",
        },
      },
      {
        store: "Open the doors for the measured flash sale.",
        system: "Start the measured flash-sale preset.",
        action: {
          kind: "sale.start",
          params: { preset: "flash-sale" },
          label: "Start the flash sale",
        },
        watch:
          "Arrivals jump to about 390/s while serving stays near 140/s; every lane's queue grows.",
        done: backlog(2000),
      },
      {
        store: "Watch the wait time climb, then fall after the rush.",
        system:
          "Live p95 rises with the backlog; it drains only after the burst ends.",
        watch:
          "Nothing crashes: insufficient capacity shows up as queueing, not failure.",
        done: idle,
      },
    ],
  },
  {
    id: "manual-rescue",
    title: "Manual rescue",
    store: "You call two more cashiers in when queues build.",
    system:
      "Switch to manual three replicas under backlog and watch the drain rate.",
    steps: [
      {
        store: "Start with one cashier.",
        system: "Fixed mode.",
        action: {
          kind: "scaling.mode",
          params: { mode: "fixed" },
          label: "Fixed: one cashier",
        },
      },
      {
        store: "Open the doors for a longer rush.",
        system: "Start the extended flash sale.",
        action: {
          kind: "sale.start",
          params: { preset: "flash-sale-extended" },
          label: "Start the extended sale",
        },
        done: backlog(3000),
      },
      {
        store: "Call in three cashiers: one per lane.",
        system:
          "Manual mode at three replicas: the group rebalances to one partition each.",
        action: {
          kind: "scaling.mode",
          params: { mode: "manual", replicas: 3 },
          label: "Manual: three cashiers",
        },
        watch:
          "A brief handover, then serving roughly triples and queues shrink.",
        done: members(3),
      },
      {
        store: "Close the doors and let the queues clear.",
        system: "End the sale and drain.",
        action: { kind: "sale.stop", label: "End the sale" },
        done: idle,
      },
    ],
  },
  {
    id: "cpu-vs-lag",
    title: "CPU versus lag",
    store:
      "Two floor managers: one watches how busy cashiers look, one watches queue length.",
    system: "Run the same sale under CPU-HPA, then lag-KEDA, as fresh runs.",
    steps: [
      {
        store: "Hire the “cashiers look busy” manager.",
        system: "CPU-HPA mode.",
        action: {
          kind: "scaling.mode",
          params: { mode: "cpu-hpa" },
          label: "CPU-HPA",
        },
      },
      {
        store: "Run the flash sale.",
        system: "Start the measured flash sale.",
        action: {
          kind: "sale.start",
          params: { preset: "flash-sale" },
          label: "Start the flash sale",
        },
        watch:
          "Compare the CPU chart with its HPA target: cashiers waiting on the ledger may not look busy.",
        done: idle,
      },
      {
        store: "Hire the “queues are long” manager.",
        system: "Lag-KEDA mode.",
        action: {
          kind: "scaling.mode",
          params: { mode: "lag-keda" },
          label: "Lag-KEDA",
        },
      },
      {
        store: "Run the same flash sale again.",
        system: "Start the same preset (fresh run and group).",
        action: {
          kind: "sale.start",
          params: { preset: "flash-sale" },
          label: "Start the flash sale",
        },
        watch:
          "KEDA reacts to lag per cashier. Compare both sessions in Sessions → Comparisons.",
        done: idle,
      },
    ],
  },
  {
    id: "forecast-vs-lag",
    title: "Forecast versus lag",
    store:
      "A manager who learns the rhythm of repeated rushes, against one who waits for queues.",
    system:
      "Run the repeated-sales preset under lag-KEDA, then ML-predictive, as fresh runs.",
    steps: [
      {
        store: "Hire the “queues are long” manager.",
        system: "Lag-KEDA mode.",
        action: {
          kind: "scaling.mode",
          params: { mode: "lag-keda" },
          label: "Lag-KEDA",
        },
      },
      {
        store: "Run several rushes about half a minute apart.",
        system: "Start the repeated-sales preset.",
        action: {
          kind: "sale.start",
          params: { preset: "repeated-sales" },
          label: "Start repeated sales",
        },
        watch:
          "Each rush builds a queue before KEDA's HPA adds cashiers, and they stay for a minute after.",
        done: idle,
      },
      {
        store: "Hire the manager who expects the rush.",
        system: "ML-predictive mode (forecast plus a one-second planner).",
        action: {
          kind: "scaling.mode",
          params: { mode: "ml-predictive" },
          label: "ML-predictive",
        },
      },
      {
        store: "Run the same repeated sales again.",
        system: "Start the same preset (fresh run and group).",
        action: {
          kind: "sale.start",
          params: { preset: "repeated-sales" },
          label: "Start repeated sales",
        },
        watch:
          "Open Scaling: the decision panel shows the forecast and predicted waits behind each change. Compare both sessions in Sessions.",
        done: idle,
      },
    ],
  },
  {
    id: "db-bottleneck",
    title: "Database bottleneck",
    store: "The back office slows down; more cashiers cannot help.",
    system:
      "Restrict PostgreSQL CPU, then add consumers and see the drain stay flat.",
    steps: [
      {
        store: "Three cashiers on duty.",
        system: "Manual three replicas.",
        action: {
          kind: "scaling.mode",
          params: { mode: "manual", replicas: 3 },
          label: "Manual: three",
        },
      },
      {
        store: "Slow the back office.",
        system: "Restrict the database's CPU to 50m for two minutes.",
        action: {
          kind: "fault.inject",
          params: {
            kind: "restrict_db_cpu",
            params: { duration_s: 120, limit_m: 50 },
          },
          label: "Restrict DB CPU",
        },
      },
      {
        store: "Open the doors.",
        system: "Start the flash sale.",
        action: {
          kind: "sale.start",
          params: { preset: "flash-sale" },
          label: "Start the flash sale",
        },
        watch:
          "DB p95 rises and the database-slow signal appears; three consumers drain little faster than one would.",
        done: idle,
      },
    ],
  },
  {
    id: "crash-replay",
    title: "Crash and replay",
    store:
      "A cashier faints after writing the ledger but before advancing “now serving”.",
    system:
      "Crash after a DB commit, before the offset commit; observe replay and deduplication.",
    steps: [
      {
        store: "One cashier on duty.",
        system: "Fixed mode.",
        action: {
          kind: "scaling.mode",
          params: { mode: "fixed" },
          label: "Fixed",
        },
      },
      {
        store: "Open the doors.",
        system: "Start the steady preset.",
        action: {
          kind: "sale.start",
          params: { preset: "steady" },
          label: "Start a steady sale",
        },
      },
      {
        store: "Make the cashier faint after the next ledger entry.",
        system: "Inject crash-after-DB-commit (exit 75).",
        action: {
          kind: "fault.inject",
          params: { kind: "crash_after_db_commit", params: {} },
          label: "Inject the crash",
        },
        watch:
          "The pod collapses with exit 75, restarts, replays the record, and the duplicate check stores it once. See Failure lab → Recent outcomes.",
      },
      {
        store: "Close the doors; reconciliation runs after the drain.",
        system: "End the sale and drain.",
        action: { kind: "sale.stop", label: "End the sale" },
        done: idle,
      },
    ],
  },
];

export function Guide({ live }: { live: Live }) {
  const lens = useLens();
  const store = lens === "store";
  const [demoId, setDemoId] = useState(DEMOS[0].id);
  const [step, setStep] = useState(0);
  const operation = useOperation(live);
  const demo = DEMOS.find((item) => item.id === demoId) ?? DEMOS[0];
  const point = live.points.at(-1);
  const current = demo.steps[Math.min(step, demo.steps.length - 1)];
  const reached = current.done ? current.done(point) : false;
  return (
    <div className="guide" data-testid="guide">
      <h3>Guided demonstrations</h3>
      <div
        className="preset-list"
        role="radiogroup"
        aria-label="Demonstrations"
      >
        {DEMOS.map((item) => (
          <button
            key={item.id}
            role="radio"
            aria-checked={item.id === demoId}
            className={`preset ${item.id === demoId ? "active" : ""}`}
            onClick={() => {
              setDemoId(item.id);
              setStep(0);
            }}
          >
            <strong>{item.title}</strong>
            <span>{store ? item.store : item.system}</span>
          </button>
        ))}
      </div>
      <ol className="guide-steps">
        {demo.steps.map((item, index) => (
          <li
            key={index}
            className={index === step ? "current" : index < step ? "done" : ""}
          >
            <p>{store ? item.store : item.system}</p>
            {index === step && (
              <>
                {item.watch && <p className="hint">Watch: {item.watch}</p>}
                <div className="button-row">
                  {item.action && (
                    <button
                      className="primary"
                      disabled={operation.busy(item.action.kind)}
                      onClick={() =>
                        operation.run(
                          item.action!.kind,
                          item.action!.params ?? {},
                        )
                      }
                      data-testid="guide-action"
                    >
                      {item.action.label}
                    </button>
                  )}
                  <button
                    onClick={() =>
                      setStep(Math.min(step + 1, demo.steps.length))
                    }
                    disabled={
                      index === demo.steps.length - 1 && !reached && !!item.done
                    }
                  >
                    {reached ? "Done: next" : "Next"}
                  </button>
                </div>
              </>
            )}
          </li>
        ))}
      </ol>
      {(operation.error ||
        [current.action?.kind]
          .map((kind) => kind && operation.failure(kind))
          .find(Boolean)) && (
        <p className="error-text">
          {operation.error ?? operation.failure(current.action?.kind ?? "")}
        </p>
      )}
      <details className="disclosure">
        <summary>
          Glossary <span>Store and system terms</span>
        </summary>
        <dl className="glossary">
          {Object.values(TERMS).map((term) => (
            <div key={term.system} className="dl-row">
              <dt>{store ? term.store : term.system}</dt>
              <dd>
                {store ? term.system : term.store}
                {"limit" in term && term.limit ? (
                  <span className="muted"> · {term.limit}</span>
                ) : null}
              </dd>
            </div>
          ))}
        </dl>
      </details>
    </div>
  );
}
