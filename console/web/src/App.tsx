import { useEffect, useMemo, useRef, useState } from "react";
import { useConsole } from "./api";
import type { Lens } from "./analogy";
import { buildFloor } from "./graph";
import { count, rate, seconds } from "./format";
import { LensContext, LensToggle } from "./lens";
import {
  ConnectionBadge,
  SourceStrip,
  WorkflowChip,
} from "./components/Status";
import { Timeline } from "./components/Timeline";
import { Pipeline } from "./components/Pipeline";
import { Charts } from "./components/Charts";
import { Inspector } from "./components/Inspector";
import { SaleBar } from "./components/SaleBar";
import { Signals } from "./components/Signals";
import { SalePanel } from "./components/SalePanel";
import { ScalingPanel } from "./components/ScalingPanel";
import { FailureLab, type FaultsState } from "./components/FailureLab";
import { Guide } from "./components/Guide";
import { SessionsView } from "./components/Sessions";
import { useOperation } from "./operations";

function stored<T extends string>(
  key: string,
  fallback: T,
  allowed: readonly T[],
): T {
  try {
    const value = localStorage.getItem(key) as T | null;
    return value && allowed.includes(value) ? value : fallback;
  } catch {
    return fallback;
  }
}
function remember(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* Preferences are optional. */
  }
}
const PAGES = {
  overview: {
    title: "Pipeline overview",
    description: "Follow a sale from arrival to a recorded order.",
    icon: "◈",
    label: "Overview",
  },
  sale: {
    title: "Run a sale",
    description: "Choose the traffic. Watch how your system handles it.",
    icon: "▷",
    label: "Sale",
  },
  scaling: {
    title: "Manage capacity",
    description: "Add consumers yourself, or let the autoscaler respond.",
    icon: "▥",
    label: "Scaling",
  },
  faults: {
    title: "Failure lab",
    description: "Introduce a fault, observe the impact, then recover.",
    icon: "⚡",
    label: "Failure lab",
  },
  sessions: {
    title: "Sessions & results",
    description:
      "Replay a run, check its records, and compare scaling strategies.",
    icon: "◷",
    label: "Sessions",
  },
  guide: {
    title: "Learn the system",
    description: "Walk through a sale and understand what each component does.",
    icon: "?",
    label: "Guide",
  },
} as const;
type Page = keyof typeof PAGES;

export default function App() {
  const live = useConsole();
  const [lens, setLens] = useState<Lens>(() =>
    stored("streamscale.lens", "store", ["store", "system"] as const),
  );
  const [motion, setMotion] = useState<"on" | "off">(() =>
    stored(
      "streamscale.motion",
      matchMedia("(prefers-reduced-motion: reduce)").matches ? "off" : "on",
      ["on", "off"] as const,
    ),
  );
  const [page, setPage] = useState<Page>("overview");
  const [presenting, setPresenting] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [inspecting, setInspecting] = useState(false);
  const drawer = useRef<HTMLDialogElement>(null);
  const heading = useRef<HTMLHeadingElement>(null);
  const navigate = (next: Page) => {
    setPage(next);
    window.scrollTo({ top: 0 });
  };
  const select = (id: string | null) => {
    setSelected(id);
    if (id) setInspecting(true);
  };

  useEffect(() => remember("streamscale.lens", lens), [lens]);
  useEffect(() => remember("streamscale.motion", motion), [motion]);
  useEffect(() => {
    heading.current?.focus({ preventScroll: true });
  }, [page]);
  useEffect(() => {
    if (inspecting) drawer.current?.showModal();
    else drawer.current?.close();
  }, [inspecting]);
  useEffect(() => {
    document.body.classList.toggle("presenting", presenting);
    if (
      presenting &&
      document.fullscreenEnabled &&
      !document.fullscreenElement
    ) {
      document.documentElement.requestFullscreen().catch(() => undefined);
    } else if (!presenting && document.fullscreenElement)
      document.exitFullscreen().catch(() => undefined);
    const changed = () => {
      if (!document.fullscreenElement) setPresenting(false);
    };
    document.addEventListener("fullscreenchange", changed);
    return () => document.removeEventListener("fullscreenchange", changed);
  }, [presenting]);
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      if (
        event.ctrlKey ||
        event.metaKey ||
        event.altKey ||
        target.isContentEditable ||
        ["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName) ||
        drawer.current?.open
      )
        return;
      if (event.key === "p") setPresenting((value) => !value);
      else if (event.key === "l")
        setLens((value) => (value === "store" ? "system" : "store"));
      else if (event.key === "m")
        setMotion((value) => (value === "on" ? "off" : "on"));
      else if (event.key === "Escape") setPresenting(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const floor = useMemo(
    () => buildFloor(live.points, live.sample, live.now),
    [live.points, live.sample, live.now],
  );
  const cpuTarget =
    floor.manager.controllers.find(
      (row) => row.kind === "HorizontalPodAutoscaler" && !row.owned_by_keda,
    )?.cpu_target ?? null;
  const recovery = useOperation(live);
  const recoveryError = recovery.error ?? recovery.failure("fault.recover_all");
  const activeFaults = (
    (live.sample?.faults as FaultsState | undefined)?.active ?? []
  ).length;
  const point = live.points.at(-1);
  const mode =
    (
      {
        fixed: "Fixed · 1 consumer",
        manual: "Manual scaling",
        "cpu-hpa": "CPU autoscaling",
        "lag-keda": "Lag autoscaling",
        "ml-reactive": "ML-reactive autoscaling",
        "ml-predictive": "ML-predictive autoscaling",
      } as Record<string, string>
    )[floor.manager.mode] ?? "Waiting for scaling state";

  return (
    <LensContext.Provider value={lens}>
      <a className="skip-link" href="#main">
        Skip to main content
      </a>
      <div className="console-shell">
        <aside className="navigation">
          <a
            className="console-brand"
            href="#overview"
            onClick={(event) => {
              event.preventDefault();
              navigate("overview");
            }}
          >
            <img src="/favicon.svg" alt="" width={30} height={30} />
            <span>
              StreamScale<small>PIPELINE LAB</small>
            </span>
          </a>
          <nav aria-label="Main navigation">
            {(Object.entries(PAGES) as [Page, (typeof PAGES)[Page]][]).map(
              ([id, item]) => (
                <button
                  key={id}
                  aria-current={page === id ? "page" : undefined}
                  onClick={() => navigate(id)}
                  data-testid={
                    id === "overview"
                      ? "view-live"
                      : id === "sessions"
                        ? "view-sessions"
                        : `tab-${id}`
                  }
                >
                  <span className="nav-icon" aria-hidden="true">
                    {item.icon}
                  </span>
                  {item.label}
                  {id === "faults" && activeFaults > 0 && (
                    <span className="nav-count">{activeFaults}</span>
                  )}
                </button>
              ),
            )}
          </nav>
          <div className="nav-foot">
            <span className="eyebrow">A live experiment</span>
            <p>Traffic → queue → consumers → database</p>
            <button className="link-button" onClick={() => navigate("guide")}>
              How it works ↗
            </button>
          </div>
        </aside>
        <main className="console-main" id="main" tabIndex={-1}>
          <header className="page-header">
            <div>
              <span className="eyebrow">
                STREAMSCALE / {PAGES[page].label.toUpperCase()}
              </span>
              <h1 ref={heading} tabIndex={-1}>
                {PAGES[page].title}
              </h1>
              <p>{PAGES[page].description}</p>
            </div>
            <div className="header-tools">
              <ConnectionBadge live={live} />
              <details className="view-options">
                <summary>View options</summary>
                <div className="options-content">
                  <strong>Choose your vocabulary</strong>
                  <LensToggle lens={lens} onChange={setLens} />
                  <p className="hint">
                    Store explains the analogy. System uses Kafka and Kubernetes
                    terms.
                  </p>
                  <button
                    aria-pressed={motion === "off"}
                    onClick={() => setMotion(motion === "on" ? "off" : "on")}
                  >
                    Motion {motion}
                  </button>
                  <button
                    aria-pressed={presenting}
                    onClick={() => setPresenting((value) => !value)}
                  >
                    {presenting ? "Leave presentation" : "Present"}
                  </button>
                  <p className="hint">Keys: L lens · M motion · P present</p>
                </div>
              </details>
            </div>
          </header>
          {live.workflow?.holder && <WorkflowChip live={live} />}
          {activeFaults > 0 && (
            <div className="recovery-banner" role="status">
              <span>
                <strong>
                  {activeFaults} active{" "}
                  {activeFaults === 1 ? "fault" : "faults"}
                </strong>{" "}
                · Automatic recovery is scheduled.
              </span>
              <button
                className="danger"
                disabled={recovery.busy("fault.recover_all")}
                onClick={() => recovery.run("fault.recover_all")}
                data-testid="topbar-recover"
              >
                Recover pipeline
              </button>
            </div>
          )}
          {recoveryError && (
            <p className="error-text" role="alert">
              {recoveryError}
            </p>
          )}
          {page !== "sessions" && (
            <>
              <section
                className="live-summary"
                aria-label="Live pipeline summary"
              >
                <div>
                  <span>Arriving / processed</span>
                  <strong>
                    {rate(point?.rates.arrival)}{" "}
                    <small>/ {rate(point?.rates.drain)}</small>
                  </strong>
                  <small>Events per second</small>
                </div>
                <div>
                  <span>Backlog</span>
                  <strong data-testid="summary-backlog">
                    {count(point?.lag_total)}
                  </strong>
                  <small>Events waiting to be processed</small>
                </div>
                <div>
                  <span>Processing latency</span>
                  <strong>{seconds(point?.e2e_p95)}</strong>
                  <small>p95 · live estimate</small>
                </div>
                <button onClick={() => navigate("scaling")}>
                  <span>
                    Consumers ready <span aria-hidden="true">↗</span>
                  </span>
                  <strong data-testid="summary-ready">
                    {count(point?.ready)}{" "}
                    <small>/ {count(point?.desired)} desired</small>
                  </strong>
                  <small>
                    {mode}
                    {floor.manager.override ? " · override" : ""}
                  </small>
                </button>
              </section>
              <Signals point={point} />
            </>
          )}
          {page !== "sessions" && page !== "sale" && (
            <SaleBar
              live={live}
              onPlan={() => navigate("sale")}
              activeOnly={page !== "overview"}
            />
          )}
          {page === "overview" && (
            <>
              <Pipeline
                floor={floor}
                selected={selected}
                onSelect={select}
                reducedMotion={motion === "off"}
                cpuTarget={cpuTarget}
                onInspect={() => {
                  setSelected(null);
                  setInspecting(true);
                }}
              />
              <Charts
                points={live.points}
                journal={live.journal}
                cpuTarget={cpuTarget}
                compact
              />
              <details className="disclosure timeline-section">
                <summary>
                  Activity timeline <span>Actions, changes & recovery</span>
                </summary>
                <Timeline journal={live.journal} operations={live.operations} />
              </details>
            </>
          )}
          {/* Keep editors mounted so navigating to the flow never discards a draft. */}
          <section hidden={page !== "sale"} className="task-page">
            <SalePanel live={live} />
          </section>
          <section hidden={page !== "scaling"} className="task-page">
            <ScalingPanel live={live} />
          </section>
          <section hidden={page !== "faults"} className="task-page">
            <FailureLab live={live} />
          </section>
          <section hidden={page !== "guide"} className="task-page">
            <Guide live={live} />
          </section>
          {page === "sessions" && <SessionsView live={live} />}
          <details className="disclosure diagnostics">
            <summary>
              Connection & diagnostics{" "}
              <span>Sources, workflow & dashboards</span>
            </summary>
            <WorkflowChip live={live} />
            <SourceStrip live={live} />
            <div className="button-row">
              {Object.entries(live.state?.server.dashboards ?? {}).map(
                ([name, url]) =>
                  url && (
                    <a
                      className="link-button"
                      key={name}
                      href={url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {name === "grafana" ? "Grafana" : "Prometheus"} ↗
                    </a>
                  ),
              )}
            </div>
          </details>
        </main>
        <dialog
          ref={drawer}
          className="inspection-drawer"
          aria-label="Component inspector"
          onCancel={() => setInspecting(false)}
          onClose={() => setInspecting(false)}
          onClick={(event) => {
            if (event.target === drawer.current) setInspecting(false);
          }}
        >
          {inspecting && (
            <div className="drawer-content">
              <label className="inspector-picker">
                Inspect component
                <select
                  aria-label="Inspect component"
                  value={selected ?? ""}
                  onChange={(event) => setSelected(event.target.value || null)}
                >
                  <option value="">Find an order or event</option>
                  <option value="doors">Publisher</option>
                  {floor.lanes.map((lane) => (
                    <option
                      key={lane.partition}
                      value={`lane${lane.partition}`}
                    >
                      Partition {lane.partition}
                    </option>
                  ))}
                  <option value="manager">Scaling controller</option>
                  {floor.cashiers.map((cashier) => (
                    <option
                      key={cashier.name}
                      value={`cashier:${cashier.name}`}
                    >
                      Consumer {cashier.name.split("-").at(-1)}
                    </option>
                  ))}
                  <option value="ledger">PostgreSQL</option>
                  <option value="desk">Dead-letter queue</option>
                </select>
              </label>
              <Inspector
                selected={selected}
                floor={floor}
                sample={live.sample}
                onClose={() => setInspecting(false)}
              />
              {!selected && (
                <button onClick={() => setInspecting(false)}>
                  Close inspector
                </button>
              )}
            </div>
          )}
        </dialog>
      </div>
    </LensContext.Provider>
  );
}
