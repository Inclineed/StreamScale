# Interactive console

The console is a local browser view and control surface for the real StreamScale
kind deployment. It explains the pipeline through one analogy: **a supermarket
checkout during a flash sale**. See [the execution plan](interactive-console-plan.md)
for the complete scope, the analogy table and where the analogy stops.

## Start it

```sh
./streamscale.sh console
```

The command builds the web assets, deploys the console release into the existing
`kind-streamscale` cluster, and prints a login link such as
`http://127.0.0.1:18080/?token=…`. Open that exact link. It also opens Grafana and
Prometheus forwards and links them under **Connection & diagnostics**. Ctrl+C closes the console server and its
forwards. The cluster, its data and the console's in-cluster agent keep running.

Requirements beyond the main project: Node.js 20.19 or newer with npm. The pipeline
must already be deployed (`./streamscale.sh cluster` or `all`). If port 18080 is
busy, an available port is chosen and printed. Choose one with
`./streamscale.sh console --port 18081`; dashboard ports accept the same
`--grafana-port`/`--prometheus-port` options as `ui`.

## Find your way around

- **Overview**: arrival/processing rate, backlog, p95 and ready consumers, followed
  by the live pipeline and two primary charts. **More metrics** reveals CPU,
  latency, replica, DLQ and restart charts. Activity and diagnostics expand below.
- **Sale**: choose one of four starter workloads and start it. **Customize this
  sale** reveals schedule, seed, fault percentages and preset saving. Saved
  presets use a dropdown. Your draft survives switching pages during this visit.
- **Scaling**: choose a mode, set a manual count or take over from an autoscaler.
  Advanced controller settings and history expand separately.
- **Failure lab**: choose one experiment, set its parameters, then inject it.
  Active faults and recovery stay visible; recent outcomes have a bounded list.
- **Sessions**: replay and export a recorded session, compare modes or inspect
  the measured matrix. **Guide** contains step-by-step demonstrations.

The live summary and active fault recovery remain visible across task pages.
**View options** holds the vocabulary lens, motion and presentation preferences.
**Connection & diagnostics** shows individual source health, the workflow lock
and dashboard links; the connection badge is always visible.

The pipeline supports **Map** and **List** views; List is the default on phones.
Both open the same component inspector, with a component picker and record lookup.
The drawer traps keyboard focus, Escape closes it and focus returns to its opener.
**Find a record** opens the order/event lookup directly. Advanced disclosures use
native keyboard-accessible controls. No external font or icon service is needed.

## Store and System lenses

Under **View options**, the **Store lens** names components with the analogy; the **System lens** uses
Kafka, Kubernetes and SQL names. Both show the same live values, and the other
lens's name is always visible as a caption. Select a component for its
explanation and the point where the analogy stops being accurate.

| Store lens | System lens |
| --- | --- |
| Store doors | Interactive publisher |
| Checkout lanes 0–2 | Partitions 0–2 of `orders.events.v1` |
| Tickets issued / now serving / queue length | High watermark / committed offset / lag |
| Cashiers and the lanes they cover | Console consumer pods and their partition assignment |
| Floor manager | Scaling controller (fixed, manual, CPU-HPA, lag-KEDA, ML-reactive, ML-predictive) |
| Sales ledger | PostgreSQL `order_event` and `order_state` |
| Problem-items desk | Dead-letter topic `orders.events.dlq.v1` |

## The checkout floor (M10)

The centre of the console is a live map of the pipeline:

- **Store doors** (publisher): arrival rate, acknowledged events against the
  sale's plan, delivery failures, and a warning when the broker stops accepting.
- **Three lanes** (partitions): queue length (lag) as a belt filling up, its trend
  over five seconds, tickets issued (high watermark), now serving (committed
  offset) and the cashier that currently owns the lane.
- **Cashiers** (console consumer pods): status in plain words (serving, on standby,
  waiting, retrying, collapsed with its exit code, overwhelmed, leaving), lanes
  covered, and CPU as a share of the pod's *request* (the CPU-HPA's basis), with
  the share of its limit and memory beside it.
- **Floor manager** (scaling controller): policy, and wanted · present · at lanes
  (desired replicas · ready pods · consumer-group members), shown separately.
- **Sales ledger** (PostgreSQL) and **problem-items desk** (DLQ): stored facts and
  rate, the order status board, live DB p95, DLQ copies by reason.

Edges carry the measured rate as a label. Moving dots are a **sampled
illustration** (at most six per edge, scaled logarithmically), never one dot
per event; *Motion off*, or the operating system's reduced-motion setting,
removes them and keeps every number. Above the floor, plain-language signals
name a growing backlog, missing consumers, saturated consumers, a slow database
or an unavailable broker/database, each with the measurement behind it.

Below the floor, synchronized charts share one clock and crosshair. The first two
are visible by default; **More metrics** reveals the rest: arrivals vs
served, queue by lane (lanes keep their map colours), live p95 wait, cashier
counts, CPU against the HPA target, DB write p95, problem items, and collapses
and recoveries. Each chart has a *Table* view, and vertical lines mark console
actions. Select any component for the inspector: lane offsets and ranges, a
pod's restarts, exit reasons and per-process counters, controller conditions,
the latest stored facts, the latest DLQ envelopes, and a lookup by order or
event ID that shows the broker acknowledgement, the ledger history, the latest
state and any DLQ reason.

Live p95 values are estimates from histogram buckets over 15 seconds. The exact
p95 comes from SQL timestamps once a sale has drained.

## Planning and controlling a sale (M11)

The **Sale** page plans a sale before the doors open:

- **Presets** reuse the measured M7 workloads: *Flash sale* (15 s at 52/s, 20 s
  at 390/s, 25 s at 52/s, seed 202), *Steady*, *Overload and recovery*, plus an
  *Extended flash sale* with a one-minute rush for live demonstrations.
- **Schedule and seed**: up to twelve phases of 1–3,600 s at 1–2,000 events/s,
  at most one hour and 400,000 events. The same seed reproduces the same logical
  sale; every start gets a fresh run ID and consumer group.
- **Problem shoppers**: malformed, duplicate and late percentages (0–20%), seeded
  like the measured correctness profile (defaults 1%, 2%, 5%).
- **Estimate**: events, orders, expected DLQ items, duplicates and late events, and
  the queue one, two or three consumers would build at the M7 pilot's measured
  capacity (about 130 events/s each). It is labelled as an estimate.
- **Saved presets** live in `artifacts/console/presets.json`; *Run the last sale
  again* repeats its settings as a fresh run.

While a sale runs, the same page (and the compact sale bar on other live pages) controls it:

| Control | What happens |
| --- | --- |
| Pause | No new records are produced. Delivery confirmations keep arriving and consumers keep draining. |
| Resume | The schedule clock continues from where it paused. |
| Set a rate | A live events/s rate replaces the schedule; *Follow the schedule* returns to it from the current position. |
| Stop & drain | Production ends, every sent record is flushed and acknowledged, and consumers drain the backlog. |
| Abort | Unsent records are discarded; records already sent finish and drain. The partial run is kept and reconciled. |

After the backlog reaches zero the console **reconciles the run against its
Kafka source range**, not against the plan, so stopped, aborted and interrupted
sales are judged on what was actually published: every acknowledged valid event
stored exactly once, nothing stored that is not in the log, latest state equal
to the log's ordering, every invalid source in the DLQ (copies counted
separately) and zero remaining backlog. It also reports the **exact SQL p50/p95/p99**
latency of stored facts. The result appears in **Sale → Reconciliation details** and in
`artifacts/console/sessions/<session>/reconciliation.json`.

A session is *comparable* only if it completed unmodified (no pause, rate change,
scaling change or fault) and reconciled. Interactive sessions never enter the M7
results.

Recovery is part of the design: closing the browser changes nothing; restarting
the console server reattaches to the running or draining sale; if the in-cluster
agent restarts mid-sale, the run is finalized as `interrupted` from its durable
acknowledgement log and still drains and reconciles. Only one console server may
run at a time. Buttons that appear in place of the one just pressed ignore clicks
for 0.7 s, so a double click cannot pause a sale it just started.

## Scaling modes and manual override (M12)

The **Scaling** page chooses how many console consumers run:

| Mode | Replicas | Who decides |
| --- | --- | --- |
| Fixed | 1 | Nobody: the baseline. |
| Manual | 0–3 | You. Zero stops processing; it is not a crash. |
| CPU-HPA | 1–3 | Kubernetes, from CPU use relative to the pod's request (target 60%). |
| Lag-KEDA | 1–3 | KEDA, from consumer-group lag per replica (threshold 100). |
| ML-reactive | 1–3 | The agent's one-second planner at the current arrival rate (M16 ablation). |
| ML-predictive | 1–3 | The same planner with the learned arrival forecast and online estimates (M16). |

In the two ML modes the agent itself sets replicas; no HPA or ScaledObject exists.
The **Latest controller decision** panel shows the arrival rate, the planned peak,
the predicted wait for 1, 2 and 3 consumers against the 3 s target, the learned
capacity, start-up time and forecast bias, and the reason for each change. The
loop pauses during a manual override, a scaling transition or a stopped-consumers
fault. Sessions record the latest decision with each telemetry frame. The **Sale**
page adds *Gradual ramp* and *Repeated sales* presets, which follow the M16
campaign's shapes (the ramp coarsened to the console's 12-phase limit). See
[ML predictive autoscaling](ml-autoscaling.md).

From CPU-HPA, lag-KEDA or an ML mode, **Manual override** removes or pauses the autoscaler first and
then sets replicas directly, so two controllers never compete; *Release* recreates
it. Every change is a verified transition: the page shows its steps, then which
controllers own the console Deployment and whether that matches the mode. A
failed or interrupted transition is shown with a *Reapply* button. Settings
(CPU target, lag threshold, scale-down wait, minimum and maximum, never above three)
apply to the autoscaled modes. Desired replicas, ready pods and group members are
always shown separately, because they differ during scale-up and rebalances.

## Failure lab (M13)

The **Failure lab** page injects real, bounded faults while a sale runs. Choose
an experiment from the dropdown; its card states the exact scope. The in-cluster agent owns every timer and recovery, and
records them on the runs volume, so a closed browser, a stopped console server or
even an agent restart never leaves a fault behind.

| Store lens | What actually happens | Bound | What to watch |
| --- | --- | --- | --- |
| Send one cashier home | Deletes one console consumer pod | One pod | Rebalance; a replacement takes the lanes |
| Cashier faints after writing the ledger | A consumer exits 75 after its PostgreSQL commit, before its offset commit | One-shot, 2 min window | Replay of that record; the event is stored once (verified in SQL) |
| Cashier faints after the problem desk confirms | A consumer exits 76 after the DLQ acknowledgement, before its offset commit | One-shot | A possible second DLQ copy, counted by unique source |
| Cashier overwhelmed | One consumer allocates memory until its 256Mi limit kills it | One pod, at most 2 GiB or 2 min | OOMKilled (exit 137) and restart |
| Everyone on a break | Console consumers set to zero (manual or override), then restored | 5–300 s | Lag grows; no group members; no crash |
| Slow card terminals | Consumers wait before each database write | 5–2,000 ms per record, 5–300 s | Lower drain rate |
| Tired cashiers | Consumer CPU limit lowered **in place** (no restart) | 20–400m, 5–300 s | Saturation at the new limit |
| Slow back office | PostgreSQL CPU limit lowered in place | 20–400m, 5–300 s | DB p95 rises; more consumers do not help |
| Ledger office closed | PostgreSQL StatefulSet scaled to zero, then one (data retained) | 5–120 s | Offsets do not advance; consumers retry |
| Ticket system down | Kafka StatefulSet scaled to zero, then one (records retained) | 5–120 s | The publisher buffers; consumers retry |

Only one infrastructure outage runs at a time. **Recover pipeline** (the active-fault banner and Failure lab) ends every active fault now and makes sure Kafka and PostgreSQL are scaled
back to one. Every outcome is observed, not assumed: crash faults record the
consumer's own claim (pod, event, partition, offset, exit code) and are marked
verified only after the replacement has committed past that offset and SQL holds
the event exactly once; the OOM fault is verified from the pod's termination
reason. Sessions with faults reconcile like any other but never enter comparisons.

The broker and database outages affect every client of the shared Kafka and
PostgreSQL. The measured pipeline's idle consumer may log errors or restart while
Kafka is down; run `./streamscale.sh cluster` (or `all`) for a fresh, strict
verification run afterwards, as after `resume`.

## Sessions, replay and comparison (M14)

Choose **Sessions** in the main navigation:

- **Sessions and replay**: every sale is a session with its configuration,
  action journal and one-second telemetry (including the state needed to redraw
  the floor). Replay has a scrubber, play/pause and 1×/4×/10× speeds. It is
  labelled *not live* and has no pipeline controls; it only reads recorded files.
  Each session exports `points.csv`, `configuration.json`, `journal.jsonl` and
  `reconciliation.json` (every live or replayed chart also has a *PNG* download), and states whether it may enter a formal comparison and
  why not (aborted, faults, scaling or pacing changes, failed or missing
  reconciliation).
- **Comparisons**: *Run fixed, CPU-HPA and lag-KEDA* runs the same frozen sale once
  per mode, one after another, each as a fresh run and consumer group, waits for
  each drain and reconciliation, and ranks only eligible runs by exact SQL p95,
  with peak backlog, most consumers and replica-seconds beside it. Sessions are
  also grouped by identical logical workload. One run per mode on a shared local
  node is descriptive, not a statistical result.
- **Measured matrix**: the audited 27-run results and figures from
  `docs/results/current/`, read-only. The console never writes them.

## Guided demonstrations and presenting (M14)

The **Guide** page walks through six demonstrations, one step at a time with a
button for each action and what to watch: *No scaling*, *Manual rescue*, *CPU versus
lag*, *Forecast versus lag* (M16), *Database bottleneck* and *Crash and replay*. The expandable glossary maps every Store
term to its System term and states where the analogy stops.

**View options → Present** (key `p`) enters full-screen presentation with a larger floor and
charts; `Esc` leaves. `l` switches the lens and `m` toggles motion; shortcuts are
ignored while typing in a field.

## What is real

- Values come from the cluster. The in-cluster agent samples Kafka offsets and
  group membership, SQL counts, Kubernetes objects and each console consumer's
  metrics roughly once per second while someone is watching. It slows to every
  ten seconds when nobody is.
- Every source reports its own sample time. A failed source shows **—** and an
  error, never zero. The top badge says **Live** only while the browser stream is
  open, the agent answers and samples are under four seconds old. Otherwise it
  says **Stale**, **Console disconnected** or **Cluster agent unreachable**.
- Console runs never enter the measured comparison. They use the unchanged
  consumer image and resource limits, but a separate release, consumer group
  and runs directory (`/runs/console/`).

## Safety and scope

- The server binds to loopback and accepts only loopback `Host` headers. The login
  token becomes an HttpOnly, SameSite=Strict cookie. State-changing requests and
  the live stream also require a loopback `Origin`. Cluster credentials and the
  agent token never reach the browser.
- The agent's Kubernetes role names the exact resources it may change: the
  console consumer Deployment's scale, the console autoscalers, and the Kafka and
  PostgreSQL StatefulSet scale (used only for temporary outages). Pod restarts and
  CPU restriction are checked against the console consumer label in code.
- The console holds the existing cluster workflow lock while a sale is active, so
  cluster deployments, scaling exercises and measured campaigns cannot publish
  into its source range at the same time, and vice versa.
- Every action is a numbered operation in `artifacts/console/journal.jsonl`.
  Repeating a click reuses the original operation instead of running it twice.

## Checks

```sh
./streamscale.sh console-check   # or: make console-check
```

This runs console Python unit tests, web type checks and unit tests, the web
build, console deployment, live-cluster acceptance (sales and controls, all four
scaling modes, every fault, replay and a three-mode comparison, and the launcher's
console lifecycle) and Chrome acceptance, ending with the full journey: start a
sale, overload it, rescue it, crash a consumer, close the database, recover and
reconcile. It writes `artifacts/console/check.json`. `./streamscale.sh all`
includes it.

| Target | Purpose |
| --- | --- |
| `make console-install` | Sync `console/.venv` and `npm ci` in `console/web` |
| `make console-web` | Type-check and build the browser assets |
| `make console-test` | Python and web unit tests |
| `make console-deploy` | Build the worker image on the deployed pipeline image and install the release |
| `make console-live` | Live-cluster acceptance only |
| `make console-serve CONSOLE_ARGS="--port 0"` | Serve the console without rebuilding |

## Files

| Location | Responsibility |
| --- | --- |
| `console/server/streamscale_console/` | Loopback FastAPI server, telemetry derivation, operations and journal |
| `console/worker/streamscale_console_worker/` | In-cluster agent, read-only observer, scoped Kubernetes client, console consumer wrapper |
| `console/deploy/` | Worker image (built on the deployed pipeline image) and the `streamscale-console` chart |
| `console/web/` | React/TypeScript browser console and Playwright browser checks |
| `console/tests/` | Unit tests and `live` cluster acceptance tests |
| `artifacts/console/` | Deployment record, journal, server login record (0600), check results |

## Troubleshooting

- **"Cluster agent unreachable"**: the console keeps retrying its port forward.
  Check `./streamscale.sh status`; after `resume`, the agent pod needs a few
  seconds. `artifacts/console/agent-forward.log` has forward errors.
- **"The StreamScale pipeline is not deployed"** from deploy: run
  `./streamscale.sh cluster` first.
- **Login page instead of the console**: open the full link printed by the
  command; it is also in `artifacts/console/server.json` while the server runs.
