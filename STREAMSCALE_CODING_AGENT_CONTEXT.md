# StreamScale — context and instructions for a coding agent

**Implementation language:** Python. The user's general preference for C++ does not change this project's agreed Python pipeline.  
**Working style:** complete one milestone at a time, explain the approach and file responsibilities before writing code, and show evidence for each acceptance criterion.

## Your assignment

Build a small, reproducible Kafka-to-PostgreSQL order-event pipeline and compare three consumer scaling modes under identical synthetic workloads. This is a **flash-sale analytics experiment**, not a checkout or payment system. Correctness is a prerequisite for comparing speed.

Start by inspecting the repository and existing requirements. If there is no implementation repository yet, create or ask for its location before writing source files. Begin with **M0: freeze the executable contract**, then implement **M1**. Do not mark a milestone done merely because files exist; run its checks and record the results. At each step, give the user a short approach outline, the files/modules you plan to touch and why, the acceptance evidence, and the next milestone.

## What the system does

1. A seeded Python workload tool creates synthetic order events and publishes them to Kafka. It can deliberately inject malformed, duplicate, and late events. Generated files and run outputs are experiment artifacts; the generator's source belongs in the repo, but it is **not** part of the continuously running consumer service.
2. Kafka input topic `orders.events.v1` has **exactly three partitions**. Publish with `order_id` as the message key. Ordering is per partition and per order, not global.
3. One to three replicas of the same Python consumer, in one consumer group, poll bounded batches. The consumer decodes and validates each record.
4. For a valid record, one PostgreSQL transaction inserts an immutable `order_event` fact if new and conditionally updates `order_state`. **Commit the database transaction before committing that partition's Kafka offset.**
5. For an invalid record, publish an envelope with the original record and reason to `orders.events.dlq.v1`. Wait for broker acknowledgement **before** committing the source offset.
6. Prometheus collects operational metrics; Grafana displays them. SQL reads the accepted event history and latest state. An experiment runner records configuration and exports per-run summaries.

Use a deliberately simple processing loop initially: handle records sequentially within a partition and advance only a contiguous sequence of durably handled offsets. A restart or rebalance can replay the last record. That is expected under **at-least-once** processing. Never claim end-to-end exactly once or a cross-system transaction between Kafka and PostgreSQL.

## Event contract v1

UTF-8 JSON, validated with JSON Schema Draft 2020-12; reject unknown top-level fields. Semantic validation also checks supported version/type, amount and timestamps.

| Field | Rule |
| --- | --- |
| `run_id` | UUID identifying one experiment execution. |
| `event_id` | UUID, globally unique for distinct logical events; a deliberate duplicate reuses the original ID. |
| `order_id` | UUID; also the Kafka message key. |
| `event_type` | `ORDER_CREATED`, `PAYMENT_CONFIRMED`, or `ORDER_CANCELLED`. |
| `event_time` | Timezone-aware RFC 3339 timestamp representing business event time. |
| `produced_at` | Timezone-aware RFC 3339 timestamp for latency measurement. |
| `order_amount` | Nonnegative decimal with two fractional places where applicable; represent precisely rather than with binary floating point. |
| `schema_version` | Positive integer; support `1` initially. |

The seed must reproduce the **logical workload**: order sequence, event kinds, amounts, timing pattern and injected faults. Each execution gets its own `run_id`. To preserve the documented global `event_id` key while rerunning the same seed, derive each distinct event ID from the new `run_id` and stable logical event index (or use an equivalent scheme with proven global uniqueness); an injected duplicate repeats that run's ID. Keep `order_id` stable for the same logical workload across runs if it helps comparison. Set `produced_at` for the actual publish attempt so end-to-end latency measures this run, while preserving the workload's relative timing and deliberate late-event relationships. Record these choices in M0 and test them. Never reuse an identical `(run_id, event_id)` set for a new run in a retained database.

## Persistence and failure rules

Keep the relational model small:

| Relation | Purpose and constraints |
| --- | --- |
| `experiment_run` | One row per run: `run_id`, mode, scenario, seed, start/end time, Git commit, JSONB configuration snapshot. |
| `order_event` | Append-only accepted facts, typed event fields, Kafka partition/offset, ingestion time and original accepted JSONB. Documented primary key `event_id`; unique `(run_id, kafka_partition, kafka_offset)`. |
| `order_state` | Derived latest state, primary key `(run_id, order_id)`, including status, amount, last event ID/time/offset and update time. |

For `order_state`, apply an incoming event only if its `(event_time, kafka_offset)` is greater than the stored pair. All events for one order share a partition, so the offset breaks equal-time ties. A late event remains in `order_event` but cannot roll the projection backward. This is the documented v1 ordering rule; do not add `order_version` without updating the contract and tests first. Derive status from the event type; do not invent a payment or inventory workflow.

The consumer must distinguish outcomes:

| Case | Durable handling before source-offset commit |
| --- | --- |
| New valid event | Commit event insert and conditional state update in one PostgreSQL transaction. |
| Redelivered valid event | Detect the existing `event_id`; leave one fact row and a correct state. Commit the source offset after confirming durable handling. |
| Invalid event | Publish a reasoned DLQ envelope and wait for broker acknowledgement. |
| PostgreSQL temporarily unavailable | Retry with bounded backoff or pause processing; do not put an otherwise valid record in the malformed-input DLQ or advance its offset. |
| Process crash or rebalance | Resume from the last committed offset; redelivery must be safe. |

The DLQ envelope contains original key and payload, source topic/partition/offset, error code, short detail and failure timestamp. A crash after DLQ acknowledgement but before source-offset commit can publish another DLQ copy on replay; record this limit rather than promising unique DLQ entries. Avoid committing a later offset while an earlier record in the same partition lacks durable handling.

Limit indexes initially to primary/unique constraints and `(run_id, event_time)` and `(order_id, event_time DESC)`. Add further indexes only for an observed query need with `EXPLAIN` evidence. Business event history lives in PostgreSQL; backlog is measured from Kafka offsets, not inferred from the database.

## Experiment contract

Compare these **alternative** modes with the same consumer image and resource limits:

| Mode | Scaling control | Replica range |
| --- | --- | --- |
| Fixed | One consumer | 1 |
| CPU-HPA | Kubernetes CPU-based Horizontal Pod Autoscaler | 1–3 |
| Lag-KEDA | KEDA Kafka consumer-lag scaler | 1–3 |

Never enable HPA and KEDA together for a measured run. Three partitions cap useful consumer parallelism at three. Use a local `kind` Kubernetes cluster and Helm for the deployment experiment; use a small local Kafka/PostgreSQL stack for early development. HPA needs usable Kubernetes CPU metrics. Keep Kafka, PostgreSQL, topic partitioning, consumer image and resource configuration comparable across modes.

Run three scenarios—steady traffic, flash-sale burst, and sustained overload followed by recovery—with three repetitions of each mode/scenario pair: **27 planned measured runs**, after a pilot. Reuse the same scenario seed and logical workload across compared modes. Capture run ID, seed, mode, scenario, image/Git revision, configuration, warm-up and measurement windows, resource limits, and start/end times. Isolate each run's state and offsets, or explicitly reset them, so a previous run cannot affect the next one.

**Primary outcomes:** throughput, p95 end-to-end latency, maximum consumer lag. **Supporting outcomes:** recovery time, scaling response, CPU and memory, replica-seconds. Prometheus should expose generated/accepted/duplicate/invalid/DLQ/failed counters, end-to-end and DB-latency histograms, and lag/replica gauges. Use bounded labels such as run/mode/scenario/result; never label by `event_id` or `order_id`. Export a CSV summary keyed by run ID and preserve the raw metrics needed to audit it.

Before ranking performance, reconcile producer acknowledgements, accepted unique IDs, duplicates and invalid/DLQ outcomes, accounting explicitly for possible DLQ redelivery. An acceptable run has **zero missing acknowledged valid identifiers, zero duplicate stored identifiers, and no latest-state regression after controlled restart and backlog drain**. Exclude failed runs from performance rankings and report the failure. No mode is presumed faster; CPU versus lag is a hypothesis to test.

## Scope boundaries

Keep the first version to one input topic, one DLQ, one PostgreSQL database, one consumer implementation and one metrics stack. Do not add payment settlement, inventory reservation, fraud detection, personal data, customer-facing flows, Spark, Flink, Airflow, Kafka Connect, Schema Registry, Kafka transactions, predictive autoscaling, multiple cloud providers, or a full web dashboard. Do not generate names, email addresses, addresses or payment credentials.

> **Scope change (M16, 2026-10-28, user-requested):** predictive autoscaling is now in scope as two additional alternative modes, `ml-predictive` and its `ml-reactive` ablation, measured in a separate protocol v3 campaign beside the unchanged M7 results. The other boundaries still apply. See `docs/ml-autoscaling.md`.

## Suggested repository layout

```text
src/streamscale/          consumer loop, validation, storage, metrics
schemas/                  versioned event and DLQ contracts
db/migrations/            PostgreSQL schema
tools/workload/           seeded generator and Kafka publisher
deploy/local/             local development stack
deploy/k8s/               kind/Helm configuration and scaling modes
monitoring/               Prometheus/Grafana definitions
experiments/              run orchestration, reconciliation, CSV analysis
tests/                    behavior and failure tests
docs/                     decisions, operations, reproducibility notes
```

Adjust this to the existing repository instead of reshuffling working code. Keep generated datasets, secrets and bulky raw run results out of source control; a small deterministic fixture can be committed. Pin dependencies and provide commands for a new teammate to reproduce the local pipeline.

## Milestones and evidence

| Milestone | Deliverable | Acceptance evidence |
| --- | --- | --- |
| **M0 — Contract** | Schema, run/ID generation rule, late-event rule, DLQ codes, SQL migration plan, workload and measurement definitions. | Reviewed decisions and fixtures; contradictions with older documents resolved before migration. |
| **M1 — Local foundation** | Repo structure, dependencies, Kafka/PostgreSQL local stack, three-partition topic. | Reproducible startup and manual publish/consume smoke test. |
| **M2 — Workload** | Seeded generator and publisher, normal/invalid/duplicate/late fixtures. | Same seed gives same logical sequence; key/partition, fault and ID behavior verified. |
| **M3 — One consumer** | Validation, one transactional DB write, manual source-offset commits. | SQL history/state inspection and crash-after-DB-commit replay without duplicate rows. |
| **M4 — Fault handling** | DLQ, retry/backoff, late-event rule, restart/rebalance behavior. | Deliberate malformed input, DB outage, duplicate, late arrival and replay cases pass. |
| **M5 — Deployment and metrics** | Container, kind/Helm, Prometheus and Grafana. | One consumer works in cluster; metrics and run labels are inspectable. |
| **M6 — Scaling modes** | Fixed, CPU-HPA and lag-KEDA configurations. | Independently exercise each mode; 1–3 replica bound and mode isolation verified. |
| **M7 — Runs** | Pilot, then controlled 27-run matrix and reconciliation. | Per-run configuration, raw evidence and CSV; only correct runs enter comparison. |
| **M8 — Analysis** | Plots, interpretation, reproducibility instructions and presentation evidence. | Report explains limitations and results without claiming unmeasured improvement. |

Before each milestone, confirm owner, estimate, acceptance check and dependencies in the tracker. After each milestone, record tested commands/results, actual task-state timestamps, defects, review, and the next task. Dataset generation and benchmark outputs are experimental work adjacent to the service code, not proof that the service is implemented. Sprint burnup, burndown, throughput, cumulative flow, cycle time and velocity require actual dated transitions and agreed estimates; do not invent values from the old 15% planning figure.

## First response expected from the coding agent

1. State what repository and requirements you inspected, and any mismatch with this brief.
2. Propose the M0 contract decisions, especially cross-run event IDs and the existing `(event_time, kafka_offset)` projection ordering. Treat the rules above as the default reading of the supplied design, and surface any reason they cannot be implemented.
3. Give a short M1 approach with intended files, responsibilities, setup commands and a concrete smoke test. Then implement M1 once the contract is coherent; report actual evidence and stop at its boundary before starting M2.

**Supplied project references:** `StreamScale_Data_Model_Group6.tex`, `Lab4_5_Group6.tex`, the Group 6 tracker and its data-model change note. Their statements describe a plan, not verified runtime behavior. The user's subsequent milestone sequence and direct instructions take precedence where they refine those artifacts.
