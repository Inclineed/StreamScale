# Measured experiment protocol

This page describes protocol v2 (M7, 27 runs). M16's protocol v3 adds the
`ml-reactive` and `ml-predictive` modes and the ramp and repeated-sales shapes in
a 75-run campaign; it is described in [ML predictive autoscaling](ml-autoscaling.md#measured-protocol-v3)
and run with `./streamscale.sh experiments-v3`. Since M16, `all` runs the v3
campaign; v2 stays runnable with `./streamscale.sh experiments` and its committed
results in `docs/results/current/` are not rewritten. A complete retained
campaign is verified by checksums even after later code changes; resuming an
incomplete one still requires unchanged code.

M7 is implemented. Run `./streamscale.sh experiments`, or add `--fresh` to create
a new campaign while retaining all previous evidence.
Only one cluster workflow may run at a time; the launcher takes an exclusive
lock. Do not start a separate cluster/scaling run during measurements.

The version 2 local protocol starts with 30 seconds of fault-free warmup and an
unpaced 1,000-event fixed-consumer pilot. Its SQL acceptance rate determines
even integer event rates: steady = 0.4 times pilot capacity, surge = 3 times
capacity, capped at 1,000 events/second. Rates and windows are then written to
`frozen-protocol.json` before the matrix starts and never adjusted mid-campaign.
The original M0 proposal used longer 120-second windows and fixed rates; these
60-second local windows are a declared protocol revision, not directly
comparable to that proposal.

| Scenario | Publishing schedule | Logical seed |
| --- | --- | --- |
| Steady | 60 seconds at steady rate | 101 |
| Flash sale | 15 seconds steady, 20 surge, 25 steady | 202 |
| Overload/recovery | 35 seconds surge, 25 steady | 303 |

Each mode/scenario has three repetitions. Seed 4242 shuffles the cell order.
Every cell first performs a separate 30-second fault-free warmup in the same
mode, followed by a fresh measured run/group with the correctness profile
(invalid, duplicate and late attempts). Its complete schedule is generated;
the number of orders does not truncate a phase. The correctness profile selects
valid attempts equal to 5% of the base event count and delays them by ten
seconds, so the 60-second base schedule has actual late publication through
about 70 seconds. `publish_seconds` records this; the primary window stays
60 seconds and all modes use identical delayed manifests. Duplicate/invalid
fixtures also affect the observed attempt rate. Late tail arrivals constrain
the five-fresh-zero recovery criterion as well as the final drain.
The logical scenario manifest
hash must match across all nine corresponding runs, while run/event/group IDs
are fresh. The original immutable image, consumer resource requests/limits,
three partitions and retained broker/SQL installation are shared. Run metadata
contains the configuration and image/Git provenance, including any dirty state.
Warmup warms the shared services; measured group startup is still included.

`summary.csv` is keyed by measured run UUID. `comparison.csv` contains only
eligible measured runs; `all-runs.csv` also includes pilot and warmups.
`failed-attempts.csv` records interrupted executions excluded from comparison.
Run directories preserve manifests, producer acknowledgement logs, exact SQL
fact timestamps, before/after-restart inspections, one-second samples of fresh
broker-offset lag and ready replicas, five-second Kubernetes resource samples,
Prometheus range responses, raw exporter scrapes and file checksums.

Primary throughput counts unique SQL facts accepted in the half-open scheduled
publishing window, divided by that window's duration. Separate columns report
processing-span throughput and drain-inclusive throughput. p95 is the empirical
nearest rank of all this run's unique SQL `ingested_at - produced_at` values,
including the drained tail; it is not an average of pod histogram percentiles.
Maximum lag is the maximum sampled sum of all three partitions. Overload
recovery starts at the high-to-steady transition and ends at five distinct fresh
one-second zero-lag samples; missing recovery is explicitly censored.
Scaling response is the first increase above the ready replica count preceding
the load transition. No response is recorded as missing, not zero seconds.
Replica-seconds and CPU core-seconds integrate actual observed timestamps;
coverage columns disclose missing leading measurements. Resource integrals
hold each observation until the next; these are sampled approximations.

Sampling continues through planned publication and confirmed drain. A rollout
restart occurs afterward, outside the measurement window. Both inspections
must reconcile every acknowledged valid ID, duplicates, reasoned DLQ sources,
latest states and zero lag. SQL identities/counts must match inspection, clocks
must produce nonnegative latency, at least 80% of scheduled lag samples and
consumer resource data must be present. Failed or interrupted measurements do
not enter rankings. Restart replay guarantees remain at-least-once; the DLQ
may contain additional copies of a source record.

Resume preserves the frozen protocol and original image. It refuses changed
experiment/runtime/chart source, changed raw checksums or missing files. An
interrupted active run is drained/reconciled before new publication, retained
as excluded evidence and retried with a new run ID. A completed campaign is
reused without republishing its 27 cells. A fresh fixed demo is left running
after new measurements finish, for the normal `verify`/`ui` commands.

`make experiment-audit` independently rechecks preserved file hashes, the
comparison CSV values, raw SQL p95/window throughput, fresh consumer groups,
all 27 cells, and the actual pod image IDs and resource settings. This audit
needs no running cluster. The experiment Make targets include it automatically.
During an active campaign, `python3 -m experiments.evidence --partial` audits
completed evidence without claiming the matrix is complete.

The single kind node shares host CPU, broker, database and monitoring resources.
Small samples (three repetitions), short windows, metrics/controller cadence,
group startup, retained database growth and cache carryover limit interpretation.
Randomized order mitigates carryover but does not eliminate it. A sampled lag
maximum is not a continuous maximum. Results describe this local system only.
