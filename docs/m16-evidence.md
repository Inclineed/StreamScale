# M16 evidence: ML predictive autoscaling

Started 2026-10-28 on the user's request to add an ML-driven scaling policy. Design:
[ML predictive autoscaling](ml-autoscaling.md). State: **implementation complete and
live-exercised; simulator cluster calibration and the 75-run measured campaign are
pending.** No measured comparison of the ML modes is claimed yet.

## Decisions recorded with the user

- Purpose: make forecasting matter, with a gradual-ramp and a repeated-sales shape
  whose timing and size vary by seed; evaluation seeds never appear in training.
- Objective: keep predicted queueing delay within 3 s, then use the fewest consumers.
- Learning: pretrain in a simulator calibrated to this cluster, adapt online.
- Model: quantile gradient-boosted trees (scikit-learn, separately locked `ml/`
  project) exported to JSON and evaluated in plain Python.
- Campaign: protocol v3, 5 modes × 5 scenarios × 3 repetitions; results beside M7.
- Launcher: `all` runs the M16 campaign only; M7 stays runnable separately.
- Rejected alternatives: reinforcement learning (needs a simulator good enough to
  plan with directly), deep sequence models (too little data, large image cost),
  Bayesian tuning of KEDA (tunes an existing policy, still bound by the 15 s HPA sync).

## Scope delivered

- Exporter gauge `streamscale_kafka_high_watermark{partition}` (arrival rate from
  Kafka offsets, never from the schedule).
- Controller core `src/streamscale/autoscale/`: planner with pod-start delay,
  rebalance pause, per-consumer-count capacity, recourse and confirmation of
  forecast-only scale-ups; reactive and predictive policies; replay; controller loop;
  scoped Kubernetes client (replicas 1–3 only).
- Seeded shapes with `train:`/`eval:`/`stress:` namespaces; migration `0002` widens
  the mode and scenario checks (0001 unchanged).
- `ml/`: simulator, M7 calibration (gate passed), cluster-calibration hook,
  training, quantile tuning (chose 0.9), offline evaluation.
- Chart: autoscaler Deployment, ServiceAccount and Role (`get` deployment, `get/patch`
  `deployments/scale`, by resource name); no runs volume, ConfigMap or credentials.
- Runner/audit/analysis: protocol v3, per-protocol folders, paired seeds per
  repetition, per-sale metrics, ML isolation and seed-separation audit, scratch and
  calibration subsets that analysis refuses to write under `docs/results`.
- Console: both ML modes in the agent (same core), override/fault interplay, decision
  panel, ramp and repeated-sales presets, a sixth guided demo.
- Launcher, Make targets, README, run guide, scaling, experiments and console docs.

## Checks run

| Check | Result |
| --- | --- |
| `make test`, `make check` | 208 passed; Ruff clean |
| `make integration` | 28 passed (0002 applied on scratch schemas and the local DB) |
| `make ml-test` | 35 passed (tree export matches scikit-learn within 1e-9) |
| `make console-test` (Python, web) | 147 + 19 passed; web typecheck and build clean |
| Simulator M7 calibration | Fixed and Lag-KEDA peak lag and replica-seconds within gate |
| Forecaster hold-out (`train:100000..`) | Pinball 0.058–0.151 vs persistence 0.093–0.369; coverage 0.90 at q=0.9 |
| `./streamscale.sh scaling` (2026-10-28T17:38–17:43Z) | All five modes passed; both ML modes scaled 1→3 at +1 s and reconciled 5,940 facts |
| v3 scratch campaign `campaign-20261028T175318-e93b60` | 4 runs passed reconciliation, audit (949 samples) and report |

## Defects found and fixed

- First live ML exercise never scaled: with an unpaced backlog and zero arrivals, the
  delay before any pod could start masked every candidate equally. The planner now
  ignores the uncontrollable first seconds (regression test added).
- `sale_metrics` failed on runs without a sale (pilot warmup).
- The console limits sales to 12 phases; the ramp preset is coarsened.
- The comparison-CSV audit compared rows without the ML-only columns.
- Scratch analysis used the full protocol's mode list instead of the subset's.

## Open findings

- Scratch ramp run (pre-fix image): ML-predictive reached three ready consumers
  about 10 s before Lag-KEDA, yet its peak lag was higher (1,550 vs 938) because the
  learned per-consumer capacity at three consumers was about 90/s against 112/s at
  one, below the ramp's 324/s peak. The planner assumed one capacity for all counts.
  Per-count capacity learning was added after this run; the simulator's
  three-consumer efficiency (0.95 from M7 alone) is refined by `make ml-calibrate`.
- These are single scratch runs, not evidence.

## Cluster calibration (2026-10-28)

Campaign `artifacts/experiments/v3/calibration/campaign-20261028T181815-056607`:
fixed, Lag-KEDA and ML-reactive on flash-sale, ramp and repeated-sales `train:`
shapes, one repetition each. All 9 measured runs reconciled and passed the audit.
One attempt (repeated-sales/ML-reactive) was excluded and retried: its samples
contain a 4,507 s gap, consistent with the host suspending mid-run.

Refitting the simulator's timing to these runs matched 8 of 9 within 30%; the
refit then misses M7's flash-sale KEDA peak lag (4,365 vs 1,714). Two findings:

- This cluster's Lag-KEDA flash-sale peak lag was 4,507, against M7's 1,714 median:
  its first extra consumer was ready about 22 s after the sale started, not 8 s.
  HPA sync phase varies run to run, which single repetitions cannot average out.
- The retried repeated-sales/ML-reactive run reached 3,687 peak lag although it
  requested three consumers 1 s after the sale began: those pods took 9–12 s to
  become Ready (3–5 s in other runs), and the controller's frequent scale changes
  created 9 pods in one run. Pod start time on a loaded node is not captured by a
  single fixed delay.

The packaged simulator and forecaster therefore stay calibrated to M7 only
(`ml/calibration.json`, gate passed); the cluster refit is kept as
`artifacts/ml/cluster-calibration.json` and its report for the record.

## Scale-down stabilization and pilot (2026-10-28)

Before the campaign the controller's scale-in was changed (scale-up unchanged):

- Hysteresis: the next lower count must keep its predicted delay within
  0.5 × 3 s = 1.5 s, assuming no later rescue, for 15 consecutive seconds. Any
  other second restarts the clock.
- The group must be stable: ready = desired = Kafka group members for 5 s.
- Only one consumer is removed per change, and at least 20 s after any change.
- Bounds stay 1–3. Six new unit tests cover these rules.

Pilot 1 (`campaign-20261028T214855-f8b255`, excluded) exposed a harness defect, not
a data loss: the reconciliation `kubectl exec` targeted a consumer pod that the ML
controller was scaling in. A re-run of the same inspection passed with 17,834 of
17,834 facts and zero missing IDs. Fixes: inspection retries on a Ready,
non-terminating pod, and the ML controller is paused after drain, before
reconciliation and the controlled restart.

Pilot 2 (`campaign-20261028T220530-c90ec5`, scratch, `eval:` repetition-1 shapes):
all 4 runs reconciled before and after restart with zero missing or duplicate IDs;
audit passed (941 samples).

| Scenario | Mode | p95 (s) | Peak lag | Replica-s | Scale changes (up/down) |
| --- | --- | ---: | ---: | ---: | ---: |
| flash-sale | ML-predictive | 4.59 | 1,100 | 149 | 3 (1/2) |
| flash-sale | ML-reactive | 4.28 | 1,026 | 153 | 3 (1/2) |
| repeated-sales | ML-predictive | 3.74 | 739 | 530 | 9 (4/5) |
| repeated-sales | ML-reactive | 5.26 | 1,089 | 502 | 11 (4/7) |

Single scratch runs; not evidence. Every scale-in was one consumer and none came
within 15 s of the previous scale-in. ML-reactive still dropped to one consumer
2–3 s before two of the repeated sales, then had to scale up again; ML-predictive
kept two consumers through those gaps. That difference is what the campaign
measures, so it was not tuned away.

Frozen after the pilot: controller settings in `experiments/protocols/v3.json`
(target 3 s, ready delay 5 s, hold 15 s, margin 0.5, interval 20 s, stable group
5 s) and forecaster sha256 `1c5304be…d491` (retrained for the new controller on
`train:` seeds, quantile 0.9). The protocol pins the hash; `freeze()` refuses any
other model.

## Measured campaign (protocol v3)

`./streamscale.sh experiments-v3 --fresh`, campaign `campaign-20261028T225554-fc5dec`,
2026-10-28T22:55:54Z to 2026-10-29T03:22:06Z, image `streamscale:fa2bb34b0d8ce0d1`
built from clean commit `a35190b`, forecaster `1c5304be…`. The host was held awake
with `systemd-inhibit`. Log: `artifacts/logs/m16-campaign.log`.

- 75 of 75 measured runs passed reconciliation before and after the controlled
  restart; 152 accepted executions (pilot, warmups, measured); 0 excluded attempts.
- Independent audit passed: 12,049 raw samples, ML isolation in every sample,
  `eval:` shape seeds disjoint from the forecaster's `train:` seeds, model hash
  matches the pinned one.
- Pilot capacity this campaign was 83.77 events/s (M7: 130.50), so rates froze at
  32/s steady and 250/s surge. The host was slower than during M7; M16 numbers are
  not directly comparable with M7's.
- `./streamscale.sh analysis-v3` wrote `docs/results/m16/`; `docs/results/current/`
  (M7) is unchanged.

Median run p95 latency (s) [min, max] over three repetitions, and median ready
replica-seconds:

| Scenario | Lag-KEDA | ML-reactive | ML-predictive |
| --- | --- | --- | --- |
| steady | 1.12 · 71 | 1.13 · 72 | 1.12 · 71 |
| flash-sale | 7.16 [6.11, 8.99] · 157 | 3.70 [3.56, 4.09] · 149 | 4.04 [3.66, 4.09] · 149 |
| overload-recovery | 7.02 [4.50, 7.34] · 194 | 4.32 [3.95, 6.61] · 183 | 5.94 [5.78, 6.64] · 178 |
| ramp | 5.93 [5.14, 7.44] · 164 | 4.23 [3.78, 7.76] · 155 | 1.61 [1.18, 3.20] · 169 |
| repeated-sales | 6.23 [3.69, 7.16] · 420 | 3.97 [3.22, 4.36] · 387 | 2.88 [2.61, 4.29] · 403 |

Descriptive reading of this campaign only:

- Both ML modes had lower median p95 than Lag-KEDA in every surge scenario, at
  similar or lower replica-seconds. Most of that gap is also present for
  ML-reactive, so it comes from the one-second loop rather than the forecast.
- The forecast (ML-predictive vs ML-reactive) helped where traffic had structure:
  ramp 1.61 vs 4.23 s and repeated sales 2.88 vs 3.97 s. It did not help on the
  unannounced step surges: flash-sale 4.04 vs 3.70 s, overload 5.94 vs 4.32 s.
- Ranges overlap in several cells (for example ramp ML-reactive [3.78, 7.76]).
  Three repetitions on one shared node; no statistical significance is claimed.
- Fixed and CPU-HPA results are in the report; steady load was identical across modes.

## Next

1. `./streamscale.sh all --no-ui` full acceptance (running).
2. Commit results locally.

## Previously planned (done)

1. `./streamscale.sh experiments-v3 --fresh` (about 3.5 hours; keep the host awake),
   then `./streamscale.sh analysis-v3`.
2. `./streamscale.sh all --no-ui` full acceptance, including console checks.
3. Consider damping scale-in churn (pod start grows under churn) before the
   campaign, since the protocol freezes controller settings once it starts.
