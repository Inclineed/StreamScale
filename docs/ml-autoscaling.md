# ML predictive autoscaling (M16)

M16 adds two scaling modes and two workload shapes, and measures them beside the
three M7 modes. The original brief listed predictive autoscaling as out of scope;
the user lifted that boundary for M16 (see the brief's scope note).

| Mode | What sets replicas | Learned parts |
| --- | --- | --- |
| `ml-predictive` | One-second planner using a quantile forecast of arrivals | Forecaster (offline) and bias, capacity, pod-start delay (online) |
| `ml-reactive` | The same planner, assuming the current arrival rate continues | None (ablation) |

Both modes keep the 1–3 replica contract and use the same consumer image and
resources as every other mode.

## Why this model

The decision space is three replica counts, so the policy does not need a large
model. What matters is timing. In M7, KEDA's first extra consumer was ready
7.6–9.6 s into the flash sale, of which pod start and group join were about
4–5 s; the rest is KEDA's 2 s poll plus the HPA's 15 s sync. After the backlog
cleared, KEDA's 60 s scale-down window kept three consumers until the run ended,
which is most of its 173 vs 90 replica-seconds against fixed.

So the controller is model-predictive control over a small queue model:

1. **Observe** each second: per-partition high watermarks and lag from the run
   exporter, desired and ready replicas from the Deployment.
2. **Forecast** arrivals: a quantile (0.6 in the frozen M16 model, chosen by tuning;
   0.9 before the scale-in change) of the peak arrival rate in four windows covering the next 20 s, from
   gradient-boosted trees. No
   forecast window falls below the current arrival rate, so the forecast can only
   add consumers earlier than the ablation, never remove them sooner.
3. **Plan**: for 1, 2 and 3 consumers, simulate per-partition backlog over the
   horizon, including the pod-start delay and the rebalance pause, and choose the
   fewest consumers whose predicted queueing delay stays within 3 s. Delay before
   any new pod could start is excluded, because no choice can change it. Smaller
   counts are judged assuming the next decision may still add consumers.
4. **Act**: scale up at once. Scale down by one consumer only after the next
   lower count has kept its predicted delay within 1.5 s (half the target, with
   no later rescue) for 15 consecutive seconds, the group has been ready and
   stable (ready = desired = Kafka members) for 5 s, and at least 20 s have
   passed since the last change. Stale data holds.

Reinforcement learning and deep sequence models were considered and rejected for
this setup: they need far more training data than a local cluster produces, and a
simulator good enough to train them is good enough to plan with directly. See the
planning discussion recorded in the M16 evidence.

## Where the code lives

| Path | Responsibility |
| --- | --- |
| `src/streamscale/autoscale/` | Plain-Python controller core shared by the cluster, console and simulator: observations, features, forecaster evaluation, planner, policies, replay, controller loop, scoped Kubernetes client |
| `src/streamscale/autoscale/models/forecaster-v1.json` | Exported trees with feature order, quantile, training seeds, simulator parameters and tuning record |
| `tools/workload/streamscale_workload/shapes.py` | Seeded shapes for all five scenarios, with `train:`/`eval:`/`stress:` seed namespaces |
| `ml/` | Separately locked offline tooling (numpy, scikit-learn): simulator, calibration, training, tuning, evaluation |
| `deploy/k8s/chart/templates/scaling.yaml` | Autoscaler Deployment, ServiceAccount and Role for the ML modes |
| `experiments/protocols/v3.json` | The M16 measured protocol |
| `console/worker/streamscale_console_worker/autoscale.py` | The same core driving the console's consumers |

The cluster image gains no libraries: trees are evaluated in plain Python, and
predictions match scikit-learn to floating-point precision (tested).

## Offline pipeline

```sh
./streamscale.sh ml          # tests + offline evaluation (no cluster)
./streamscale.sh ml-train    # refit simulator, retune quantile, retrain model
make ml-sim                  # simulated comparison on training seeds
make autoscale-replay REPLAY=flash-sale   # planner decisions over a synthetic trace
```

- **Simulator** (`ml/streamscale_ml/sim.py`): one-second closed loop with three
  partitions, per-pod sequential service, pod-ready delay, readiness-probe lag,
  rebalance pause, the correctness profile's 2% extra and 5% late records, and
  KEDA-like, HPA-like and fixed baselines.
- **Calibration** (`make ml-fit` → `ml/calibration.json`): fitted to M7's
  committed medians. Fixed and KEDA peak lag and replica-seconds must land within
  20% of the median or inside the measured range; scaling response is fitted too.
  CPU-HPA is reported but not gated, because its metrics pipeline is approximated.
- **Training** (`make ml-train`): about 120,000 rows from 700 closed-loop traces on
  `train:0..699`, with rates drawn per trace relative to capacity and a variable
  quiet lead so the model cannot memorise the protocol's fixed sale start. Targets
  are the peak arrivals per capacity in each future window. Hold-out quality uses
  `train:100000..`; quantile tuning uses `train:200000..` and chooses the quantile
  that most often meets the 3 s target, then the fewest replica-seconds.
- **Evaluation** (`make ml-eval` → `artifacts/ml/offline-eval.json`): every mode
  on `eval:` seeds and on out-of-range `stress:` seeds. These are simulated
  outcomes, not measurements.

Training code refuses `eval:` seeds; the measured audit checks that the model's
training seeds and the campaign's shape seeds come from different namespaces.

## In the cluster

Choosing an ML mode renders one `streamscale-autoscaler` Deployment (same image,
command `streamscale autoscale --policy …`). Its Role allows only `get` on the
consumer Deployment and `get`/`patch` on its `scale` subresource, by resource
name. It mounts no runs volume, run ConfigMap or database credentials, so it
cannot read a run's planned schedule. It serves `/metrics` (Prometheus job
`autoscaler`) and `/decisions` with every recent decision and its reason. Mode
switches delete it before resetting the consumer to one replica.

## Measured protocol v3

`./streamscale.sh experiments-v3` runs 5 modes × 5 scenarios × 3 repetitions in
seed-4343 order, after its own pilot. Steady, flash-sale and overload-recovery keep
their v2 shapes; ramp and repeated-sales vary by repetition from reserved `eval:`
seeds, and every mode in a repetition sees the same logical sequence. Controller
settings and the model hash are frozen with the pilot rates before measurement.
New per-run metrics: consumers ready at each sale's start, per-sale response and
scale changes. Results go to `docs/results/m16/`; M7's `docs/results/current/`
is never rewritten. A scratch subset (`--modes`, `--scenarios`, `--repetitions`)
runs in its own folder and analysis refuses to write it under `docs/results`.

## Limits

- Evaluation shapes come from the same generator and ranges as training, under
  different seeds; stress seeds test outside those ranges in simulation only.
- The model inherits simulator error; calibration is against M7 medians on one node.
- `ml-reactive` against Lag-KEDA mostly measures loop speed; only `ml-predictive`
  against `ml-reactive` isolates what the forecast and online learning add.
