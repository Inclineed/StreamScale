# M16 supplement: target compliance, step-surge diagnosis and ablation

Derived read-only from the frozen campaign; the [M16 report](../m16/report.md) and [M7 report](../current/report.md) are unchanged. Values are medians over three repetitions on one node. No statistical significance is claimed.

## Queueing-target compliance (3 s)

**Measured** = share of unique accepted events whose SQL end-to-end latency was within 3 s. **Predicted** = share of controller decisions whose chosen replica count had a planner-predicted queueing delay within 3 s; a model output, available only for ML modes, that excludes the seconds before any new pod could start. The two measure different things and should not be read as one number.

| Scenario | Mode | Measured within 3 s | Measured p95 (s) | Predicted: decisions within 3 s | Predicted median delay (s) |
| --- | --- | ---: | ---: | ---: | ---: |
| flash-sale | fixed | 14% | 33.43 | n/a | n/a |
| flash-sale | cpu-hpa | 22% | 11.67 | n/a | n/a |
| flash-sale | lag-keda | 27% | 7.16 | n/a | n/a |
| flash-sale | ml-reactive | 81% | 3.70 | 81% | 0.13 |
| flash-sale | ml-predictive | 75% | 4.04 | 87% | 0.13 |
| overload-recovery | fixed | 4% | 50.93 | n/a | n/a |
| overload-recovery | cpu-hpa | 4% | 27.72 | n/a | n/a |
| overload-recovery | lag-keda | 34% | 7.02 | n/a | n/a |
| overload-recovery | ml-reactive | 80% | 4.32 | 81% | 0.14 |
| overload-recovery | ml-predictive | 52% | 5.94 | 76% | 0.12 |
| ramp | fixed | 30% | 22.36 | n/a | n/a |
| ramp | cpu-hpa | 36% | 11.26 | n/a | n/a |
| ramp | lag-keda | 47% | 5.93 | n/a | n/a |
| ramp | ml-reactive | 72% | 4.23 | 91% | 0.13 |
| ramp | ml-predictive | 100% | 1.61 | 99% | 0.13 |
| repeated-sales | fixed | 19% | 21.94 | n/a | n/a |
| repeated-sales | cpu-hpa | 81% | 13.72 | n/a | n/a |
| repeated-sales | lag-keda | 70% | 6.23 | n/a | n/a |
| repeated-sales | ml-reactive | 85% | 3.97 | 91% | 0.13 |
| repeated-sales | ml-predictive | 96% | 2.88 | 92% | 0.13 |
| steady | fixed | 100% | 1.13 | n/a | n/a |
| steady | cpu-hpa | 100% | 1.15 | n/a | n/a |
| steady | lag-keda | 100% | 1.12 | n/a | n/a |
| steady | ml-reactive | 100% | 1.13 | 100% | 0.13 |
| steady | ml-predictive | 100% | 1.11 | 100% | 0.14 |

## Why ML-predictive had higher p95 on flash-sale and overload-recovery

Seconds after the load transition (flash-sale: the surge start; overload-recovery: publication start, when the surge begins), medians of three runs:

| Scenario | Mode | Controller ready | First decision | Scale-up requested | Three consumers ready | Forecast-driven decisions | Measured p95 (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| flash-sale | ml-reactive | -11.8 | -11.6 | 2.4 | 9.2 | 0.0 | 3.7 |
| flash-sale | ml-predictive | -11.1 | -11.8 | 1.8 | 9.7 | 0.0 | 4.0 |
| overload-recovery | ml-reactive | 2.7 | 2.6 | 2.6 | 10.6 | 0.0 | 4.3 |
| overload-recovery | ml-predictive | 4.5 | 3.9 | 3.9 | 11.4 | 0.0 | 5.9 |

Findings from the decision traces (`surge-timing.csv`, `autoscaler-decisions.json`):

1. **The forecast changed no decision on these step surges.** No ML-predictive scale-up was forecast-driven in any of the six runs: every one was triggered by the current arrival rate and backlog, at the same decision as ML-reactive would make. A sudden step has no precursor, so the forecaster could not anticipate it.
2. **Overload-recovery: later controller start.** Its surge begins at publication start, so every second before the controller's first decision is backlog. The predictive controller loads and parses its 285 KB forecaster at startup (2.7 s measured at 0.25 CPU in the same image). Its first decision, scale-up request and readiness came later than ML-reactive's in two of three repetitions, and its peak lag was higher in all three. This is a startup cost of the predictive controller, not a forecasting error.
3. **Pod start variance dominates the rest.** Three consumers became ready 4–8 s after the request in both modes; the spread between repetitions of one mode is as large as the gap between modes. On flash-sale the median p95 gap (4.04 vs 3.70 s) is inside the ranges of both modes.
4. **Online adaptation did not cause the gap.** Simulating the same evaluation seeds with adaptation on, off, forecast on and off gives identical results for both step-surge shapes: the scale-up is to the maximum in every variant.

## Follow-up: forecasting versus online adaptation (measured)

Because the main campaign cannot separate them, a follow-up ran the frozen protocol, model and `eval:` ramp seeds (three repetitions) with one switch changed: `forecast-only` (no learned bias, capacity or pod delay) and `adapt-only` (the forecast output ignored). `full` and ML-reactive were rerun in the same session as paired baselines. Follow-up campaigns are scratch evidence, audited like the main campaign; they do not replace its numbers.

| Variant (ramp) | Measured p95 (s) | Measured within 3 s | Peak lag | Replica-s |
| --- | ---: | ---: | ---: | ---: |
| reactive | 5.14 | 48% | 1108 | 167 |
| adapt-only | 4.71 | 50% | 1010 | 164 |
| forecast-only | 3.82 | 85% | 774 | 166 |
| full | 2.88 | 96% | 585 | 173 |

## Provenance

- Campaign: `campaign-20261028T225554-fc5dec` (2026-10-28T22:55:54.035503Z → 2026-10-29T03:22:06.258031Z), 75 measured runs, audit `passed`, 12,049 raw samples.
- Image `streamscale:fa2bb34b0d8ce0d1` from commit `a35190b41f5adc01208dfe52562e27313ea6bf16` (dirty: False).
- Forecaster sha256 `1c5304befb480e2412fe3168ff35ed1db1977b6e557702d739827b0e25edd491`, quantile 0.6, trained on `train:0..train:699`.
- campaign.json sha256 `a20696113c19ef73df2b9b25fd1ee24d9aeb5830a4fbaa72f53734aba0c756ef`; summary.csv sha256 `654e4f4ba1f172b6633294d9eebf8bb3c44ea11917e85b188417d091ce74c73e`.
- Follow-up `adapt-only`: `campaign-20261029T073028-06bee1`, 3 runs, audit `passed`, commit `5e9d55df4fd647d2cb4dab3355c46f721083f109`.
- Follow-up `forecast-only`: `campaign-20261029T071658-fe587a`, 3 runs, audit `passed`, commit `5e9d55df4fd647d2cb4dab3355c46f721083f109`.
- Follow-up `full`: `campaign-20261029T065244-666dcb`, 6 runs, audit `passed`, commit `5e9d55df4fd647d2cb4dab3355c46f721083f109`.

Reproduce (raw evidence is local under `artifacts/experiments/v3/`):

```sh
uv run --frozen python -m experiments.evidence --protocol v3        # re-audit
uv run --project experiments/analysis --frozen python -m experiments.analyze \
    --protocol v3 --output /tmp/m16-check                          # frozen report
uv run --frozen python -m experiments.supplement                    # this file
```
