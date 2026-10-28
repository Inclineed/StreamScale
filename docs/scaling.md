# Scaling modes (M6, M16)

M16 adds `ml-reactive` and `ml-predictive`: a one-second controller Deployment
sets the consumer's replicas (1–3) directly through the `scale` subresource, with
no HPA or ScaledObject. See [ML predictive autoscaling](ml-autoscaling.md) for the
model, its RBAC and isolation checks. `./streamscale.sh scaling` exercises all
five modes and retains each ML run's `autoscaler-decisions.json`.

```sh
uv run --frozen python deploy/k8s/manage.py up --mode ml-predictive --orders 3000
```

All modes use the same image, three input partitions and consumer resources:
100m CPU / 128Mi requests, 500m CPU / 256Mi limits. Fixed starts one replica;
CPU-HPA and lag-KEDA start one and have an enforced 1–3 target range. A temporary
zero ready count during rollout/rebalance is recorded, rather than confused
with the configured minimum. The singleton exporter never joins the group.

```sh
./streamscale.sh scaling
# Or deploy an individual mode (paced publication is opt-in):
uv run --frozen python deploy/k8s/manage.py up --mode cpu-hpa --orders 3000
uv run --frozen python deploy/k8s/manage.py up --mode lag-keda --orders 3000
```

The chart renders exactly one alternative controller: an autoscaling/v2 CPU
HPA at 60% of the CPU request, or a KEDA Kafka ScaledObject with lag threshold
100. KEDA owns its generated HPA; it is not a second CPU controller. Targets
are limited to three replicas. The generator and Kafka scaler use the same
unique `streamscale.run.<run-name>` group, which starts after retained records.
Mode switches remove the old controller, wait for its HPA to disappear, and
reset the drained Deployment to one replica. Autoscaled consumer manifests omit
`spec.replicas`; Helm uses client-side updates to avoid stale replica ownership
conflicts when returning to fixed mode. Kafka advertises a namespace-qualified
address so KEDA in its separate namespace can resolve both bootstrap and broker.
Finish and drain a measured execution before switching.

The exercise publishes a fresh correctness workload in each mode, samples
controller state and replicas, reconciles SQL/DLQ, and requires both autoscalers
to add a ready consumer under backlog. Evidence lives in
`artifacts/k8s/<run-name>/scaling-samples.jsonl` and `scaling-verification.json`;
the combined result is `artifacts/k8s/scaling-verification.json`. It ends with a
fresh, verified fixed demo so the regular dashboard check remains reproducible.

Metrics Server 0.9.0 is vendored from the checksum-verified official manifest.
KEDA 2.21.0's chart is downloaded into the ignored `.tools/charts` directory and
verified against its pinned official index digest. All four controller images
are also pinned to their multi-architecture manifest digests. Installation always uses
the dedicated kind-streamscale context. Metrics Server uses kubelet insecure
TLS solely for this local kind cluster's self-signed kubelet certificate.
Resource and external metrics API availability are required before exercises start.

Sources: [Metrics Server](https://github.com/kubernetes-sigs/metrics-server),
[KEDA Kafka scaler](https://keda.sh/docs/2.21/scalers/apache-kafka/),
[KEDA ScaledObject](https://keda.sh/docs/2.21/reference/scaledobject-spec/).
