"""One-second control loop for the ml-predictive and ml-reactive scaling modes.

Inputs are the run exporter's Kafka metrics (high watermarks, lag, freshness) and the
consumer Deployment's desired/ready counts. The controller has no access to the run's
schedule: it mounts neither the runs volume nor the run ConfigMap.
"""

import json
import time
from collections import deque
from threading import Event, Lock
from urllib.request import urlopen

from prometheus_client import Counter, Gauge, Info

from streamscale.autoscale.observation import Observation, exporter_sample
from streamscale.autoscale.planner import PlannerConfig
from streamscale.autoscale.policy import build_policy
from streamscale.metrics import MetricsServer, registry

KEEP_DECISIONS = 900


def fetch_text(url, timeout=3):
    with urlopen(url, timeout=timeout) as response:
        return response.read().decode()


class Controller:
    """Testable core of the loop: one ``tick`` reads, decides and (maybe) scales."""

    def __init__(
        self,
        policy_name,
        config: PlannerConfig,
        scaler,
        exporter_url,
        forecaster=None,
        variant="full",
    ):
        self.policy_name = policy_name
        self.config = config
        self.scaler = scaler
        self.exporter_url = exporter_url
        self.forecaster = forecaster
        self.variant = variant
        self.policy = None
        self.run_id = None
        self.decisions = deque(maxlen=KEEP_DECISIONS)
        self.lock = Lock()

    def reset(self, run_id):
        self.run_id = run_id
        self.policy = build_policy(self.policy_name, self.config, self.forecaster, self.variant)

    def tick(self, now=None, fetch=fetch_text):
        sample = exporter_sample(fetch(self.exporter_url))
        if sample.run_id != self.run_id or self.policy is None:
            self.reset(sample.run_id)
        desired, ready = self.scaler.status()
        observation = Observation(
            at=time.time() if now is None else now,
            highs=sample.highs,
            lags=sample.lags,
            ready=ready,
            desired=desired,
            fresh_at=sample.success,
            members=sample.members,
        )
        decision = self.policy.step(observation)
        applied = False
        if decision.replicas != desired:
            self.scaler.scale(decision.replicas)
            applied = True
        record = decision.to_dict() | {
            "run_id": sample.run_id,
            "ready": ready,
            "lag": observation.lag,
            "applied": applied,
        }
        with self.lock:
            self.decisions.append(record)
        return record

    def recent(self):
        with self.lock:
            return list(self.decisions)

    def describe(self):
        policy = self.policy.describe() if self.policy else {"policy": self.policy_name}
        return policy | {"run_id": self.run_id}


def serve_controller(controller: Controller, port, stop: Event, interval=1.0):
    collection = registry()
    labels = ("policy",)
    policy = controller.policy_name
    desired = Gauge(
        "streamscale_autoscaler_desired_replicas", "Chosen replicas", labels, registry=collection
    ).labels(policy)
    arrival = Gauge(
        "streamscale_autoscaler_arrival_rate", "Observed arrivals/s", labels, registry=collection
    ).labels(policy)
    forecast = Gauge(
        "streamscale_autoscaler_forecast_rate",
        "Planned peak arrivals/s over the horizon",
        labels,
        registry=collection,
    ).labels(policy)
    delay = Gauge(
        "streamscale_autoscaler_predicted_delay_seconds",
        "Predicted worst queueing delay for each candidate count",
        (*labels, "replicas"),
        registry=collection,
    )
    decisions = Counter(
        "streamscale_autoscaler_decisions_total",
        "Control decisions by result",
        (*labels, "result"),
        registry=collection,
    )
    errors = Counter(
        "streamscale_autoscaler_errors_total", "Failed control ticks", labels, registry=collection
    ).labels(policy)
    success = Gauge(
        "streamscale_autoscaler_last_success_unixtime",
        "Last completed decision",
        labels,
        registry=collection,
    ).labels(policy)
    info = Info("streamscale_autoscaler", "Controller identity", registry=collection)
    for result in ("hold", "scale_up", "scale_down"):
        decisions.labels(policy, result)

    def recent():
        return json.dumps(
            {"controller": controller.describe(), "decisions": controller.recent()}
        ).encode(), "application/json"

    server = MetricsServer(collection, port, routes={"/decisions": recent})
    try:
        while not stop.is_set():
            started = time.monotonic()
            try:
                record = controller.tick()
                desired.set(record["replicas"])
                if record["arrival_rate"] is not None:
                    arrival.set(record["arrival_rate"])
                if record["forecast_rate"] is not None:
                    forecast.set(record["forecast_rate"])
                for count, value in record["predicted_delay"].items():
                    delay.labels(policy, count).set(value)
                if record["replicas"] > record["previous"]:
                    decisions.labels(policy, "scale_up").inc()
                elif record["replicas"] < record["previous"]:
                    decisions.labels(policy, "scale_down").inc()
                else:
                    decisions.labels(policy, "hold").inc()
                described = controller.describe()
                info.info(
                    {
                        "policy": policy,
                        "run_id": str(described.get("run_id")),
                        "model_sha256": str(described.get("model_sha256", "")),
                        "variant": str(described.get("variant", "full")),
                    }
                )
                success.set(time.time())
                server.ready.set()
            except Exception as error:
                # Never log payloads or credentials; hold replicas by not acting.
                print(f"Autoscaler tick failed: {type(error).__name__}: {error}", flush=True)
                errors.inc()
            stop.wait(max(0.0, interval - (time.monotonic() - started)))
        return {"status": "stopped", "policy": policy}
    finally:
        server.close()
