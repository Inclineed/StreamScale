"""Scaling modes for console consumers, with single-controller ownership.

Modes: fixed (1 replica), manual (0–3), cpu-hpa (CPU HorizontalPodAutoscaler, 1–3),
lag-keda (KEDA Kafka lag ScaledObject, 1–3), and ml-reactive / ml-predictive (the
agent's autoscale component sets replicas, 1–3, with no cluster object). A manual
override suspends the
active autoscaler by removing it before writing replicas; releasing the override
recreates the selected controller. HPA and KEDA never target the Deployment at
the same time: every transition removes unwanted controllers, waits until they
and KEDA's generated HPA are gone, writes replicas, creates the wanted controller
and then verifies ownership. Transitions run in the background, are persisted,
and an interrupted one is reported as such on the next agent start.
"""

import json
import threading
import time

from streamscale.run_artifacts import utc_now, write_json
from streamscale_console_worker import control
from streamscale_console_worker.kube import (
    CONSUMER,
    CPU_HPA,
    KEDA_HPA,
    MAX_CONSOLE_REPLICAS,
    NAMESPACE,
    SCALED_OBJECT,
    KubeError,
)

MODES = ("fixed", "manual", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive")
# Kubernetes-object autoscalers; their controllers are created and verified here.
AUTOSCALED = ("cpu-hpa", "lag-keda")
# The agent's own autoscale component owns replicas in these modes (no cluster object).
AGENT_OWNED = ("ml-reactive", "ml-predictive")
# Every mode where replicas are controller-owned and a manual override applies.
CONTROLLED = AUTOSCALED + AGENT_OWNED
MODE_TEXT = "fixed, manual, cpu-hpa, lag-keda, ml-reactive or ml-predictive"
DEFAULT_SETTINGS = {
    "min": 1,
    "max": 3,
    "cpu_target": 60,
    "lag_threshold": 100,
    "scale_down_seconds": 60,
    "polling_interval": 2,
}
IDLE_GROUP = "streamscale.console.idle"
BOOTSTRAP = "streamscale-kafka.streamscale.svc.cluster.local:9092"


class ScalingError(ValueError):
    pass


def validate_settings(current, changes):
    settings = dict(current)
    limits = {
        "min": (1, MAX_CONSOLE_REPLICAS),
        "max": (1, MAX_CONSOLE_REPLICAS),
        "cpu_target": (10, 400),
        "lag_threshold": (5, 10_000),
        "scale_down_seconds": (0, 600),
        "polling_interval": (1, 30),
    }
    for key, value in (changes or {}).items():
        if key not in limits:
            raise ScalingError(f"Unknown scaling setting {key}")
        low, high = limits[key]
        if type(value) is not int or not low <= value <= high:
            raise ScalingError(f"{key} must be an integer from {low} to {high}")
        settings[key] = value
    if settings["min"] > settings["max"]:
        raise ScalingError("The minimum cannot exceed the maximum")
    return settings


def hpa_body(settings):
    return {
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": CPU_HPA,
            "namespace": NAMESPACE,
            "labels": {"app.kubernetes.io/managed-by": "streamscale-console"},
        },
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": CONSUMER},
            "minReplicas": settings["min"],
            "maxReplicas": settings["max"],
            "metrics": [
                {
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "target": {
                            "type": "Utilization",
                            "averageUtilization": settings["cpu_target"],
                        },
                    },
                }
            ],
            # Same behaviour as the measured CPU-HPA mode.
            "behavior": {
                "scaleUp": {
                    "stabilizationWindowSeconds": 0,
                    "policies": [{"type": "Pods", "value": 2, "periodSeconds": 15}],
                },
                "scaleDown": {
                    "stabilizationWindowSeconds": settings["scale_down_seconds"],
                    "policies": [{"type": "Pods", "value": 1, "periodSeconds": 15}],
                },
            },
        },
    }


def scaled_object_body(settings, group):
    return {
        "apiVersion": "keda.sh/v1alpha1",
        "kind": "ScaledObject",
        "metadata": {
            "name": SCALED_OBJECT,
            "namespace": NAMESPACE,
            "labels": {"app.kubernetes.io/managed-by": "streamscale-console"},
        },
        "spec": {
            "scaleTargetRef": {"name": CONSUMER},
            "minReplicaCount": settings["min"],
            "maxReplicaCount": settings["max"],
            "pollingInterval": settings["polling_interval"],
            "advanced": {
                "horizontalPodAutoscalerConfig": {
                    "behavior": {
                        "scaleUp": {
                            "stabilizationWindowSeconds": 0,
                            "policies": [{"type": "Pods", "value": 2, "periodSeconds": 15}],
                        },
                        "scaleDown": {
                            "stabilizationWindowSeconds": settings["scale_down_seconds"],
                            "policies": [{"type": "Pods", "value": 1, "periodSeconds": 15}],
                        },
                    }
                }
            },
            # Same trigger as the measured lag-KEDA mode, following the console's run group.
            "triggers": [
                {
                    "type": "kafka",
                    "useCachedMetrics": True,
                    "metadata": {
                        "bootstrapServers": BOOTSTRAP,
                        "consumerGroup": group,
                        "topic": "orders.events.v1",
                        "lagThreshold": str(settings["lag_threshold"]),
                        "activationLagThreshold": "0",
                        "offsetResetPolicy": "latest",
                        "allowIdleConsumers": "false",
                        "excludePersistentLag": "false",
                    },
                }
            ],
        },
    }


def expected_controllers(mode, override):
    if override or mode not in AUTOSCALED:
        return set()
    return {"hpa"} if mode == "cpu-hpa" else {"keda"}


def owners(hpas, scaled):
    """Which controller kinds currently target the console consumer Deployment."""
    found = set()
    for row in hpas:
        name = row["metadata"]["name"]
        if name == CPU_HPA:
            found.add("hpa")
        elif name == KEDA_HPA:
            found.add("keda-hpa")
        else:
            found.add("other:" + name)
    for row in scaled:
        if row["metadata"]["name"] == SCALED_OBJECT:
            found.add("keda")
        else:
            found.add("other:" + row["metadata"]["name"])
    return found


def ownership_ok(expected, found):
    if expected == {"keda"}:
        # KEDA owns its generated HPA; that is one controller, not two.
        return found in ({"keda"}, {"keda", "keda-hpa"})
    return found == expected


class Scaling:
    def __init__(self, agent, root=control.ROOT, sleep=time.sleep):
        self.agent = agent
        self.kube = agent.kube
        self.root = root
        self.sleep = sleep
        self.lock = threading.Lock()
        self.work = threading.Lock()
        self.path = root / "control" / "scaling.json"
        self.state = self.load()
        self.commands = {
            "scaling.mode": self.set_mode,
            "scaling.replicas": self.set_replicas,
            "scaling.override": self.enter_override,
            "scaling.release": self.release_override,
            "scaling.settings": self.set_settings,
            "scaling.reapply": lambda body: self.start({}, "Reapply the recorded mode"),
        }
        self.sections = {"scaling": self.section}
        self.runners = [self.startup]
        self.busy = [lambda: self.state["transition"]["state"] == "running"]
        self.group = IDLE_GROUP
        sale = agent.components.get("sale")
        if sale is not None:
            sale.listeners.append(self.follow_sale)

    # Persistence ---------------------------------------------------------------

    def load(self):
        document = control.read_json(self.path)
        if document:
            if document.get("transition", {}).get("state") == "running":
                document["transition"] = {
                    "state": "interrupted",
                    "step": document["transition"].get("step"),
                    "error": "The agent restarted during this transition; reapply to finish it",
                    "at": utc_now(),
                }
            return document
        return {
            "mode": None,
            "override": None,
            "manual_replicas": 1,
            "settings": dict(DEFAULT_SETTINGS),
            "segments": [],
            "transition": {"state": "idle", "step": None, "error": None, "at": None},
            "history": [],
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock:
            snapshot = json.loads(json.dumps(self.state))
        write_json(self.path, snapshot)

    def startup(self, stop):
        """Adopt the cluster's actual controller state the first time the agent runs."""
        if self.state["mode"] is not None:
            self.save()
            return
        try:
            found = owners(*self.kube.autoscalers())
            replicas = self.kube.consumer_deployment()["spec"].get("replicas", 1)
        except Exception:  # noqa: BLE001 - retried on the first command
            return
        if "keda" in found:
            mode = "lag-keda"
        elif "hpa" in found:
            mode = "cpu-hpa"
        else:
            mode = "fixed" if replicas == 1 else "manual"
        with self.lock:
            self.state["mode"] = mode
            self.state["manual_replicas"] = replicas
            self.state["segments"] = [
                {"mode": mode, "override": False, "from": time.time(), "to": None}
            ]
        self.save()

    # Commands ------------------------------------------------------------------

    def set_mode(self, body):
        mode = body.get("mode")
        if mode not in MODES:
            raise ScalingError(f"Mode must be {MODE_TEXT}")
        changes = {"mode": mode, "override": None}
        if mode == "manual":
            replicas = body.get("replicas", self.state.get("manual_replicas", 1))
            changes["manual_replicas"] = self._replicas(replicas)
        if "settings" in body:
            changes["settings"] = validate_settings(self.state["settings"], body["settings"])
        return self.start(changes, f"Switch to {mode}")

    def set_replicas(self, body):
        replicas = self._replicas(body.get("replicas"))
        if self.state["override"]:
            return self.start(
                {"override": self.state["override"] | {"replicas": replicas}},
                f"Override at {replicas}",
            )
        if self.state["mode"] != "manual":
            raise ScalingError(
                "Replicas are controller-owned in this mode; switch to manual or enter an override"
            )
        return self.start({"manual_replicas": replicas}, f"Set {replicas} manual replicas")

    def enter_override(self, body):
        if self.state["mode"] not in CONTROLLED:
            raise ScalingError("Override suspends an autoscaler; use manual mode directly")
        replicas = self._replicas(body.get("replicas", 3))
        override = {"replicas": replicas, "since": time.time(), "base": self.state["mode"]}
        return self.start({"override": override}, f"Manual override at {replicas}")

    def release_override(self, body):
        if not self.state["override"]:
            raise ScalingError("No manual override is active")
        return self.start({"override": None}, f"Release override; restore {self.state['mode']}")

    def set_settings(self, body):
        settings = validate_settings(self.state["settings"], body.get("settings"))
        return self.start({"settings": settings}, "Change scaling settings")

    def _replicas(self, value):
        if type(value) is not int or not 0 <= value <= MAX_CONSOLE_REPLICAS:
            raise ScalingError("Replicas must be an integer from 0 to 3")
        return value

    # Transitions ---------------------------------------------------------------

    def start(self, changes, label):
        if not self.work.acquire(blocking=False):
            raise ScalingError("Another scaling transition is in progress")
        with self.lock:
            target = json.loads(json.dumps(self.state)) | changes
            self.state["transition"] = {
                "state": "running",
                "label": label,
                "step": "planning",
                "error": None,
                "at": utc_now(),
                "target": {key: target[key] for key in ("mode", "override", "manual_replicas")},
            }
        self.save()
        threading.Thread(target=self._run, args=(target, label), daemon=True).start()
        return {"transition": label}

    def _step(self, text):
        with self.lock:
            self.state["transition"]["step"] = text
        self.save()

    def _run(self, target, label):
        try:
            self.apply(target)
            with self.lock:
                previous = (self.state["mode"], bool(self.state["override"]))
                for key in ("mode", "override", "manual_replicas", "settings"):
                    self.state[key] = target[key]
                current = (self.state["mode"], bool(self.state["override"]))
                if current != previous:
                    now = time.time()
                    if self.state["segments"]:
                        self.state["segments"][-1]["to"] = now
                    self.state["segments"].append(
                        {"mode": current[0], "override": current[1], "from": now, "to": None}
                    )
                    self.state["segments"] = self.state["segments"][-200:]
                self.state["history"] = (
                    self.state["history"] + [{"at": time.time(), "label": label}]
                )[-100:]
                self.state["transition"] = {
                    "state": "succeeded",
                    "label": label,
                    "step": "verified",
                    "error": None,
                    "at": utc_now(),
                }
        except Exception as error:  # noqa: BLE001 - surfaced; never reported as success
            with self.lock:
                self.state["transition"] |= {
                    "state": "failed",
                    "error": f"{type(error).__name__}: {str(error)[:240]}",
                    "at": utc_now(),
                }
        finally:
            self.save()
            self.work.release()

    def apply(self, target):
        mode, override = target["mode"], target["override"]
        settings = target["settings"]
        wanted = expected_controllers(mode, override)
        self._step("removing controllers that must not own the Deployment")
        if "hpa" not in wanted:
            self.kube.delete_cpu_hpa()
        if "keda" not in wanted:
            self.kube.delete_scaled_object()
        self._wait(
            lambda found: not (found - {"hpa", "keda", "keda-hpa"}) and _subset(found, wanted)
        )
        if override:
            replicas = override["replicas"]
        elif mode == "fixed":
            replicas = 1
        elif mode == "manual":
            replicas = target["manual_replicas"]
        else:
            current = self.kube.consumer_deployment()["spec"].get("replicas", 1)
            replicas = min(max(current, settings["min"]), settings["max"])
        self._step(f"setting {replicas} desired replica(s)")
        self.kube.scale_consumers(replicas)
        if wanted == {"hpa"}:
            self._step("creating the CPU HorizontalPodAutoscaler")
            self.kube.apply_cpu_hpa(hpa_body(settings))
        elif wanted == {"keda"}:
            self._step("creating the KEDA lag ScaledObject")
            self.kube.apply_scaled_object(scaled_object_body(settings, self.current_group()))
        self._step("verifying single-controller ownership")
        found = self._wait(lambda found: ownership_ok(wanted, found), timeout=60)
        if not ownership_ok(wanted, found):
            raise RuntimeError(
                f"Controller ownership is {sorted(found)}, expected {sorted(wanted)}"
            )

    def _wait(self, predicate, timeout=90):
        deadline = time.monotonic() + timeout
        while True:
            found = owners(*self.kube.autoscalers())
            if predicate(found):
                return found
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Timed out waiting for controllers; still present: {sorted(found)}"
                )
            self.sleep(1)

    def current_group(self):
        try:
            pointer = control.current_run(self.root)
            if pointer:
                run = control.read_json(self.root / "runs" / pointer["run_name"] / "run.json")
                if run and run.get("group_id"):
                    return run["group_id"]
        except (OSError, ValueError):
            pass
        return IDLE_GROUP

    def follow_sale(self, sale):
        """A new sale has a new group; point the lag scaler at it before consumers join."""
        if self.state["mode"] != "lag-keda" or self.state["override"]:
            return
        group = sale.run["group_id"]
        try:
            self.kube.apply_scaled_object(scaled_object_body(self.state["settings"], group))
        except KubeError as error:
            with self.lock:
                self.state["transition"] |= {"error": f"KEDA group update failed: {error}"}
            self.save()

    # Telemetry -----------------------------------------------------------------

    def section(self):
        with self.lock:
            state = json.loads(json.dumps(self.state))
        try:
            found = owners(*self.kube.autoscalers())
            wanted = expected_controllers(state["mode"], state["override"])
            state["ownership"] = {
                "found": sorted(found),
                "expected": sorted(wanted),
                "consistent": ownership_ok(wanted, found),
            }
        except Exception as error:  # noqa: BLE001
            state["ownership"] = {"error": type(error).__name__}
        state["override"] = bool(state["override"]) and state["override"]
        return state


def _subset(found, wanted):
    reduced = {"keda" if name == "keda-hpa" else name for name in found}
    return reduced <= wanted


def component(agent):
    return Scaling(agent)
