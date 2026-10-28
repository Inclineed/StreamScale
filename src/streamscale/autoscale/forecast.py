"""Plain-Python evaluation of exported scikit-learn gradient-boosted quantile trees.

Each window model predicts the 90th-percentile peak arrival rate (in consumer capacities)
over a future window. scikit-learn evaluates trees on float32 features, so inputs are
rounded to float32 here too; predictions then match scikit-learn to floating-point noise.
"""

import hashlib
import json
import struct
from importlib.resources import files
from pathlib import Path

from streamscale.autoscale.features import FEATURES

FORMAT = "streamscale-gbt-quantile-v1"
DEFAULT_MODEL = "forecaster-v1.json"


def f32(value: float) -> float:
    return struct.unpack("f", struct.pack("f", value))[0]


class Tree:
    __slots__ = ("feature", "threshold", "left", "right", "value")

    def __init__(self, data):
        self.feature = data["feature"]
        self.threshold = data["threshold"]
        self.left = data["left"]
        self.right = data["right"]
        self.value = data["value"]
        nodes = len(self.feature)
        if not all(len(data[key]) == nodes for key in ("threshold", "left", "right", "value")):
            raise ValueError("Tree arrays have inconsistent lengths")

    def predict(self, row):
        node = 0
        while self.left[node] != -1:
            if row[self.feature[node]] <= self.threshold[node]:
                node = self.left[node]
            else:
                node = self.right[node]
        return self.value[node]


class WindowModel:
    def __init__(self, data):
        self.start, self.end = data["window"]
        self.init = float(data["init"])
        self.learning_rate = float(data["learning_rate"])
        self.trees = [Tree(tree) for tree in data["trees"]]

    def predict(self, row):
        return self.init + self.learning_rate * sum(tree.predict(row) for tree in self.trees)


class Forecaster:
    """Quantile forecasts of peak arrivals for each future window, in records/second."""

    def __init__(self, payload: dict, sha256: str):
        if payload.get("format") != FORMAT:
            raise ValueError(f"Unsupported forecaster format {payload.get('format')!r}")
        if tuple(payload.get("features", ())) != FEATURES:
            raise ValueError("Forecaster feature order does not match this controller")
        self.metadata = {key: value for key, value in payload.items() if key != "windows"}
        self.sha256 = sha256
        self.quantile = float(payload["quantile"])
        self.windows = [WindowModel(window) for window in payload["windows"]]
        if not self.windows:
            raise ValueError("Forecaster has no windows")

    @property
    def horizons(self):
        return [(model.start, model.end) for model in self.windows]

    def predict(self, features, capacity):
        """Per-window peak arrival rates (records/s) for one feature vector."""
        if len(features) != len(FEATURES):
            raise ValueError("Feature vector length does not match the model")
        row = [f32(value) for value in features]
        return [max(0.0, model.predict(row)) * capacity for model in self.windows]


def load(path: Path | None = None) -> Forecaster:
    """Load a model file; the default is the one packaged with this image."""
    raw = (
        path.read_bytes()
        if path is not None
        else (files("streamscale.autoscale") / "models" / DEFAULT_MODEL).read_bytes()
    )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("Forecaster file is not valid JSON") from error
    return Forecaster(payload, hashlib.sha256(raw).hexdigest())
