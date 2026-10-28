"""Seeded publishing schedules for protocol v3, shared by freeze(), training and simulation.

Seeds are namespaced strings. ``train:N`` seeds feed model training and simulator tuning;
``eval:N`` seeds are reserved for measured evaluation and ``stress:N`` for out-of-range
offline checks. Training code calls ``require_namespace(seed, "train")`` so an evaluation
shape can never leak into a model.
"""

import random

SHAPES = ("steady", "flash-sale", "overload-recovery", "ramp", "repeated-sales")
NAMESPACES = ("train", "eval", "stress")
SHAPE_VERSION = 1

# Fractions of the frozen surge rate and durations in seconds, (low, high) inclusive.
RANGES = {
    "normal": {
        "ramp_climb": (15, 25),
        "ramp_peak": (0.8, 1.0),
        "sale_count": (3, 4),
        "sale_seconds": (12, 20),
        "sale_peak": (0.7, 1.0),
        "gap_seconds": (25, 40),
    },
    # Deliberately outside the training ranges, for offline robustness checks only.
    "stress": {
        "ramp_climb": (6, 40),
        "ramp_peak": (0.5, 1.2),
        "sale_count": (2, 6),
        "sale_seconds": (6, 30),
        "sale_peak": (0.5, 1.2),
        "gap_seconds": (12, 70),
    },
}
LEAD_SECONDS = 15
HOLD_SECONDS = 10
TAIL_SECONDS = 25


def parse_seed(seed: str):
    namespace, _, number = str(seed).partition(":")
    if namespace not in NAMESPACES or not number.isdigit():
        raise ValueError(f"Shape seeds look like train:N, eval:N or stress:N, not {seed!r}")
    return namespace, int(number)


def require_namespace(seed: str, namespace: str):
    found, _ = parse_seed(seed)
    if found != namespace:
        raise ValueError(f"Seed {seed!r} is not a {namespace} seed")
    return seed


def even(value: float) -> int:
    """Rates stay even so every schedule has an even (order-pair) event total."""
    return max(2, int(round(value / 2)) * 2)


def _merge(phases):
    merged = []
    for seconds, rate in phases:
        if seconds <= 0:
            continue
        if merged and merged[-1][1] == rate:
            merged[-1][0] += seconds
        else:
            merged.append([seconds, rate])
    return merged


def schedule(name: str, seed: str, steady: int, surge: int):
    """Return ``[[seconds, events/second], ...]`` for one shape and namespaced seed."""
    if name not in SHAPES:
        raise ValueError(f"Unknown shape {name!r}")
    if not 0 < steady < surge:
        raise ValueError("Rates must satisfy 0 < steady < surge")
    namespace, _ = parse_seed(seed)
    ranges = RANGES["stress" if namespace == "stress" else "normal"]
    rng = random.Random(f"shape-v{SHAPE_VERSION}:{name}:{seed}")
    steady, surge = even(steady), even(surge)
    if name == "steady":
        return [[60, steady]]
    if name == "flash-sale":
        return [[15, steady], [20, surge], [25, steady]]
    if name == "overload-recovery":
        return [[35, surge], [25, steady]]
    if name == "ramp":
        climb = rng.randint(*ranges["ramp_climb"])
        peak = even(surge * rng.uniform(*ranges["ramp_peak"]))
        steps = [even(steady + (peak - steady) * (index + 1) / climb) for index in range(climb)]
        return _merge(
            [[LEAD_SECONDS, steady]]
            + [[1, rate] for rate in steps]
            + [[HOLD_SECONDS, peak], [TAIL_SECONDS, steady]]
        )
    phases = [[LEAD_SECONDS, steady]]
    count = rng.randint(*ranges["sale_count"])
    for index in range(count):
        if index:
            phases.append([rng.randint(*ranges["gap_seconds"]), steady])
        phases.append(
            [rng.randint(*ranges["sale_seconds"]), even(surge * rng.uniform(*ranges["sale_peak"]))]
        )
    phases.append([TAIL_SECONDS, steady])
    return _merge(phases)


def sale_starts(phases, steady: int):
    """Seconds (from publication start) at which the rate rises above ``steady``."""
    starts, elapsed, previous = [], 0, None
    for seconds, rate in phases:
        if rate > steady and (previous is None or previous <= steady):
            starts.append(elapsed)
        previous = rate
        elapsed += seconds
    return starts
