import pytest
from streamscale_workload.generator import validate_schedule
from streamscale_workload.shapes import (
    RANGES,
    SHAPES,
    parse_seed,
    require_namespace,
    sale_starts,
    schedule,
)


@pytest.mark.parametrize("shape", SHAPES)
def test_shapes_are_deterministic_valid_and_even(shape):
    first = schedule(shape, "eval:1", 52, 390)
    assert first == schedule(shape, "eval:1", 52, 390)
    validate_schedule(first)
    assert all(rate % 2 == 0 for _, rate in first)


def test_v2_shapes_match_the_frozen_m7_protocol():
    assert schedule("flash-sale", "train:3", 52, 390) == [[15, 52], [20, 390], [25, 52]]
    assert schedule("overload-recovery", "eval:0", 52, 390) == [[35, 390], [25, 52]]
    assert schedule("steady", "eval:0", 52, 390) == [[60, 52]]


def test_seeds_change_new_shapes_and_namespaces_are_separate():
    assert schedule("repeated-sales", "train:1", 52, 390) != schedule(
        "repeated-sales", "train:2", 52, 390
    )
    assert schedule("ramp", "train:1", 52, 390) != schedule("ramp", "eval:1", 52, 390)


def test_repeated_sales_respect_normal_ranges():
    ranges = RANGES["normal"]
    for index in range(30):
        phases = schedule("repeated-sales", f"train:{index}", 52, 390)
        starts = sale_starts(phases, 52)
        assert ranges["sale_count"][0] <= len(starts) <= ranges["sale_count"][1]
        peaks = [rate for _, rate in phases if rate > 52]
        assert all(0.7 * 390 - 2 <= rate <= 390 for rate in peaks)
        assert 120 <= sum(seconds for seconds, _ in phases) <= 260


def test_ramp_climbs_gradually_to_a_peak():
    phases = schedule("ramp", "train:5", 52, 390)
    rates = [rate for seconds, rate in phases for _ in range(seconds)]
    peak = max(rates)
    climb = rates[15 : rates.index(peak)]
    assert len(climb) >= 10
    assert climb == sorted(climb)
    assert 0.8 * 390 - 2 <= peak <= 390


def test_seed_namespaces_are_enforced():
    assert parse_seed("stress:4") == ("stress", 4)
    with pytest.raises(ValueError, match="train:N"):
        parse_seed("42")
    with pytest.raises(ValueError, match="not a train seed"):
        require_namespace("eval:1", "train")
    with pytest.raises(ValueError, match="Unknown shape"):
        schedule("blackfriday", "train:1", 52, 390)
