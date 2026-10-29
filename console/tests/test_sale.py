"""Sale engine: configuration limits, seeded fault mix, pacing and reconciliation maths."""

from uuid import UUID

import pytest
from streamscale_console.sales import comparable, presets
from streamscale_console_worker.reconcile import percentile
from streamscale_console_worker.sale import (
    LATE_DELAY_US,
    Pacer,
    SaleError,
    console_workload,
    mode_label,
    validate_config,
)

RUN = UUID("12345678-1234-5678-1234-567812345678")


def config(**overrides):
    base = {
        "scenario": "flash-sale",
        "seed": 202,
        "schedule": [[15, 52], [20, 390], [25, 52]],
        "faults": {"malformed_pct": 1, "duplicate_pct": 2, "late_pct": 5},
        "mode": "fixed",
    }
    return validate_config(base | overrides)


def test_flash_sale_preset_matches_the_measured_logical_workload():
    valid = config()
    assert valid["orders"] == 4940
    attempts = console_workload(valid, RUN)
    base = 9880
    assert len(attempts) == base + round(base * 0.02)
    assert sum(a["fault"].startswith("invalid_") for a in attempts) == round(base * 0.01)
    # Duplicates copy their source, so duplicates of late events are late too.
    late = sum(a["fault"] == "late" and not a["duplicate"] for a in attempts)
    assert late == round(base * 0.05)
    assert [a["attempt_index"] for a in attempts] == list(range(len(attempts)))
    scheduled = [a["scheduled_at_us"] for a in attempts]
    assert scheduled == sorted(scheduled)


def test_same_seed_same_logical_sale_and_fresh_event_ids_per_run():
    first = console_workload(config(), RUN)
    second = console_workload(config(), UUID(int=RUN.int + 1))
    strip = [{k: v for k, v in a.items() if k != "event_id"} for a in first]
    assert strip == [{k: v for k, v in a.items() if k != "event_id"} for a in second]
    assert {a["event_id"] for a in first}.isdisjoint({a["event_id"] for a in second})


def test_late_events_are_published_later_but_keep_their_business_time():
    attempts = console_workload(config(), RUN)
    late = [a for a in attempts if a["fault"] == "late" and not a["duplicate"]]
    assert late and all(a["scheduled_at_us"] - a["event_time_us"] == LATE_DELAY_US for a in late)


def test_duplicates_repeat_an_existing_valid_event_id():
    attempts = console_workload(config(), RUN)
    originals = {a["event_id"] for a in attempts if not a["duplicate"]}
    duplicates = [a for a in attempts if a["duplicate"]]
    assert duplicates and all(a["event_id"] in originals for a in duplicates)
    assert not any(a["fault"].startswith("invalid_") for a in duplicates)


@pytest.mark.parametrize(
    "overrides",
    [
        {"schedule": []},
        {"schedule": [[10, 2001]]},
        {"schedule": [[3601, 1]]},
        {"schedule": [[1, 1]]},
        {"schedule": [[1000, 1000]]},
        {"schedule": [["10", 5]]},
        {"faults": {"malformed_pct": 21}},
        {"faults": {"unknown_pct": 1}},
        {"seed": -1},
        {"scenario": "black-friday"},
        {"mode": "predictive"},
    ],
)
def test_out_of_range_sales_are_refused(overrides):
    with pytest.raises(SaleError):
        config(**overrides)


def test_manual_sales_are_recorded_with_the_fixed_measured_label():
    assert mode_label("manual") == "fixed"
    assert mode_label("lag-keda") == "lag-keda"


def schedule(count, spacing_us):
    return [{"scheduled_at_us": index * spacing_us} for index in range(count)]


def test_pacer_follows_the_schedule_and_pause_freezes_it():
    pacer = Pacer(schedule(100, 100_000))  # 10 per second
    assert pacer.due(0, 0.0, False) == 1
    assert pacer.due(1, 1.0, False) == 11
    # Paused for five seconds: nothing becomes due and the clock does not advance.
    assert pacer.due(11, 6.0, True) == 11
    assert pacer.due(11, 6.5, False) == 16


def test_pacer_live_rate_replaces_and_returns_to_the_schedule():
    pacer = Pacer(schedule(10_000, 1_000_000))  # 1 per second
    pacer.due(0, 0.0, False)
    pacer.set_rate(200, 1)
    assert pacer.due(1, 0.5, False) == 1 + 100
    assert pacer.current_rate(101) == 200
    pacer.set_rate(None, 101)
    # The schedule resumes from the cursor's own scheduled time, not from zero.
    assert pacer.virtual_us == 101 * 1_000_000
    assert pacer.due(101, 1.0, False) == 102


def test_pacer_never_bursts_more_than_one_second_of_backlog():
    pacer = Pacer(schedule(10_000, 1))
    pacer.set_rate(100, 0)
    pacer.due(0, 0.0, False)
    assert pacer.due(0, 30.0, False) == 100


def test_exact_percentile_is_nearest_rank():
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.95) == 10
    assert percentile([5.0], 0.5) == 5.0
    assert percentile([], 0.95) is None


def test_only_complete_unmodified_reconciled_sales_are_comparable():
    passed = {"status": "passed", "outcome": "completed"}
    assert comparable({}, passed)
    assert not comparable({"controls_changed": True}, passed)
    assert not comparable({"segments": [{"mode": "manual"}]}, passed)
    assert not comparable({"faults": [{"kind": "restart"}]}, passed)
    assert not comparable({}, passed | {"outcome": "stopped"})
    assert not comparable({}, passed | {"status": "failed"})


def test_presets_reuse_the_frozen_measured_rates(tmp_path):
    catalog = presets()
    flash = next(row for row in catalog["presets"] if row["id"] == "flash-sale")
    assert flash["schedule"] == [[15, 52], [20, 390], [25, 52]] and flash["seed"] == 202
    assert catalog["rates"] == {"steady": 52, "surge": 390}
    missing = presets(tmp_path / "absent.json")
    assert missing["presets"][0]["schedule"] == flash["schedule"]


def test_forecastable_presets_are_valid_v3_sales():
    from streamscale_console_worker.sale import validate_config

    catalog = {row["id"]: row for row in presets()["presets"]}
    for identifier in ("ramp", "repeated-sales"):
        row = catalog[identifier]
        accepted = validate_config(
            {
                "scenario": row["scenario"],
                "seed": row["seed"],
                "schedule": row["schedule"],
                "faults": row["faults"],
                "mode": "ml-predictive",
                "preset": identifier,
            }
        )
        assert accepted["scenario"] == identifier and accepted["mode"] == "ml-predictive"
    assert len(catalog["repeated-sales"]["schedule"]) <= 12


def test_only_broker_connectivity_errors_are_retried():
    from streamscale_console.sales import transient

    down = 'KafkaException: KafkaError{code=_ALL_BROKERS_DOWN,val=-187,str="..."}'
    assert transient(down)
    assert not transient("missing acknowledged events")
    assert not transient(None)
