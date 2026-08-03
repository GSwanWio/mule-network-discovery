"""Validate the controlled Step 8 live acceptance matrix."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from network_mule_discovery.live_acceptance_matrix import (
    LIVE_ACCEPTANCE_CASES,
    MAXIMUM_INITIAL_LIVE_CALLS,
    get_live_acceptance_case,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    NON_SEED_CUSTOMER_COUNT,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    LINKED_CUSTOMER_IDS,
)
from network_mule_discovery.synthetic_scenario_registry import (
    SUPPORTED_SYNTHETIC_SCENARIOS,
)


def main() -> None:
    """Validate scenario coverage and billable-call limits."""
    scenario_ids = tuple(
        case.scenario_id
        for case in LIVE_ACCEPTANCE_CASES
    )

    assert scenario_ids == (
        SUPPORTED_SYNTHETIC_SCENARIOS
    )
    assert len(set(scenario_ids)) == len(
        scenario_ids
    )

    call_caps = {
        case.scenario_id: case.max_live_calls
        for case in LIVE_ACCEPTANCE_CASES
    }

    assert call_caps == {
        "scenario_1": 11,
        "scenario_2": 1,
        "scenario_3": 2,
        "scenario_4": 0,
        "scenario_5": 1,
    }
    assert MAXIMUM_INITIAL_LIVE_CALLS == 15

    assert all(
        case.expected_unchanged_rerun_calls == 0
        for case in LIVE_ACCEPTANCE_CASES
    )
    assert all(
        case.expected_termination_reason
        == "FRONTIER_EXHAUSTED"
        for case in LIVE_ACCEPTANCE_CASES
    )

    scenario_1 = get_live_acceptance_case(
        "scenario_1"
    )
    assert scenario_1.expected_visible_max_depth == 4
    assert scenario_1.expected_raw_node_count == 108
    assert scenario_1.expected_raw_edge_count == 109

    scenario_2 = get_live_acceptance_case(
        "SCENARIO_2"
    )
    assert (
        scenario_2.minimum_collapsed_customer_count
        == NON_SEED_CUSTOMER_COUNT
    )
    assert scenario_2.required_decisions == (
        "COMMON_PUBLIC_SUPPRESS",
    )

    scenario_3 = get_live_acceptance_case(
        "scenario_3"
    )
    assert set(
        scenario_3.required_decisions
    ) == {
        "MULE_LIKE",
        "INSUFFICIENT_EVIDENCE",
    }

    scenario_4 = get_live_acceptance_case(
        "scenario_4"
    )
    assert scenario_4.max_live_calls == 0
    assert scenario_4.required_decisions == ()
    assert scenario_4.expected_group_count == 2

    scenario_5 = get_live_acceptance_case(
        "scenario_5"
    )
    assert (
        scenario_5.minimum_collapsed_customer_count
        == len(LINKED_CUSTOMER_IDS)
    )
    assert scenario_5.changed_evidence_supported
    assert (
        scenario_5
        .expected_changed_evidence_requeued_calls
        == 1
    )

    assert [
        case.scenario_id
        for case in LIVE_ACCEPTANCE_CASES
        if case.changed_evidence_supported
    ] == ["scenario_5"]

    try:
        get_live_acceptance_case(
            "scenario_6"
        )
    except ValueError:
        unsupported_rejected = True
    else:
        unsupported_rejected = False

    assert unsupported_rejected

    print(
        "Live synthetic acceptance matrix "
        "smoke test passed."
    )
    print("Supported scenarios: 5")
    print("Initial live-call cap: 15")
    print("Unchanged rerun call cap: 0")
    print("Changed-evidence scenario: scenario_5")
    print("Unsupported scenario rejected: passed")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
