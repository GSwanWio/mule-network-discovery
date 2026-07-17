"""Smoke test for decision reuse and expansion queuing."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.decision_runner import (
    run_decision_projection,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the demo data source."""
    return CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_pool.csv"
        ),
        customer_identity_path=(
            PROJECT_ROOT
            / "data/demo/customer_identity.csv"
        ),
        seed_mule_events_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_events.csv"
        ),
        counterparty_events_path=(
            PROJECT_ROOT
            / "data/demo/counterparty_events.csv"
        ),
        output_directory=OUTPUT_DIRECTORY,
    )


def main() -> None:
    """Validate incremental decision behavior."""
    decisions = pd.read_csv(
        PROJECT_ROOT
        / "data/demo/ai_decisions.csv",
        dtype="string",
        keep_default_na=False,
    )

    first_result = run_decision_projection(
        data_source=build_data_source(),
        decisions=decisions,
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        persist_outputs=True,
    )

    assert len(first_result.applied_decisions) == 5
    assert len(first_result.ignored_decisions) == 1
    assert len(first_result.expansion_queue) == 9

    assert set(
        first_result.expansion_queue[
            "action_type"
        ].value_counts().to_dict().items()
    ) == {
        ("RUN_COUNTERPARTY_AI", 2),
        ("RUN_CUSTOMER_AI", 5),
        ("DISCOVER_CUSTOMER_RELATIONSHIPS", 2),
    }

    assert (
        first_result.groups[
            "counterparty_ai_pending_count"
        ].sum()
        == 2
    )

    assert (
        first_result.groups[
            "approved_suspicious_counterparty_count"
        ].sum()
        == 1
    )

    assert (
        first_result.groups[
            "suppressed_counterparty_count"
        ].sum()
        == 1
    )

    assert (
        first_result.groups[
            "customer_assessment_pending_count"
        ].sum()
        == 5
    )

    assert (
        first_result.groups[
            "mule_like_customer_count"
        ].sum()
        == 2
    )

    assert (
        first_result.groups[
            "recursive_expansion_source_count"
        ].sum()
        == 4
    )

    r3002_nodes = first_result.nodes.loc[
        first_result.nodes["entity_key"]
        == "RETAIL|R3002"
    ]

    assert len(r3002_nodes) == 1

    assert r3002_nodes[
        "customer_assessment_status"
    ].eq("MULE_LIKE").all()

    assert r3002_nodes[
        "expansion_source_flag"
    ].all()

    r3001_nodes = first_result.nodes.loc[
        first_result.nodes["entity_key"]
        == "RETAIL|R3001"
    ]

    assert len(r3001_nodes) == 1

    assert r3001_nodes[
        "customer_assessment_status"
    ].eq(
        "BLOCKED_PENDING_COUNTERPARTY_AI"
    ).all()

    assert not r3001_nodes[
        "expansion_source_flag"
    ].any()

    stale_decision = (
        first_result.ignored_decisions.iloc[0]
    )

    assert (
        stale_decision["ignored_reason"]
        == "FEATURE_SNAPSHOT_CHANGED"
    )

    second_result = run_decision_projection(
        data_source=build_data_source(),
        decisions=decisions,
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        persist_outputs=False,
    )

    assert_frame_equal(
        first_result.groups,
        second_result.groups,
        check_dtype=True,
    )

    assert_frame_equal(
        first_result.nodes,
        second_result.nodes,
        check_dtype=True,
    )

    assert_frame_equal(
        first_result.edges,
        second_result.edges,
        check_dtype=True,
    )

    assert_frame_equal(
        first_result.expansion_queue,
        second_result.expansion_queue,
        check_dtype=True,
    )

    print("Decision engine smoke test passed.")
    print(
        "Applied cached decisions: "
        f"{len(first_result.applied_decisions)}"
    )
    print(
        "Ignored stale decisions: "
        f"{len(first_result.ignored_decisions)}"
    )
    print(
        "Queued AI actions: 7"
    )
    print(
        "Queued relationship expansions: 2"
    )
    print(
        "Recursive expansion sources: 4"
    )
    print("Deterministic rerun: passed")


if __name__ == "__main__":
    main()
