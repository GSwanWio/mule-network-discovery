"""Smoke test for unified seed-led groups."""

from __future__ import annotations

import sys
from pathlib import Path

from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.unified_group_runner import (
    run_unified_group_projection,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the full demo data source."""
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
    """Validate the unified projection."""
    first_result = run_unified_group_projection(
        data_source=build_data_source(),
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        persist_outputs=True,
    )

    assert len(first_result.groups) == 2
    assert len(first_result.nodes) == 17
    assert len(first_result.edges) == 15

    assert sorted(
        first_result.groups[
            "total_node_count"
        ].tolist()
    ) == [8, 9]

    assert sorted(
        first_result.groups[
            "total_edge_count"
        ].tolist()
    ) == [7, 8]

    assert (
        first_result.groups[
            "eid_link_count"
        ].sum()
        == 5
    )

    assert (
        first_result.groups[
            "counterparty_candidate_count"
        ].sum()
        == 4
    )

    assert (
        first_result.groups[
            "shared_counterparty_customer_count"
        ].sum()
        == 4
    )

    assert (
        first_result.groups[
            "beneficiary_seed_link_count"
        ].sum()
        == 2
    )

    assert (
        first_result.groups[
            "customer_assessment_pending_count"
        ].sum()
        == 7
    )

    assert (
        first_result.groups[
            "counterparty_ai_pending_count"
        ].sum()
        == 4
    )

    assert (
        first_result.groups[
            "recursive_expansion_source_count"
        ].sum()
        == 2
    )

    assert first_result.groups[
        "group_id"
    ].is_unique

    assert first_result.nodes[
        "node_id"
    ].is_unique

    assert first_result.edges[
        "edge_id"
    ].is_unique

    assert not first_result.edges[
        "recursive_expansion_allowed_flag"
    ].any()

    eid_edges = first_result.edges.loc[
        first_result.edges["edge_type"]
        == "SAME_EMIRATES_ID"
    ]

    assert eid_edges[
        "customer_discovery_allowed_flag"
    ].all()

    counterparty_edges = first_result.edges.loc[
        first_result.edges["edge_type"].isin(
            [
                "SEED_COUNTERPARTY_EVIDENCE",
                "SHARED_EXTERNAL_COUNTERPARTY",
            ]
        )
    ]

    assert not counterparty_edges[
        "customer_discovery_allowed_flag"
    ].any()

    beneficiary_edges = first_result.edges.loc[
        first_result.edges["edge_type"]
        == "BENEFICIARY_ADDED_SEED_ACCOUNT"
    ]

    assert beneficiary_edges[
        "customer_discovery_allowed_flag"
    ].all()

    second_result = run_unified_group_projection(
        data_source=build_data_source(),
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

    print("Unified group smoke test passed.")
    print(f"Groups: {len(first_result.groups)}")
    print(f"Nodes: {len(first_result.nodes)}")
    print(f"Edges: {len(first_result.edges)}")
    print("Stable seed-anchored group IDs: passed")
    print("Deterministic rerun: passed")


if __name__ == "__main__":
    main()
