"""Smoke test for bounded recursive expansion."""

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
from network_mule_discovery.recursive_expansion import (
    DeterministicDemoDecisionAdapter,
    PreparedExpansionEvidenceSource,
    RecursiveGuardrails,
    run_recursive_expansion,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the complete demo data source."""
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


def run_once():
    """Run one deterministic recursive test."""
    initial_decisions = pd.read_csv(
        PROJECT_ROOT
        / "data/demo/ai_decisions.csv",
        dtype="string",
        keep_default_na=False,
    )

    return run_recursive_expansion(
        data_source=build_data_source(),
        initial_decisions=initial_decisions,
        evidence_source=(
            PreparedExpansionEvidenceSource(
                PROJECT_ROOT
                / "data/demo/"
                "recursive_relationship_candidates.csv"
            )
        ),
        decision_adapter=(
            DeterministicDemoDecisionAdapter()
        ),
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        guardrails=RecursiveGuardrails(),
        persist_outputs=True,
    )


def main() -> None:
    """Validate the recursive workflow."""
    first_result = run_once()

    assert (
        first_result.termination_reason
        == "FRONTIER_EMPTY"
    )

    assert len(first_result.groups) == 2
    assert len(first_result.nodes) == 27
    assert len(first_result.edges) == 25

    assert len(
        first_result.generated_decisions
    ) == 16

    assert len(
        first_result.expansion_ledger
    ) == 5

    assert first_result.remaining_queue.empty

    assert (
        first_result.groups[
            "recursive_expansion_source_count"
        ].sum()
        == 7
    )

    assert (
        first_result.groups[
            "mule_like_customer_count"
        ].sum()
        == 5
    )

    assert (
        first_result.groups[
            "approved_suspicious_counterparty_count"
        ].sum()
        == 4
    )

    assert (
        first_result.groups[
            "suppressed_counterparty_count"
        ].sum()
        == 3
    )

    expansion_sources = set(
        first_result.nodes.loc[
            first_result.nodes[
                "expansion_source_flag"
            ],
            "entity_key",
        ]
    )

    assert {
        "RETAIL|R3002",
        "RETAIL|R3003",
        "RETAIL|R4001",
        "SME|B4002",
        "RETAIL|R4005",
    }.issubset(expansion_sources)

    r4004 = first_result.nodes.loc[
        first_result.nodes["entity_key"]
        == "RETAIL|R4004"
    ]

    assert len(r4004) == 1

    assert r4004[
        "customer_assessment_status"
    ].eq(
        "BLOCKED_PENDING_COUNTERPARTY_AI"
    ).all()

    r5001 = first_result.nodes.loc[
        first_result.nodes["entity_key"]
        == "RETAIL|R5001"
    ]

    assert r5001[
        "customer_assessment_status"
    ].eq("LOW_CONCERN").all()

    second_result = run_once()

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
        first_result.generated_decisions,
        second_result.generated_decisions,
        check_dtype=True,
    )

    assert_frame_equal(
        first_result.round_log,
        second_result.round_log,
        check_dtype=True,
    )

    print("Recursive expansion smoke test passed.")
    print(
        f"Termination: "
        f"{first_result.termination_reason}"
    )
    print(f"Nodes: {len(first_result.nodes)}")
    print(f"Edges: {len(first_result.edges)}")
    print(
        "Generated AI decisions: "
        f"{len(first_result.generated_decisions)}"
    )
    print(
        "Completed customer expansions: "
        f"{len(first_result.expansion_ledger)}"
    )
    print("Remaining queue: 0")
    print("Deterministic rerun: passed")


if __name__ == "__main__":
    main()
