"""Smoke test for persisted two-day incremental processing."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.recursive_expansion import (
    DeterministicDemoDecisionAdapter,
    PreparedExpansionEvidenceSource,
    RecursiveGuardrails,
    run_recursive_expansion,
)


DAY_ONE = "2026-07-16"
DAY_TWO = "2026-07-17"

OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the existing demo data source."""
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


def run_day_one():
    """Run the existing recursive prototype once."""
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
        run_date=DAY_ONE,
        output_directory=OUTPUT_DIRECTORY,
        guardrails=RecursiveGuardrails(),
        persist_outputs=False,
    )


def main() -> None:
    """Prove that an unchanged second day creates no repeated work."""
    day_one_result = run_day_one()

    assert (
        day_one_result.termination_reason
        == "FRONTIER_EMPTY"
    )

    assert day_one_result.remaining_queue.empty

    assert len(
        day_one_result.generated_decisions
    ) > 0

    assert len(
        day_one_result.expansion_ledger
    ) > 0

    with TemporaryDirectory() as directory:
        state_store = CsvDailyStateStore(
            Path(directory)
        )

        committed_state = (
            state_store.commit_recursive_result(
                result=day_one_result,
                run_date=DAY_ONE,
            )
        )

        day_two_plan = (
            build_incremental_daily_plan(
                state_store=state_store,
                run_date=DAY_TWO,
            )
        )

        assert (
            day_two_plan.queued_ai_action_count
            == 0
        )

        assert (
            day_two_plan
            .queued_expansion_action_count
            == 0
        )

        assert day_two_plan.actionable_queue.empty

        assert (
            day_two_plan.applied_decision_count
            > 0
        )

        assert (
            day_two_plan
            .completed_queue_item_count
            == len(
                day_one_result.expansion_ledger
            )
        )

        persisted_state = (
            state_store.load_snapshot()
        )

        assert persisted_state.frontier_queue.empty

        assert len(
            persisted_state.decision_store
        ) >= len(
            day_one_result.generated_decisions
        )

        assert len(
            persisted_state.expansion_ledger
        ) == len(
            day_one_result.expansion_ledger
        )

        assert set(
            persisted_state.network.nodes[
                "node_id"
            ]
        ) == set(
            day_one_result.nodes["node_id"]
        )

        assert set(
            persisted_state.network.edges[
                "edge_id"
            ]
        ) == set(
            day_one_result.edges["edge_id"]
        )

        repeated_plan = (
            build_incremental_daily_plan(
                state_store=state_store,
                run_date=DAY_TWO,
            )
        )

        assert_frame_equal(
            day_two_plan.actionable_queue,
            repeated_plan.actionable_queue,
            check_dtype=True,
        )

        assert (
            repeated_plan.queued_ai_action_count
            == 0
        )

        assert (
            repeated_plan
            .queued_expansion_action_count
            == 0
        )

    print(
        "Daily incremental state smoke test passed."
    )
    print(
        "Day 1 generated AI decisions: "
        f"{len(day_one_result.generated_decisions)}"
    )
    print(
        "Day 1 completed expansions: "
        f"{len(day_one_result.expansion_ledger)}"
    )
    print("Day 2 repeated AI actions: 0")
    print("Day 2 repeated expansions: 0")
    print("Stable network IDs: passed")
    print("Persisted frontier: empty")


if __name__ == "__main__":
    main()
