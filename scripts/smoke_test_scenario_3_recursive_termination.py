"""Validate Scenario 3 recursive consumption and termination offline."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_schemas import (
    build_counterparty_identity,
)
from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
)
from network_mule_discovery.daily_state import CsvDailyStateStore
from network_mule_discovery.frontier_ai import run_customer_ai_frontier
from network_mule_discovery.recursive_termination import (
    run_recursive_termination,
)
from network_mule_discovery.scenario_3_synthetic_data import (
    RUN_DATE,
    SEED_CUSTOMER_ID,
)
from smoke_test_scenario_3_live_customer_decisions import (
    PAYMENT_BACKED_SUBJECT_KEY,
    Scenario3CustomerDecisionAdapter,
    build_test_inputs,
)


BENEFICIARY_EDGE_TYPES = frozenset({
    "BENEFICIARY_ADDED_SEED_ACCOUNT",
    "BENEFICIARY_ADDED_MULE_ACCOUNT",
})


def _expected_seed_counterparty_key(source_directory: Path) -> str:
    seed_pool = pd.read_csv(
        source_directory / "seed_mule_pool.csv",
        dtype="string",
        keep_default_na=False,
    )
    row = seed_pool.loc[
        seed_pool["seed_customer_id"].eq(SEED_CUSTOMER_ID)
    ]

    assert len(row) == 1

    return build_counterparty_identity(
        rail="LOCAL",
        counterparty_iban="",
        counterparty_swift_bic="",
        counterparty_account_number=row.iloc[0]["seed_account_number"],
    ).counterparty_key


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        unified_result, customer_payloads, customer_keys = (
            build_test_inputs(root)
        )

        assert PAYMENT_BACKED_SUBJECT_KEY in customer_keys

        state_directory = root / "live_state"
        adapter = Scenario3CustomerDecisionAdapter()
        customer_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=customer_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=2,
            ),
            adapter_factory=lambda: adapter,
        )
        ready_discovery = (
            customer_result.controlled_run.final_plan.actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                )
            ]
        )
        assert list(ready_discovery["subject_key"]) == [
            PAYMENT_BACKED_SUBJECT_KEY
        ]

        state_store = CsvDailyStateStore(state_directory)
        before_snapshot = state_store.load_snapshot()
        before_call_count = len(CsvAiCallLedger(state_directory).load())
        expected_counterparty_key = _expected_seed_counterparty_key(
            source_directory
        )

        result = run_recursive_termination(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=customer_payloads,
        )

        assert result.source_entity_key == PAYMENT_BACKED_SUBJECT_KEY
        assert result.discovery_performed is True
        assert result.expansion_ledger_appended is True
        assert result.discovery.new_counterparty_keys == tuple()
        assert result.discovery.relationships.empty
        assert result.discovery.skipped_existing_counterparty_keys == tuple()
        assert set(result.discovery.unshared_counterparty_keys) == {
            expected_counterparty_key
        }
        assert result.final_plan.actionable_queue.empty
        assert result.final_plan.failed_closed_item_count == 0

        assert len(result.expansion_ledger) == 1
        expansion = result.expansion_ledger.iloc[0]
        assert str(expansion["round_number"]) == "1"
        assert expansion["source_entity_key"] == PAYMENT_BACKED_SUBJECT_KEY
        assert str(expansion["relationship_rows_found"]) == "0"
        assert expansion["expansion_status"] == "COMPLETED"

        after_snapshot = state_store.load_snapshot()
        assert len(after_snapshot.network.nodes) == len(
            before_snapshot.network.nodes
        )
        assert len(after_snapshot.network.edges) == len(
            before_snapshot.network.edges
        )
        assert len(after_snapshot.network.nodes) == 3
        assert len(after_snapshot.network.edges) == 2

        source_node_key = f"CUSTOMER|{PAYMENT_BACKED_SUBJECT_KEY}"
        represented_edges = after_snapshot.network.edges.loc[
            after_snapshot.network.edges["edge_type"].isin(
                BENEFICIARY_EDGE_TYPES
            )
            & (
                after_snapshot.network.edges["source_node_key"].eq(
                    source_node_key
                )
                | after_snapshot.network.edges["target_node_key"].eq(
                    source_node_key
                )
            )
        ]
        assert len(represented_edges) == 1

        group = after_snapshot.network.groups.iloc[0]
        assert group["termination_status"] == "TERMINATED"
        assert group["termination_reason"] == "FRONTIER_EXHAUSTED"
        status = result.termination_status.iloc[0]
        assert status["ready_frontier_count"] == 0
        assert status["failed_frontier_count"] == 0
        assert status["termination_status"] == "TERMINATED"
        assert status["termination_reason"] == "FRONTIER_EXHAUSTED"
        assert int(status["total_node_count"]) == 3
        assert int(status["total_edge_count"]) == 2

        telemetry = result.guardrail_telemetry.iloc[0]
        assert int(telemetry["total_node_count"]) == 3
        assert int(telemetry["total_edge_count"]) == 2
        assert int(telemetry["current_frontier_width"]) == 0
        assert telemetry["guardrail_status"] == "TELEMETRY_ONLY"
        assert telemetry["termination_reason"] == "FRONTIER_EXHAUSTED"
        assert len(CsvAiCallLedger(state_directory).load()) == (
            before_call_count
        )

        repeated = run_recursive_termination(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=customer_payloads,
        )
        assert repeated.discovery_performed is False
        assert repeated.expansion_ledger_appended is False
        assert len(repeated.expansion_ledger) == 1
        assert repeated.final_plan.actionable_queue.empty

        repeated_snapshot = state_store.load_snapshot()
        assert len(repeated_snapshot.network.nodes) == 3
        assert len(repeated_snapshot.network.edges) == 2
        assert len(CsvAiCallLedger(state_directory).load()) == (
            before_call_count
        )

    print("Scenario 3 recursive termination smoke test passed.")
    print("Final recursive source consumed: RETAIL|R3002")
    print("Known-mule beneficiary edges already represented: 1")
    print("Unshared known-mule account counterparties skipped: 1")
    print("New shared counterparties discovered: 0")
    print("New graph nodes/edges: 0/0")
    print("Expansion round one completed with zero rows: passed")
    print("Ready frontier count: 0")
    print("Failed frontier count: 0")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Repeated discovery actions: 0")
    print("Repeated AI calls: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
