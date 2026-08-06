"""Validate deduplicated Scenario 1 frontier termination offline."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (PROJECT_ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
)
from network_mule_discovery.daily_state import CsvDailyStateStore
from network_mule_discovery.recursive_counterparty_frontier import (
    run_recursive_counterparty_frontier,
)
from network_mule_discovery.recursive_customer_frontier import (
    run_recursive_customer_frontier,
)
from network_mule_discovery.recursive_termination import (
    run_recursive_termination,
)
from network_mule_discovery.scenario_1_synthetic_data import RUN_DATE
from smoke_test_scenario_1_recursive_counterparty_frontier import (
    RecursiveCounterpartyAdapter,
    build_initial_state,
    forbidden_factory,
)
from smoke_test_scenario_1_recursive_customer_frontier import (
    RecursiveCustomerAdapter,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        (
            source_directory,
            state_directory,
            initial_network,
            existing_payloads,
        ) = build_initial_state(root)
        counterparty_result = run_recursive_counterparty_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=existing_payloads,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=1,
                run_call_limit=1,
            ),
            adapter_factory=RecursiveCounterpartyAdapter,
        )
        combined_payloads = pd.concat(
            [
                existing_payloads,
                counterparty_result.new_features.counterparty_payloads,
            ],
            ignore_index=True,
        )
        customer_result = run_recursive_customer_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=combined_payloads,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=4,
                run_call_limit=3,
            ),
            adapter_factory=RecursiveCustomerAdapter,
        )
        all_payloads = pd.concat(
            [
                combined_payloads,
                customer_result.new_features.customer_payloads,
            ],
            ignore_index=True,
        )
        state_store = CsvDailyStateStore(state_directory)
        ready_discovery = (
            customer_result.customer_frontier
            .controlled_run.final_plan
            .actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                )
            ]
        )
        assert sorted(
            ready_discovery["subject_key"]
            .astype("string")
            .tolist()
        ) == [
            "RETAIL|R1005",
            "SME|B2001",
        ]

        try:
            run_recursive_termination(
                source_directory=source_directory,
                state_directory=state_directory,
                run_date=RUN_DATE,
                supplemental_subject_payloads=all_payloads,
            )
        except Exception as exc:
            assert (
                "Expected exactly one final recursive "
                "discovery action"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Legacy termination accepted multiple "
                "ready discovery sources."
            )

        b2001_result = run_recursive_counterparty_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=all_payloads,
            selected_source_entity_key="SME|B2001",
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=4,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
        )
        assert b2001_result.controlled_run.calls_executed == 0
        assert (
            b2001_result.discovery.source_entity_key
            == "SME|B2001"
        )
        assert (
            b2001_result.discovery.new_counterparty_keys
            == tuple()
        )
        assert b2001_result.discovery.relationships.empty

        remaining_discovery = (
            b2001_result.controlled_run
            .final_plan.actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                )
            ]
        )
        assert list(
            remaining_discovery["subject_key"]
        ) == ["RETAIL|R1005"]

        before_snapshot = state_store.load_snapshot()
        before_call_count = len(
            CsvAiCallLedger(state_directory).load()
        )

        result = run_recursive_termination(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=all_payloads,
        )
        assert result.source_entity_key == "RETAIL|R1005"
        assert result.discovery_performed is True
        assert result.expansion_ledger_appended is True
        assert result.discovery.new_counterparty_keys == tuple()
        assert result.discovery.relationships.empty
        assert set(
            result.discovery.skipped_existing_counterparty_keys
        ) == {"LOCAL_ACCOUNT|990200000001"}
        assert result.discovery.unshared_counterparty_keys == tuple()
        assert result.final_plan.actionable_queue.empty
        assert result.final_plan.failed_closed_item_count == 0
        assert len(result.expansion_ledger) == 3
        final_rows = result.expansion_ledger.loc[
            result.expansion_ledger["source_entity_key"].eq(
                "RETAIL|R1005"
            )
        ]
        assert len(final_rows) == 1
        final_row = final_rows.iloc[0]
        assert str(final_row["round_number"]) == "3"
        assert final_row["source_entity_key"] == "RETAIL|R1005"
        assert str(final_row["relationship_rows_found"]) == "0"
        assert final_row["expansion_status"] == "COMPLETED"
        after_snapshot = state_store.load_snapshot()
        assert len(after_snapshot.network.nodes) == len(
            before_snapshot.network.nodes
        )
        assert len(after_snapshot.network.edges) == len(
            before_snapshot.network.edges
        )
        assert len(after_snapshot.network.nodes) == (
            len(initial_network.nodes) + 4
        )
        assert len(after_snapshot.network.edges) == (
            len(initial_network.edges) + 4
        )
        group = after_snapshot.network.groups.iloc[0]
        assert group["termination_status"] == "TERMINATED"
        assert group["termination_reason"] == "FRONTIER_EXHAUSTED"
        status = result.termination_status.iloc[0]
        assert status["ready_frontier_count"] == 0
        assert status["failed_frontier_count"] == 0
        assert status["termination_status"] == "TERMINATED"
        assert status["termination_reason"] == "FRONTIER_EXHAUSTED"
        telemetry = result.guardrail_telemetry.iloc[0]
        assert int(telemetry["max_observed_depth"]) == 4
        assert int(telemetry["total_node_count"]) == 108
        assert int(telemetry["total_edge_count"]) == 109
        assert int(telemetry["current_frontier_width"]) == 0
        assert telemetry["guardrail_status"] == "TELEMETRY_ONLY"
        assert telemetry["termination_reason"] == "FRONTIER_EXHAUSTED"
        assert len(CsvAiCallLedger(state_directory).load()) == before_call_count

        repeated = run_recursive_termination(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=all_payloads,
        )
        assert repeated.discovery_performed is False
        assert repeated.expansion_ledger_appended is False
        assert len(repeated.expansion_ledger) == 3
        assert repeated.final_plan.actionable_queue.empty
        repeated_snapshot = state_store.load_snapshot()
        assert len(repeated_snapshot.network.nodes) == len(
            after_snapshot.network.nodes
        )
        assert len(repeated_snapshot.network.edges) == len(
            after_snapshot.network.edges
        )
        assert len(CsvAiCallLedger(state_directory).load()) == before_call_count

        print("Scenario 1 recursive termination smoke test passed.")
        print("Final recursive source consumed: RETAIL|R1005")
        print("Already observed counterparty skipped: 1")
        print("New shared counterparties discovered: 0")
        print("New graph nodes/edges: 0/0")
        print("Expansion round three completed with zero rows: passed")
        print("Ready frontier count: 0")
        print("Failed frontier count: 0")
        print("Termination reason: FRONTIER_EXHAUSTED")
        print("Maximum observed depth: 4")
        print("Guardrail mode: telemetry only")
        print("Repeated discovery actions: 0")
        print("Repeated AI calls: 0")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
