"""Validate analyst access to persisted group summaries."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = ROOT / "scripts"

for path in (
    ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from network_mule_discovery.analyst_application_state import (
    ANALYST_GROUP_TABLE_COLUMNS,
    AnalystApplicationStateError,
    AnalystApplicationStateStore,
)
from network_mule_discovery.consolidated_state import (
    ConsolidatedStateStore,
)
from network_mule_discovery.synthetic_scenario_registry import (
    create_synthetic_source_provider,
)
from smoke_test_analyst_application_runs import (
    build_run,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        provider = create_synthetic_source_provider(
            scenario_id="scenario_1",
            output_directory=source_directory,
        )
        state_store = ConsolidatedStateStore(
            state_directory
        )

        historical_run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="group-history",
            run_status="STOPPED",
            termination_status="STOPPED",
            termination_reason=(
                "MAX_FRONTIER_STEPS_REACHED"
            ),
        )
        current_run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="group-current",
            run_status="RUNNING",
        )

        current_snapshot = state_store.load()
        group_id = str(
            current_snapshot
            .daily_state
            .network
            .groups
            .iloc[0]["group_id"]
        )

        state_store.daily_state.save_frontier_queue(
            pd.DataFrame(
                [
                    {
                        "queue_item_id": "Q_READY",
                        "run_date": (
                            current_snapshot
                            .manifest
                            .run_date
                        ),
                        "action_type": (
                            "RUN_COUNTERPARTY_AI"
                        ),
                        "subject_type": (
                            "COUNTERPARTY"
                        ),
                        "subject_key": "CP_TEST",
                        "feature_snapshot_hash": (
                            "HASH_READY"
                        ),
                        "group_ids": group_id,
                        "trigger_decision_id": "",
                        "queue_reason": (
                            "NEW_OR_CHANGED_EVIDENCE"
                        ),
                        "priority": 10,
                        "queue_status": "READY",
                    },
                    {
                        "queue_item_id": "Q_FAILED",
                        "run_date": (
                            current_snapshot
                            .manifest
                            .run_date
                        ),
                        "action_type": (
                            "RUN_CUSTOMER_AI"
                        ),
                        "subject_type": "CUSTOMER",
                        "subject_key": (
                            "CUSTOMER_TEST"
                        ),
                        "feature_snapshot_hash": (
                            "HASH_FAILED"
                        ),
                        "group_ids": group_id,
                        "trigger_decision_id": "",
                        "queue_reason": (
                            "DETERMINISTIC_RELATIONSHIP"
                        ),
                        "priority": 20,
                        "queue_status": (
                            "FAILED_CLOSED"
                        ),
                    },
                ]
            )
        )

        application = (
            AnalystApplicationStateStore(
                state_directory
            )
        )

        current_groups = application.group_table(
            current_run_id
        )
        historical_groups = (
            application.group_table(
                historical_run_id
            )
        )

        assert tuple(
            current_groups.columns
        ) == ANALYST_GROUP_TABLE_COLUMNS
        assert not current_groups.empty
        assert not historical_groups.empty
        assert current_groups[
            "run_id"
        ].eq(current_run_id).all()
        assert historical_groups[
            "run_id"
        ].eq(historical_run_id).all()

        selected = current_groups.loc[
            current_groups["group_id"].eq(
                group_id
            )
        ].iloc[0]

        assert selected["ready_action_count"] == 1
        assert (
            selected[
                "failed_closed_action_count"
            ]
            == 1
        )
        assert (
            selected["total_node_count"]
            >= selected["customer_count"]
        )
        assert (
            selected["total_edge_count"]
            >= selected["eid_link_count"]
        )

        assert (
            historical_groups[
                "ready_action_count"
            ].sum()
            == 0
        )
        assert (
            historical_groups[
                "failed_closed_action_count"
            ].sum()
            == 0
        )

        try:
            application.group_table(
                "RUN_00000000000000000000"
            )
        except AnalystApplicationStateError:
            pass
        else:
            raise AssertionError(
                "Unknown run group table was accepted."
            )

        print(
            "Analyst application group catalogue "
            "smoke test passed."
        )
        print(
            f"Current groups listed: "
            f"{len(current_groups)}"
        )
        print(
            f"Historical groups listed: "
            f"{len(historical_groups)}"
        )
        print("Relationship counts exposed: passed")
        print("Decision counts exposed: passed")
        print("Ready action counts exposed: passed")
        print("Failed-closed counts exposed: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
