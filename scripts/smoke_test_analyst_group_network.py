"""Validate the selected-group analyst network contract."""

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
    AnalystApplicationStateError,
    AnalystApplicationStateStore,
)
from network_mule_discovery.analyst_group_network import (
    AnalystGroupNetworkStore,
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

        run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="analyst-group-network",
            run_status="RUNNING",
        )

        application = AnalystApplicationStateStore(
            state_directory
        )
        group_id = (
            application.group_table(run_id)
            .iloc[0]["group_id"]
        )
        snapshot = application.load_run(run_id)
        group_nodes = (
            snapshot.daily_state.network.nodes.loc[
                snapshot
                .daily_state
                .network
                .nodes["group_id"]
                .eq(group_id)
            ]
        )

        customer_key = str(
            group_nodes.loc[
                group_nodes["node_type"]
                .eq("CUSTOMER"),
                "entity_key",
            ].iloc[0]
        )
        counterparty_key = str(
            group_nodes.loc[
                group_nodes["node_type"]
                .eq("COUNTERPARTY"),
                "counterparty_key",
            ].iloc[0]
        )

        state_store.daily_state.append_decisions(
            pd.DataFrame(
                [
                    {
                        "decision_id": "D_CUSTOMER",
                        "subject_type": "CUSTOMER",
                        "subject_key": customer_key,
                        "feature_snapshot_hash": (
                            "HASH_CUSTOMER"
                        ),
                        "decision": "LOW_CONCERN",
                        "reason_code": "SMOKE_TEST",
                        "decision_version": "test-v1",
                        "decided_at": (
                            "2026-08-03T00:00:00Z"
                        ),
                        "source": "SMOKE_TEST",
                    },
                    {
                        "decision_id": "D_COUNTERPARTY",
                        "subject_type": "COUNTERPARTY",
                        "subject_key": counterparty_key,
                        "feature_snapshot_hash": (
                            "HASH_COUNTERPARTY"
                        ),
                        "decision": (
                            "SUSPICIOUS_EXPAND"
                        ),
                        "reason_code": "SMOKE_TEST",
                        "decision_version": "test-v1",
                        "decided_at": (
                            "2026-08-03T00:01:00Z"
                        ),
                        "source": "SMOKE_TEST",
                    },
                    {
                        "decision_id": "D_UNRELATED",
                        "subject_type": "CUSTOMER",
                        "subject_key": "UNRELATED_CUSTOMER",
                        "feature_snapshot_hash": (
                            "HASH_UNRELATED"
                        ),
                        "decision": "LOW_CONCERN",
                        "reason_code": "SMOKE_TEST",
                        "decision_version": "test-v1",
                        "decided_at": (
                            "2026-08-03T00:02:00Z"
                        ),
                        "source": "SMOKE_TEST",
                    },
                ]
            )
        )

        state_store.daily_state.save_frontier_queue(
            pd.DataFrame(
                [
                    {
                        "queue_item_id": "Q_SELECTED",
                        "run_date": (
                            snapshot.manifest.run_date
                        ),
                        "action_type": (
                            "RUN_CUSTOMER_AI"
                        ),
                        "subject_type": "CUSTOMER",
                        "subject_key": customer_key,
                        "feature_snapshot_hash": (
                            "QUEUE_HASH_SELECTED"
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
                        "queue_item_id": "Q_UNRELATED",
                        "run_date": (
                            snapshot.manifest.run_date
                        ),
                        "action_type": (
                            "RUN_CUSTOMER_AI"
                        ),
                        "subject_type": "CUSTOMER",
                        "subject_key": (
                            "UNRELATED_CUSTOMER"
                        ),
                        "feature_snapshot_hash": (
                            "QUEUE_HASH_UNRELATED"
                        ),
                        "group_ids": "G_UNRELATED",
                        "trigger_decision_id": "",
                        "queue_reason": (
                            "NEW_OR_CHANGED_EVIDENCE"
                        ),
                        "priority": 20,
                        "queue_status": "READY",
                    },
                ]
            )
        )

        state_store.daily_state.append_expansion_ledger(
            pd.DataFrame(
                [
                    {
                        "run_date": (
                            snapshot.manifest.run_date
                        ),
                        "round_number": "1",
                        "queue_item_id": (
                            "EXP_SELECTED"
                        ),
                        "source_entity_key": customer_key,
                        "group_ids": group_id,
                        "relationship_rows_found": "2",
                        "expansion_status": "COMPLETED",
                    },
                    {
                        "run_date": (
                            snapshot.manifest.run_date
                        ),
                        "round_number": "1",
                        "queue_item_id": (
                            "EXP_UNRELATED"
                        ),
                        "source_entity_key": (
                            "UNRELATED_CUSTOMER"
                        ),
                        "group_ids": "G_UNRELATED",
                        "relationship_rows_found": "0",
                        "expansion_status": "COMPLETED",
                    },
                ]
            )
        )

        network = AnalystGroupNetworkStore(
            state_directory
        ).load(
            run_id=run_id,
            group_id=group_id,
        )

        assert network.run_id == run_id
        assert network.group_id == group_id
        assert (
            len(network.nodes)
            == network.summary.total_node_count
        )
        assert (
            len(network.edges)
            == network.summary.total_edge_count
        )
        assert set(
            network.decisions["decision_id"]
        ) == {
            "D_CUSTOMER",
            "D_COUNTERPARTY",
        }
        assert list(
            network.frontier_queue[
                "queue_item_id"
            ]
        ) == ["Q_SELECTED"]
        assert list(
            network.expansion_ledger[
                "queue_item_id"
            ]
        ) == ["EXP_SELECTED"]

        node_ids = set(network.nodes["node_id"])

        assert set(
            network.edges["source_node_id"]
        ).issubset(node_ids)
        assert set(
            network.edges["target_node_id"]
        ).issubset(node_ids)

        try:
            AnalystGroupNetworkStore(
                state_directory
            ).load(
                run_id=run_id,
                group_id="G_UNKNOWN",
            )
        except AnalystApplicationStateError:
            pass
        else:
            raise AssertionError(
                "Unknown group was accepted."
            )

        print(
            "Analyst selected-group network "
            "smoke test passed."
        )
        print(
            f"Group nodes loaded: "
            f"{len(network.nodes)}"
        )
        print(
            f"Group edges loaded: "
            f"{len(network.edges)}"
        )
        print("Related decisions loaded: 2")
        print("Unrelated decisions excluded: passed")
        print("Group frontier isolated: passed")
        print("Group expansion history isolated: passed")
        print("Edge endpoint integrity: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
