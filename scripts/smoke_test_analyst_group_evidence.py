"""Validate analyst relationship and AI evidence isolation."""

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
    AnalystApplicationStateStore,
)
from network_mule_discovery.analyst_group_evidence import (
    AnalystGroupEvidenceStore,
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
            state_namespace="analyst-group-evidence",
            run_status="RUNNING",
        )

        application = AnalystApplicationStateStore(
            state_directory
        )
        group_id = str(
            application.group_table(
                run_id
            ).iloc[0]["group_id"]
        )
        snapshot = application.load_run(run_id)
        nodes = (
            snapshot.daily_state.network.nodes
        )
        group_nodes = nodes.loc[
            nodes["group_id"].eq(group_id)
        ]

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
                        "subject_key": (
                            "UNRELATED_CUSTOMER"
                        ),
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

        state_store.ai_calls.append_executions(
            run_date=snapshot.manifest.run_date,
            executed_actions=pd.DataFrame(
                [
                    {
                        "queue_item_id": "Q_CUSTOMER",
                        "action_type": "RUN_CUSTOMER_AI",
                        "subject_type": "CUSTOMER",
                        "subject_key": customer_key,
                        "feature_snapshot_hash": (
                            "HASH_CUSTOMER"
                        ),
                        "execution_status": "COMPLETED",
                        "attempted_at": (
                            "2026-08-03T00:00:00Z"
                        ),
                        "generated_decision_id": (
                            "D_CUSTOMER"
                        ),
                        "decision": "LOW_CONCERN",
                        "reason_code": "SMOKE_TEST",
                        "confidence": "0.91",
                        "rationale": (
                            "Customer evidence rationale"
                        ),
                        "key_evidence_json": (
                            '["customer evidence"]'
                        ),
                        "model": "test-model",
                        "prompt_version": (
                            "test-prompt:customer"
                        ),
                        "response_id": "resp_customer",
                        "request_id": "req_customer",
                        "response_status": "completed",
                        "input_tokens": "100",
                        "output_tokens": "20",
                        "reasoning_tokens": "5",
                    },
                    {
                        "queue_item_id": "Q_COUNTERPARTY",
                        "action_type": (
                            "RUN_COUNTERPARTY_AI"
                        ),
                        "subject_type": "COUNTERPARTY",
                        "subject_key": counterparty_key,
                        "feature_snapshot_hash": (
                            "HASH_COUNTERPARTY"
                        ),
                        "execution_status": (
                            "FAILED_CLOSED"
                        ),
                        "attempted_at": (
                            "2026-08-03T00:01:00Z"
                        ),
                        "model": "test-model",
                        "prompt_version": (
                            "test-prompt:counterparty"
                        ),
                        "error_code": "AI_API_ERROR",
                        "error_message": (
                            "Synthetic API failure"
                        ),
                        "request_id": "req_counterparty",
                    },
                    {
                        "queue_item_id": "Q_UNRELATED",
                        "action_type": "RUN_CUSTOMER_AI",
                        "subject_type": "CUSTOMER",
                        "subject_key": (
                            "UNRELATED_CUSTOMER"
                        ),
                        "feature_snapshot_hash": (
                            "HASH_UNRELATED"
                        ),
                        "execution_status": "COMPLETED",
                        "attempted_at": (
                            "2026-08-03T00:02:00Z"
                        ),
                        "generated_decision_id": (
                            "D_UNRELATED"
                        ),
                        "decision": "LOW_CONCERN",
                        "reason_code": "SMOKE_TEST",
                    },
                ]
            ),
        )

        evidence = AnalystGroupEvidenceStore(
            state_directory
        ).load(
            run_id=run_id,
            group_id=group_id,
        )

        assert evidence.run_id == run_id
        assert evidence.group_id == group_id
        assert len(
            evidence.relationship_evidence
        ) == len(
            snapshot.daily_state.network.edges.loc[
                snapshot
                .daily_state
                .network
                .edges["group_id"]
                .eq(group_id)
            ]
        )
        assert evidence.relationship_evidence[
            "source_display_label"
        ].ne("").all()
        assert evidence.relationship_evidence[
            "target_display_label"
        ].ne("").all()

        assert set(
            evidence.decision_evidence[
                "decision_id"
            ]
        ) == {
            "D_CUSTOMER",
            "D_COUNTERPARTY",
        }

        assert set(
            evidence.ai_call_evidence[
                "subject_key"
            ]
        ) == {
            customer_key,
            counterparty_key,
        }

        failed_call = (
            evidence.ai_call_evidence.loc[
                evidence.ai_call_evidence[
                    "call_status"
                ].eq("FAILED_CLOSED")
            ]
            .iloc[0]
        )

        assert failed_call["model"] == "test-model"
        assert (
            failed_call["prompt_version"]
            == "test-prompt:counterparty"
        )
        assert (
            failed_call["error_code"]
            == "AI_API_ERROR"
        )

        print(
            "Analyst group evidence smoke test passed."
        )
        print(
            "Relationship evidence rows: "
            f"{len(evidence.relationship_evidence)}"
        )
        print("Related decisions loaded: 2")
        print("Related AI calls loaded: 2")
        print("Unrelated evidence excluded: passed")
        print("Failed-call audit exposed: passed")
        print("Model and prompt identity exposed: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
