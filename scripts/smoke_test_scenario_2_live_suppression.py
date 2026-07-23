"""Validate Scenario 2 suppression, fail-closed safety, and termination."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_PAYLOAD_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
    run_counterparty_ai_frontier,
)
from network_mule_discovery.frontier_termination import (
    FrontierTerminationError,
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionError,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    COMMON_PUBLIC_COUNTERPARTY_ACCOUNT,
    NON_SEED_CUSTOMER_COUNT,
    RUN_DATE,
    SEED_CUSTOMER_ID,
    generate_scenario_2_source_data,
)


class CommonPublicAdapter:
    """Return one deterministic common/public suppression."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.last_call_metadata: dict[str, object] | None = None

    def decide(
        self,
        *,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        feature_payload_json: str,
        run_date: date,
        round_number: int,
        sequence_number: int,
    ) -> dict[str, str]:
        assert subject_type == "COUNTERPARTY"
        assert subject_key.endswith(
            COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
        )

        payload = json.loads(feature_payload_json)
        graph_summary = payload[
            "relationship_evidence_summary"
        ]
        behavior_sampling = payload[
            "behavioral_evidence"
        ]["linked_customer_sampling"]

        assert graph_summary["relationship_count"] == 501
        assert graph_summary[
            "sampled_relationship_count"
        ] == 10
        assert behavior_sampling[
            "population_customer_count"
        ] == 501
        assert behavior_sampling[
            "sampled_customer_count"
        ] == 10
        assert len(feature_payload_json.encode("utf-8")) < 100000

        self.calls.append(subject_key)
        self.last_call_metadata = {
            "model": "offline-common-public-test",
            "prompt_version": "scenario-2-test-v1",
            "response_id": "resp_scenario_2_test",
            "request_id": "req_scenario_2_test",
            "response_status": "completed",
            "incomplete_reason": "",
            "input_tokens": 1200,
            "output_tokens": 120,
            "reasoning_tokens": 40,
            "assessment": {
                "decision": "COMMON_PUBLIC_SUPPRESS",
                "reason_code": (
                    "HIGH_DEGREE_RECURRING_PUBLIC_SERVICE"
                ),
                "rationale": (
                    "The broad, recurring, low-concentration "
                    "utility-style pattern is unsuitable for "
                    "network expansion."
                ),
                "key_evidence": [
                    "501 customers show recurring service payments.",
                    "Customer amount concentration is very low.",
                    "No rapid-drain behavior is present.",
                ],
                "confidence": "HIGH",
            },
        }

        digest = hashlib.sha256(
            "|".join(
                [
                    subject_type,
                    subject_key,
                    feature_snapshot_hash,
                    "COMMON_PUBLIC_SUPPRESS",
                ]
            ).encode("utf-8")
        ).hexdigest()[:16]

        return {
            "decision_id": f"S2{digest}",
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": feature_snapshot_hash,
            "decision": "COMMON_PUBLIC_SUPPRESS",
            "reason_code": (
                "HIGH_DEGREE_RECURRING_PUBLIC_SERVICE"
            ),
            "decision_version": "scenario-2-test-v1",
            "decided_at": "2026-07-20 12:00:00",
            "source": "TEST_DECISION_ADAPTER",
        }


class FailingAdapter:
    """Fail the single hub decision without exposing customers."""

    def decide(self, **_: object) -> dict[str, str]:
        raise OpenAIDecisionError(
            "SYNTHETIC_API_FAILURE",
            "Synthetic Scenario 2 API failure.",
        )


def forbidden_factory() -> object:
    raise AssertionError(
        "An unchanged rerun must not instantiate the adapter."
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        work_directory = root / "runtime"
        evidence_directory = (
            work_directory / "hub_discovery_evidence"
        )

        generate_scenario_2_source_data(
            source_directory
        )
        subprocess.run(
            [
                sys.executable,
                str(
                    SCRIPTS_DIRECTORY
                    / "run_scenario_2_hub_discovery_evidence.py"
                ),
                "--source-directory",
                str(source_directory),
                "--work-directory",
                str(work_directory),
                "--run-date",
                str(RUN_DATE),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        unified_result = load_unified_result(
            evidence_directory
        )
        supplemental_payloads = (
            load_supplemental_subject_payloads(
                evidence_directory
                / "features"
                / COUNTERPARTY_PAYLOAD_FILENAME
            )
        )
        group_ids = tuple(
            unified_result.groups["group_id"]
            .astype("string")
            .tolist()
        )
        settings = DailyAiSettings(
            live_ai_enabled=True,
            daily_call_limit=10,
            run_call_limit=1,
        )

        success_state = root / "success_state"
        adapter = CommonPublicAdapter()
        success = run_counterparty_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
            state_directory=success_state,
            run_date=RUN_DATE,
            settings=settings,
            reset_state=True,
            adapter_factory=lambda: adapter,
        )

        assert success.controlled_run.calls_executed == 1
        assert len(adapter.calls) == 1
        assert len(success.decision_store) == 1
        assert success.decision_store.iloc[0][
            "decision"
        ] == "COMMON_PUBLIC_SUPPRESS"
        assert success.controlled_run.final_plan.actionable_queue.empty
        assert (
            success.controlled_run.final_plan.failed_closed_item_count
            == 0
        )

        projection = success.controlled_run.final_plan.projection
        counterparty_nodes = projection.nodes.loc[
            projection.nodes["node_type"].eq("COUNTERPARTY")
        ]
        assert len(counterparty_nodes) == 1
        assert counterparty_nodes.iloc[0][
            "node_status"
        ] == "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC"
        assert not bool(
            counterparty_nodes.iloc[0][
                "customer_discovery_allowed_flag"
            ]
        )

        shared_edges = projection.edges.loc[
            projection.edges["edge_type"].eq(
                "SHARED_EXTERNAL_COUNTERPARTY"
            )
        ]
        assert len(shared_edges) == NON_SEED_CUSTOMER_COUNT
        assert shared_edges["relationship_status"].eq(
            "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC"
        ).all()
        assert not shared_edges[
            "customer_discovery_allowed_flag"
        ].astype(bool).any()
        assert not shared_edges[
            "recursive_expansion_allowed_flag"
        ].astype(bool).any()

        customer_queue = success.controlled_run.final_plan.actionable_queue.loc[
            success.controlled_run.final_plan.actionable_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI")
        ]
        recursive_queue = success.controlled_run.final_plan.actionable_queue.loc[
            success.controlled_run.final_plan.actionable_queue[
                "action_type"
            ].eq("DISCOVER_CUSTOMER_RELATIONSHIPS")
        ]
        assert customer_queue.empty
        assert recursive_queue.empty

        termination = run_frontier_exhaustion_termination(
            state_directory=success_state,
            run_date=RUN_DATE,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
            group_ids=group_ids,
            source_entity_key=(
                f"RETAIL|{SEED_CUSTOMER_ID}"
            ),
        )
        assert termination.final_plan.actionable_queue.empty
        assert len(termination.termination_status) == 1
        status = termination.termination_status.iloc[0]
        assert status["termination_status"] == "TERMINATED"
        assert status["termination_reason"] == "FRONTIER_EXHAUSTED"
        assert int(status["ready_frontier_count"]) == 0
        assert int(status["failed_frontier_count"]) == 0
        assert int(status["total_node_count"]) == 502
        assert int(status["total_edge_count"]) == 501

        repeated = run_counterparty_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
            state_directory=success_state,
            run_date=RUN_DATE,
            settings=settings,
            adapter_factory=forbidden_factory,
        )
        assert repeated.controlled_run.calls_before_run == 1
        assert repeated.controlled_run.calls_executed == 0
        assert repeated.controlled_run.final_plan.actionable_queue.empty

        repeated_termination = run_frontier_exhaustion_termination(
            state_directory=success_state,
            run_date=RUN_DATE,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
            group_ids=group_ids,
            source_entity_key=(
                f"RETAIL|{SEED_CUSTOMER_ID}"
            ),
        )
        assert repeated_termination.final_plan.actionable_queue.empty

        failure_state = root / "failure_state"
        failed = run_counterparty_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
            state_directory=failure_state,
            run_date=RUN_DATE,
            settings=settings,
            reset_state=True,
            adapter_factory=FailingAdapter,
        )
        assert failed.controlled_run.calls_executed == 1
        assert failed.decision_store.empty
        assert (
            failed.controlled_run.final_plan.failed_closed_item_count
            == 1
        )
        assert not failed.controlled_run.final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI").any()

        try:
            run_frontier_exhaustion_termination(
                state_directory=failure_state,
                run_date=RUN_DATE,
                supplemental_subject_payloads=(
                    supplemental_payloads
                ),
                group_ids=group_ids,
                source_entity_key=(
                    f"RETAIL|{SEED_CUSTOMER_ID}"
                ),
            )
        except FrontierTerminationError:
            pass
        else:
            raise AssertionError(
                "Failed-closed frontier must not terminate."
            )

    print("Scenario 2 live suppression smoke test passed.")
    print("Counterparty AI calls executed: 1")
    print("Decision: COMMON_PUBLIC_SUPPRESS")
    print(
        "Suppressed non-seed customer relationships: "
        f"{NON_SEED_CUSTOMER_COUNT}"
    )
    print("Customer AI actions queued: 0")
    print("Recursive sources queued: 0")
    print("Observed graph nodes/edges preserved: 502/501")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Unchanged repeated AI calls: 0")
    print("Failure path remained failed closed: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
