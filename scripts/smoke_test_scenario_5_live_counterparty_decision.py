"""Validate Scenario 5 controlled suppression and fail-closed behavior."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (PROJECT_ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.decision_policy import (
    COUNTERPARTY_ASSESSMENT_POLICY_VERSION,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionError,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    AMBIGUOUS_COUNTERPARTY_ACCOUNT,
    LINKED_CUSTOMER_IDS,
    RUN_DATE,
    generate_scenario_5_source_data,
)
from run_scenario_5_live_counterparty_decision import (
    execute_scenario_5_live_counterparty_decision,
)


class Scenario5InsufficientEvidenceAdapter:
    """Return the intended deterministic offline suppression."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.last_call_metadata: dict[str, object] = {}

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
        payload = json.loads(feature_payload_json)
        assert payload["subject_key"] == subject_key
        assert (
            payload["behavioral_evidence"][
                "counterparty_assessment_policy_version"
            ]
            == COUNTERPARTY_ASSESSMENT_POLICY_VERSION
        )
        assert (
            payload["behavioral_evidence"]
            ["aggregate_behavior"]
            ["distinct_customer_count"]
            == 3
        )

        self.calls.append(subject_key)
        self.last_call_metadata = {
            "model": "offline-scenario-5-test",
            "prompt_version": "scenario-5-test-v1",
            "response_id": "resp_scenario_5_live_test",
            "request_id": "req_scenario_5_live_test",
            "response_status": "completed",
            "incomplete_reason": "",
            "input_tokens": 900,
            "output_tokens": 100,
            "reasoning_tokens": 30,
            "assessment": {
                "decision": (
                    "INSUFFICIENT_EVIDENCE_SUPPRESS"
                ),
                "reason_code": (
                    "LOW_VOLUME_AMBIGUOUS_SHARED_COUNTERPARTY"
                ),
                "rationale": (
                    "The small, low-value, non-rapid pattern is "
                    "insufficient for customer exposure."
                ),
                "key_evidence": [
                    "Only three customers and three payments are observed."
                ],
                "confidence": "MEDIUM",
            },
        }

        digest = hashlib.sha256(
            (
                f"{subject_key}|"
                f"{feature_snapshot_hash}"
            ).encode("utf-8")
        ).hexdigest()[:16]

        return {
            "decision_id": f"S5L{digest}",
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "decision": (
                "INSUFFICIENT_EVIDENCE_SUPPRESS"
            ),
            "reason_code": (
                "LOW_VOLUME_AMBIGUOUS_SHARED_COUNTERPARTY"
            ),
            "decision_version": "scenario-5-test-v1",
            "decided_at": "2026-07-20 12:00:00",
            "source": "TEST_DECISION_ADAPTER",
        }


class Scenario5FailureAdapter:
    """Fail the counterparty call before any decision is created."""

    def __init__(self) -> None:
        self.calls: list[str] = []

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
        assert feature_payload_json
        self.calls.append(subject_key)
        raise OpenAIDecisionError(
            "SYNTHETIC_SCENARIO_5_FAILURE",
            "Synthetic Scenario 5 API failure.",
        )


def forbidden_factory() -> object:
    raise AssertionError(
        "Unchanged evidence must not instantiate the adapter."
    )


def build_evidence(
    *,
    source_directory: Path,
    runtime_directory: Path,
) -> None:
    subprocess.run(
        [
            sys.executable,
            str(
                SCRIPTS_DIRECTORY
                / "run_scenario_5_counterparty_evidence.py"
            ),
            "--source-directory",
            str(source_directory),
            "--work-directory",
            str(runtime_directory),
            "--run-date",
            str(RUN_DATE),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        runtime_directory = root / "runtime"

        generate_scenario_5_source_data(
            source_directory
        )
        build_evidence(
            source_directory=source_directory,
            runtime_directory=runtime_directory,
        )

        settings = DailyAiSettings(
            live_ai_enabled=True,
            daily_call_limit=5,
            run_call_limit=1,
        )
        adapter = Scenario5InsufficientEvidenceAdapter()

        first = execute_scenario_5_live_counterparty_decision(
            runtime_directory=runtime_directory,
            run_date=RUN_DATE,
            settings=settings,
            reset_state=True,
            adapter_factory=lambda: adapter,
        )

        assert len(adapter.calls) == 1
        assert (
            adapter.calls[0]
            .endswith(AMBIGUOUS_COUNTERPARTY_ACCOUNT)
        )
        assert first.telemetry["calls_executed"] == 1
        assert (
            first.telemetry["applied_decision"]
            == "INSUFFICIENT_EVIDENCE_SUPPRESS"
        )
        assert (
            first.telemetry[
                "expected_insufficient_evidence_match"
            ]
            is True
        )
        assert (
            first.telemetry[
                "suppressed_non_seed_customer_count"
            ]
            == len(LINKED_CUSTOMER_IDS)
        )
        assert (
            first.telemetry[
                "counterparty_ai_actions_queued"
            ]
            == 0
        )
        assert (
            first.telemetry[
                "customer_ai_actions_queued"
            ]
            == 0
        )
        assert (
            first.telemetry["recursive_sources_queued"]
            == 0
        )
        assert first.telemetry["failed_frontier_count"] == 0
        assert first.telemetry["termination_status"] == "TERMINATED"
        assert (
            first.telemetry["termination_reason"]
            == "FRONTIER_EXHAUSTED"
        )
        assert first.telemetry["observed_node_count"] == 4
        assert first.telemetry["observed_edge_count"] == 3
        assert first.termination is not None

        projection = first.termination.final_plan.projection
        shared_edges = projection.edges.loc[
            projection.edges["edge_type"].eq(
                "SHARED_EXTERNAL_COUNTERPARTY"
            )
        ]
        assert len(shared_edges) == len(LINKED_CUSTOMER_IDS)
        assert shared_edges["relationship_status"].eq(
            "COUNTERPARTY_SUPPRESSED_INSUFFICIENT_EVIDENCE"
        ).all()

        repeated = execute_scenario_5_live_counterparty_decision(
            runtime_directory=runtime_directory,
            run_date=RUN_DATE,
            settings=settings,
            adapter_factory=forbidden_factory,
        )
        assert repeated.telemetry["calls_executed"] == 0
        assert repeated.telemetry[
            "counterparty_ai_actions_queued"
        ] == 0
        assert (
            repeated.telemetry["applied_decision"]
            == "INSUFFICIENT_EVIDENCE_SUPPRESS"
        )
        assert repeated.termination is not None

        generate_scenario_5_source_data(
            source_directory,
            changed_evidence=True,
        )
        build_evidence(
            source_directory=source_directory,
            runtime_directory=runtime_directory,
        )

        policy_reassessment = (
            execute_scenario_5_live_counterparty_decision(
                runtime_directory=runtime_directory,
                run_date=RUN_DATE,
                settings=DailyAiSettings(
                    live_ai_enabled=False,
                    daily_call_limit=5,
                    run_call_limit=1,
                ),
                adapter_factory=forbidden_factory,
            )
        )
        reassessment_queue = (
            policy_reassessment.frontier_result
            .controlled_run.final_plan.actionable_queue
        )
        assert policy_reassessment.telemetry[
            "calls_executed"
        ] == 0
        assert policy_reassessment.telemetry[
            "applied_decision"
        ] == ""
        assert policy_reassessment.telemetry[
            "counterparty_ai_actions_queued"
        ] == 1
        assert reassessment_queue[
            "action_type"
        ].eq("RUN_COUNTERPARTY_AI").sum() == 1
        assert reassessment_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI").sum() == 0
        assert policy_reassessment.termination is None

        failure_runtime = root / "failure_runtime"
        shutil.copytree(
            runtime_directory / "counterparty_evidence",
            failure_runtime / "counterparty_evidence",
        )
        failure_adapter = Scenario5FailureAdapter()
        failed = execute_scenario_5_live_counterparty_decision(
            runtime_directory=failure_runtime,
            run_date=RUN_DATE,
            settings=settings,
            reset_state=True,
            adapter_factory=lambda: failure_adapter,
        )
        assert len(failure_adapter.calls) == 1
        assert failed.telemetry["calls_executed"] == 1
        assert failed.telemetry["applied_decision"] == ""
        assert (
            failed.telemetry[
                "suppressed_non_seed_customer_count"
            ]
            == 0
        )
        assert failed.telemetry["failed_frontier_count"] == 1
        assert failed.telemetry["termination_status"] == ""
        assert failed.telemetry["termination_reason"] == ""
        assert failed.termination is None
        assert (
            failed.frontier_result.decision_store.empty
        )

    print("Scenario 5 live counterparty decision smoke test passed.")
    print("Counterparty AI calls executed in test: 1")
    print("Decision: INSUFFICIENT_EVIDENCE_SUPPRESS")
    print(
        "Suppressed non-seed customer relationships: "
        f"{len(LINKED_CUSTOMER_IDS)}"
    )
    print("Customer AI actions queued: 0")
    print("Recursive sources queued: 0")
    print("Observed graph nodes/edges preserved: 4/3")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Unchanged repeated AI calls: 0")
    print("Changed policy/evidence requeued counterparty: passed")
    print("Stale historical decision not applied: passed")
    print("Failure path remained failed closed: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
