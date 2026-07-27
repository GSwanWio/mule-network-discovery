"""Validate insufficient-evidence suppression and material reassessment."""

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
for path in (PROJECT_ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.behavioral_features import COUNTERPARTY_PAYLOAD_FILENAME
from network_mule_discovery.daily_ai_runner import DailyAiSettings
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
    run_counterparty_ai_frontier,
)
from network_mule_discovery.frontier_termination import run_frontier_exhaustion_termination
from network_mule_discovery.scenario_5_synthetic_data import (
    AMBIGUOUS_COUNTERPARTY_ACCOUNT,
    LINKED_CUSTOMER_IDS,
    RUN_DATE,
    SEED_CUSTOMER_ID,
    generate_scenario_5_source_data,
)


class InsufficientEvidenceAdapter:
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
        payload = json.loads(feature_payload_json)
        assert payload["subject_key"] == subject_key
        assert payload["behavioral_evidence"]["aggregate_behavior"]["distinct_customer_count"] == 3
        self.calls.append(subject_key)
        self.last_call_metadata = {
            "model": "offline-insufficient-test",
            "prompt_version": "scenario-5-test-v1",
            "response_id": "resp_scenario_5_test",
            "request_id": "req_scenario_5_test",
            "response_status": "completed",
            "incomplete_reason": "",
            "input_tokens": 900,
            "output_tokens": 100,
            "reasoning_tokens": 30,
            "assessment": {
                "decision": "INSUFFICIENT_EVIDENCE_SUPPRESS",
                "reason_code": "LOW_VOLUME_AMBIGUOUS_SHARED_COUNTERPARTY",
                "rationale": "The small, low-value, non-rapid pattern is insufficient for expansion.",
                "key_evidence": ["Only three customers and three payments are observed."],
                "confidence": "MEDIUM",
            },
        }
        digest = hashlib.sha256(
            f"{subject_key}|{feature_snapshot_hash}".encode("utf-8")
        ).hexdigest()[:16]
        return {
            "decision_id": f"S5{digest}",
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": feature_snapshot_hash,
            "decision": "INSUFFICIENT_EVIDENCE_SUPPRESS",
            "reason_code": "LOW_VOLUME_AMBIGUOUS_SHARED_COUNTERPARTY",
            "decision_version": "scenario-5-test-v1",
            "decided_at": "2026-07-20 12:00:00",
            "source": "TEST_DECISION_ADAPTER",
        }


def forbidden_factory() -> object:
    raise AssertionError("Unchanged evidence must not instantiate the adapter.")


def build_evidence(source_directory: Path, work_directory: Path) -> Path:
    subprocess.run(
        [
            sys.executable,
            str(SCRIPTS_DIRECTORY / "run_scenario_5_counterparty_evidence.py"),
            "--source-directory", str(source_directory),
            "--work-directory", str(work_directory),
            "--run-date", str(RUN_DATE),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return work_directory / "counterparty_evidence"


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        initial_source = root / "initial_source"
        changed_source = root / "changed_source"
        initial_work = root / "initial_work"
        changed_work = root / "changed_work"
        state_directory = root / "state"

        generate_scenario_5_source_data(initial_source)
        initial_evidence = build_evidence(initial_source, initial_work)
        unified = load_unified_result(initial_evidence)
        payloads = load_supplemental_subject_payloads(
            initial_evidence / "features" / COUNTERPARTY_PAYLOAD_FILENAME
        )
        assert len(unified.nodes) == 4
        assert len(unified.edges) == 3

        settings = DailyAiSettings(live_ai_enabled=True, daily_call_limit=5, run_call_limit=1)
        adapter = InsufficientEvidenceAdapter()
        first = run_counterparty_ai_frontier(
            unified_result=unified,
            supplemental_subject_payloads=payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=settings,
            reset_state=True,
            adapter_factory=lambda: adapter,
        )
        assert first.controlled_run.calls_executed == 1
        assert len(adapter.calls) == 1
        assert first.decision_store.iloc[-1]["decision"] == "INSUFFICIENT_EVIDENCE_SUPPRESS"
        assert first.controlled_run.final_plan.actionable_queue.empty
        projection = first.controlled_run.final_plan.projection
        shared_edges = projection.edges.loc[
            projection.edges["edge_type"].eq("SHARED_EXTERNAL_COUNTERPARTY")
        ]
        assert len(shared_edges) == len(LINKED_CUSTOMER_IDS)
        assert shared_edges["relationship_status"].eq(
            "COUNTERPARTY_SUPPRESSED_INSUFFICIENT_EVIDENCE"
        ).all()
        seed_nodes = projection.nodes.loc[
            projection.nodes["node_roles"]
            .astype("string")
            .str.contains("SEED_MULE")
        ]

        non_seed_nodes = projection.nodes.loc[
            ~projection.nodes["node_roles"]
            .astype("string")
            .str.contains("SEED_MULE")
        ]

        assert len(seed_nodes) == 1
        assert seed_nodes[
            "expansion_source_flag"
        ].astype(bool).all()

        assert len(non_seed_nodes) == 3
        assert not non_seed_nodes[
            "expansion_source_flag"
        ].astype(bool).any()

        termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=payloads,
            group_ids=unified.groups["group_id"].astype("string").tolist(),
            source_entity_key=f"RETAIL|{SEED_CUSTOMER_ID}",
        )
        assert termination.termination_status.iloc[0]["termination_reason"] == "FRONTIER_EXHAUSTED"

        repeated = run_counterparty_ai_frontier(
            unified_result=unified,
            supplemental_subject_payloads=payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=settings,
            adapter_factory=forbidden_factory,
        )
        assert repeated.controlled_run.calls_executed == 0

        generate_scenario_5_source_data(changed_source, changed_evidence=True)
        changed_evidence = build_evidence(changed_source, changed_work)
        changed_unified = load_unified_result(changed_evidence)
        changed_payloads = load_supplemental_subject_payloads(
            changed_evidence / "features" / COUNTERPARTY_PAYLOAD_FILENAME
        )
        changed = run_counterparty_ai_frontier(
            unified_result=changed_unified,
            supplemental_subject_payloads=changed_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(live_ai_enabled=False, daily_call_limit=5, run_call_limit=1),
        )
        queue = changed.controlled_run.final_plan.actionable_queue
        assert len(queue) == 1
        assert queue.iloc[0]["action_type"] == "RUN_COUNTERPARTY_AI"
        assert queue.iloc[0]["subject_key"].endswith(AMBIGUOUS_COUNTERPARTY_ACCOUNT)

    print("Scenario 5 insufficient counterparty smoke test passed.")
    print("Initial counterparty AI actions executed: 1")
    print("Decision: INSUFFICIENT_EVIDENCE_SUPPRESS")
    print(f"Suppressed non-seed customer relationships: {len(LINKED_CUSTOMER_IDS)}")
    print("Customer AI actions queued: 0")
    print("Recursive sources queued: 0")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Unchanged repeated AI calls: 0")
    print("Material evidence change requeued counterparty: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
