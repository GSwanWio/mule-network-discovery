"""Validate Scenario 6D consolidated operational-resilience gate."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

try:
    from openai import OpenAI as _OpenAI  # noqa: F401
except (ImportError, AttributeError):
    import types

    openai_stub = types.ModuleType("openai")

    class _OfflineOpenAIStub:
        pass

    openai_stub.OpenAI = _OfflineOpenAIStub
    sys.modules["openai"] = openai_stub


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
    run_controlled_daily_ai,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_termination import (
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionError,
)
from network_mule_discovery.operational_resilience import (
    OPERATIONAL_RESILIENCE_GATE_FILENAME,
    build_operational_resilience_gate_report,
    persist_operational_resilience_gate_report,
)
from network_mule_discovery.recursive_expansion import (
    merge_expansion_relationships,
)
from network_mule_discovery.technical_reprocessing import (
    CsvTechnicalReprocessingLedger,
    requeue_failed_frontier_item,
)
import smoke_test_scenario_6b_cycle_duplicate_protection as scenario_6b


RUN_DATE = scenario_6b.RUN_DATE
GROUP_ID = scenario_6b.GROUP_ID
SOURCE_ENTITY_KEY = scenario_6b.ENTITY_A
COUNTERPARTY_KEYS = (
    scenario_6b.COUNTERPARTY_1,
    scenario_6b.COUNTERPARTY_2,
)
REQUEUE_REQUEST_ID = "OPS-SCENARIO-6D-0001"
REQUEUE_TIMESTAMP = "2026-07-22T15:00:00+00:00"


def stable_decision_id(
    *,
    subject_type: str,
    subject_key: str,
    feature_snapshot_hash: str,
) -> str:
    """Return one deterministic final-gate decision ID."""
    digest = hashlib.sha256(
        "|".join(
            [
                subject_type,
                subject_key,
                feature_snapshot_hash,
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"S6D{digest}"


class FinalGateAdapter:
    """Fail or complete exactly one expected counterparty subject."""

    def __init__(
        self,
        *,
        expected_subject_key: str,
        should_fail: bool,
        source: str,
    ) -> None:
        self.expected_subject_key = expected_subject_key
        self.should_fail = should_fail
        self.source = source
        self.calls: list[str] = []

    def decide(
        self,
        *,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        feature_payload_json: str,
        run_date,
        round_number: int,
        sequence_number: int,
    ) -> dict[str, str]:
        assert subject_type == "COUNTERPARTY"
        assert subject_key == self.expected_subject_key
        assert feature_payload_json
        assert run_date == RUN_DATE
        assert round_number == 1
        assert sequence_number == 1

        self.calls.append(subject_key)

        if self.should_fail:
            raise OpenAIDecisionError(
                "SCENARIO_6D_SYNTHETIC_TIMEOUT",
                "Synthetic transient failure for the final gate.",
                response_id="resp_s6d_failed",
                request_id="req_s6d_failed",
                response_status="incomplete",
                incomplete_reason="timeout",
            )

        return {
            "decision_id": stable_decision_id(
                subject_type=subject_type,
                subject_key=subject_key,
                feature_snapshot_hash=feature_snapshot_hash,
            ),
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": feature_snapshot_hash,
            "decision": "LEGITIMATE_SUPPRESS",
            "reason_code": "SCENARIO_6D_FINAL_GATE_SUPPRESSION",
            "decision_version": "scenario-6d-test-v1",
            "decided_at": "2026-07-22 15:05:00",
            "source": self.source,
        }


def forbidden_factory() -> object:
    raise AssertionError(
        "Terminal or failed-closed work must not instantiate the adapter."
    )


def build_cycle_graph() -> tuple[object, int]:
    """Build and replay one duplicate-safe cyclic observed graph."""
    first_relationships = scenario_6b.relationship_rows(
        source_entity_key=scenario_6b.ENTITY_A,
        target_entity_type="SME",
        target_entity_id="B6101",
        target_entity_key=scenario_6b.ENTITY_B,
        counterparty_key=scenario_6b.COUNTERPARTY_1,
        evidence_prefix="S6D-A-CP1",
    )
    second_relationships = scenario_6b.relationship_rows(
        source_entity_key=scenario_6b.ENTITY_B,
        target_entity_type="RETAIL",
        target_entity_id="R6101",
        target_entity_key=scenario_6b.ENTITY_A,
        counterparty_key=scenario_6b.COUNTERPARTY_2,
        evidence_prefix="S6D-B-CP2",
    )

    graph = merge_expansion_relationships(
        graph=scenario_6b.build_initial_graph(),
        relationships=first_relationships,
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    graph = merge_expansion_relationships(
        graph=graph,
        relationships=second_relationships,
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    assert len(graph.nodes) == 4
    assert len(graph.edges) == 4

    replayed = merge_expansion_relationships(
        graph=graph,
        relationships=pd.concat(
            [
                first_relationships,
                second_relationships,
            ],
            ignore_index=True,
        ),
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    repeated_expansion_count = (
        len(replayed.nodes)
        - len(graph.nodes)
        + len(replayed.edges)
        - len(graph.edges)
    )
    assert repeated_expansion_count == 0
    assert scenario_6b.logical_edge_keys(replayed) == (
        scenario_6b.logical_edge_keys(graph)
    )

    return replayed, repeated_expansion_count


def subject_hashes(plan) -> tuple[tuple[str, str, str], ...]:
    """Return deterministic subject-feature identities."""
    rows = plan.projection.subject_snapshots[
        [
            "subject_type",
            "subject_key",
            "feature_snapshot_hash",
        ]
    ].astype("string")

    return tuple(
        sorted(
            tuple(row)
            for row in rows.itertuples(
                index=False,
                name=None,
            )
        )
    )


def main() -> None:
    """Prove all Scenario 6 resilience controls in one persisted flow."""
    graph, repeated_expansion_count = build_cycle_graph()
    expected_node_ids = tuple(
        sorted(graph.nodes["node_id"].astype("string"))
    )
    expected_edge_ids = tuple(
        sorted(graph.edges["edge_id"].astype("string"))
    )

    with TemporaryDirectory() as directory:
        state_directory = Path(directory)
        state_store = CsvDailyStateStore(state_directory)
        state_store.save_network_state(
            network=graph,
            run_date=RUN_DATE,
        )

        expansion_rows = pd.DataFrame(
            [
                {
                    "run_date": str(RUN_DATE),
                    "round_number": "1",
                    "queue_item_id": "Q-S6D-A",
                    "source_entity_key": scenario_6b.ENTITY_A,
                    "group_ids": GROUP_ID,
                    "relationship_rows_found": "3",
                    "expansion_status": "COMPLETED",
                },
                {
                    "run_date": str(RUN_DATE),
                    "round_number": "2",
                    "queue_item_id": "Q-S6D-B",
                    "source_entity_key": scenario_6b.ENTITY_B,
                    "group_ids": GROUP_ID,
                    "relationship_rows_found": "3",
                    "expansion_status": "COMPLETED",
                },
            ]
        )
        state_store.append_expansion_ledger(expansion_rows)
        state_store.append_expansion_ledger(expansion_rows)
        assert len(state_store.load_expansion_ledger()) == 2

        initial_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
        )
        assert initial_plan.queued_ai_action_count == 2
        expected_subject_hashes = subject_hashes(initial_plan)
        ordered_subjects = tuple(
            initial_plan.actionable_queue["subject_key"]
            .astype("string")
            .tolist()
        )
        assert set(ordered_subjects) == set(COUNTERPARTY_KEYS)
        failed_key = ordered_subjects[0]
        completed_key = ordered_subjects[1]

        first_adapter = FinalGateAdapter(
            expected_subject_key=failed_key,
            should_fail=True,
            source="SCENARIO_6D_FIRST_PROCESS",
        )
        first_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=3,
                run_call_limit=1,
            ),
            adapter_factory=lambda: first_adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert first_run.calls_executed == 1
        assert first_adapter.calls == [failed_key]
        assert first_run.final_plan.failed_closed_item_count == 1
        assert first_run.final_plan.queued_ai_action_count == 1

        restarted_adapter = FinalGateAdapter(
            expected_subject_key=completed_key,
            should_fail=False,
            source="SCENARIO_6D_RESTARTED_PROCESS",
        )
        restarted_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=3,
                run_call_limit=1,
            ),
            adapter_factory=lambda: restarted_adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert restarted_run.calls_before_run == 1
        assert restarted_run.calls_executed == 1
        assert restarted_adapter.calls == [completed_key]
        assert restarted_run.final_plan.failed_closed_item_count == 1
        assert restarted_run.final_plan.actionable_queue.empty

        no_automatic_retry = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=3,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert no_automatic_retry.calls_before_run == 2
        assert no_automatic_retry.calls_executed == 0
        assert no_automatic_retry.final_plan.failed_closed_item_count == 1

        failed_frontier = (
            CsvDailyStateStore(state_directory)
            .load_snapshot()
            .frontier_queue
        )
        failed_frontier = failed_frontier.loc[
            failed_frontier["queue_status"]
            .astype("string")
            .str.upper()
            .eq("FAILED_CLOSED")
        ]
        assert len(failed_frontier) == 1
        failed_item = failed_frontier.iloc[0]
        assert failed_item["subject_key"] == failed_key
        assert int(failed_item["attempt_count"]) == 1

        requeue = requeue_failed_frontier_item(
            state_directory=state_directory,
            queue_item_id=str(failed_item["queue_item_id"]),
            requeue_request_id=REQUEUE_REQUEST_ID,
            requested_at=REQUEUE_TIMESTAMP,
            requested_by="SCENARIO_6D_TEST_OPERATOR",
            requeue_reason=(
                "Retry after confirmed transient final-gate timeout."
            ),
            expected_attempt_count=1,
        )
        assert requeue.applied
        assert not requeue.already_recorded
        assert requeue.subject_key == failed_key

        retry_adapter = FinalGateAdapter(
            expected_subject_key=failed_key,
            should_fail=False,
            source="SCENARIO_6D_EXPLICIT_RETRY",
        )
        retry_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=3,
                run_call_limit=1,
            ),
            adapter_factory=lambda: retry_adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert retry_run.calls_before_run == 2
        assert retry_run.calls_executed == 1
        assert retry_adapter.calls == [failed_key]
        assert retry_run.final_plan.actionable_queue.empty
        assert retry_run.final_plan.failed_closed_item_count == 0

        termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=None,
            group_ids=[GROUP_ID],
            source_entity_key=SOURCE_ENTITY_KEY,
        )
        assert termination.final_plan.actionable_queue.empty
        assert termination.final_plan.failed_closed_item_count == 0

        unchanged_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=4,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert unchanged_run.calls_before_run == 3
        assert unchanged_run.calls_executed == 0
        assert unchanged_run.final_plan.actionable_queue.empty

        repeated_termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=None,
            group_ids=[GROUP_ID],
            source_entity_key=SOURCE_ENTITY_KEY,
        )
        assert repeated_termination.final_plan.actionable_queue.empty

        final_store = CsvDailyStateStore(state_directory)
        final_snapshot = final_store.load_snapshot()
        final_call_ledger = CsvAiCallLedger(
            state_directory
        ).load()
        final_reprocessing_ledger = CsvTechnicalReprocessingLedger(
            state_directory
        ).load()

        report = build_operational_resilience_gate_report(
            run_date=str(RUN_DATE),
            snapshot=final_snapshot,
            ai_call_ledger=final_call_ledger,
            technical_reprocessing_ledger=(
                final_reprocessing_ledger
            ),
            termination_status=(
                repeated_termination.termination_status
            ),
            guardrail_telemetry=(
                repeated_termination.guardrail_telemetry
            ),
            current_subject_snapshots=(
                unchanged_run.final_plan.projection.subject_snapshots
            ),
            expected_node_ids=expected_node_ids,
            expected_edge_ids=expected_edge_ids,
            expected_subject_hashes=expected_subject_hashes,
            repeated_ai_call_count=unchanged_run.calls_executed,
            repeated_expansion_count=repeated_expansion_count,
        )
        assert report.gate_status == "PASSED"
        assert report.node_count == 4
        assert report.edge_count == 4
        assert report.decision_count == 2
        assert report.expansion_ledger_count == 2
        assert report.frontier_queue_count == 0
        assert report.ai_call_count == 3
        assert report.completed_ai_outcome_count == 2
        assert report.failed_ai_call_count == 1
        assert report.technical_requeue_count == 1
        assert report.repeated_ai_call_count == 0
        assert report.repeated_expansion_count == 0
        assert report.stable_node_ids
        assert report.stable_edge_ids
        assert report.stable_feature_snapshot_hashes

        report_path = persist_operational_resilience_gate_report(
            state_directory=state_directory,
            report=report,
        )
        assert report_path.name == OPERATIONAL_RESILIENCE_GATE_FILENAME
        persisted_report = json.loads(report_path.read_text())
        assert persisted_report == report.to_record()

        restarted_terminal_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=4,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert restarted_terminal_run.calls_before_run == 3
        assert restarted_terminal_run.calls_executed == 0
        assert restarted_terminal_run.final_plan.actionable_queue.empty

    print("Scenario 6D final operational-resilience gate passed.")
    print("Duplicate-safe cyclic graph nodes/edges: 4/4")
    print("First bounded run completed/failed-closed: 0/1")
    print("Restart completed remaining AI actions: 1")
    print("Automatic failed-item retries: 0")
    print("Explicit technical requeues/retry calls: 1/1")
    print("AI call ledger completed/failed/total: 2/1/3")
    print("Decision/expansion/reprocessing rows: 2/2/1 unique")
    print("Stable node, edge, and feature identities: passed")
    print("Repeated graph growth/AI calls: 0/0")
    print("Termination: TERMINATED/FRONTIER_EXHAUSTED")
    print("Guardrail telemetry: TELEMETRY_ONLY")
    print("Persisted final resilience report: passed")
    print("Restart after terminal state AI calls: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
