"""Validate Scenario 6C explicit technical reprocessing."""

from __future__ import annotations

import hashlib
import sys
from datetime import date
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
    OperationalStateIntegrityError,
    validate_persisted_operational_state,
)
from network_mule_discovery.technical_reprocessing import (
    CsvTechnicalReprocessingLedger,
    TechnicalReprocessingError,
    requeue_failed_frontier_item,
)
from smoke_test_scenario_6a_restart_safe_execution import (
    COUNTERPARTY_KEYS,
    GROUP_ID,
    RUN_DATE,
    SEED_ENTITY_KEY,
    build_restart_safe_network,
    subject_hashes,
)


FAILED_KEY = sorted(COUNTERPARTY_KEYS)[0]
COMPLETED_KEY = sorted(COUNTERPARTY_KEYS)[1]
REQUEUE_REQUEST_ID = "OPS-SCENARIO-6C-0001"
REQUEUE_TIMESTAMP = "2026-07-21T13:00:00+00:00"


def stable_decision_id(
    *,
    subject_type: str,
    subject_key: str,
    feature_snapshot_hash: str,
) -> str:
    """Create one deterministic test decision identifier."""
    digest = hashlib.sha256(
        "|".join(
            [
                subject_type,
                subject_key,
                feature_snapshot_hash,
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"S6C{digest}"


def suppression_decision(
    *,
    subject_type: str,
    subject_key: str,
    feature_snapshot_hash: str,
    source: str,
) -> dict[str, str]:
    """Return one deterministic successful suppression decision."""
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
        "reason_code": "SCENARIO_6C_CONTROLLED_REPROCESSING",
        "decision_version": "scenario-6c-test-v1",
        "decided_at": "2026-07-21 13:05:00",
        "source": source,
    }


class InitialMixedAdapter:
    """Fail one counterparty while completing the unrelated one."""

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
        assert subject_key in COUNTERPARTY_KEYS
        assert feature_payload_json
        assert run_date == RUN_DATE
        assert round_number == 1
        assert sequence_number in {1, 2}

        self.calls.append(subject_key)

        if subject_key == FAILED_KEY:
            raise OpenAIDecisionError(
                "SCENARIO_6C_SYNTHETIC_TIMEOUT",
                "Synthetic technical failure for explicit requeue.",
                response_id="resp_s6c_failed",
                request_id="req_s6c_failed",
                response_status="incomplete",
                incomplete_reason="timeout",
            )

        assert subject_key == COMPLETED_KEY

        return suppression_decision(
            subject_type=subject_type,
            subject_key=subject_key,
            feature_snapshot_hash=feature_snapshot_hash,
            source="SCENARIO_6C_INITIAL_ADAPTER",
        )


class RetrySuccessAdapter:
    """Complete only the explicitly requeued failed subject."""

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
        assert subject_key == FAILED_KEY
        assert feature_payload_json
        assert run_date == RUN_DATE
        assert round_number == 1
        assert sequence_number == 1

        self.calls.append(subject_key)

        return suppression_decision(
            subject_type=subject_type,
            subject_key=subject_key,
            feature_snapshot_hash=feature_snapshot_hash,
            source="SCENARIO_6C_RETRY_ADAPTER",
        )


def forbidden_factory() -> object:
    raise AssertionError(
        "Failed-closed or completed work must not instantiate the adapter."
    )


def frame_signature(
    frame: pd.DataFrame,
    *,
    sort_columns: list[str],
) -> tuple[tuple[str, ...], ...]:
    """Return a deterministic whole-frame signature."""
    if frame.empty:
        return ()

    prepared = (
        frame.astype("string")
        .sort_values(
            by=sort_columns,
            kind="stable",
        )
        .reset_index(drop=True)
    )

    return tuple(
        tuple(row)
        for row in prepared.itertuples(
            index=False,
            name=None,
        )
    )


def assert_integrity_rejects_duplicate_requeue(
    *,
    snapshot,
    ai_call_ledger: pd.DataFrame,
    reprocessing_ledger: pd.DataFrame,
) -> None:
    """Prove duplicate audit rows are rejected."""
    corrupted = pd.concat(
        [
            reprocessing_ledger,
            reprocessing_ledger.iloc[[0]].copy(),
        ],
        ignore_index=True,
    )
    corrupted.loc[
        corrupted.index[-1],
        "requeue_event_id",
    ] = "TR-CORRUPTED-DUPLICATE"

    try:
        validate_persisted_operational_state(
            snapshot=snapshot,
            ai_call_ledger=ai_call_ledger,
            technical_reprocessing_ledger=corrupted,
        )
    except OperationalStateIntegrityError:
        return

    raise AssertionError(
        "Duplicate requeue rows for one failed attempt were accepted."
    )


def main() -> None:
    """Prove one explicit requeue produces exactly one retry."""
    with TemporaryDirectory() as directory:
        state_directory = Path(directory)
        state_store = CsvDailyStateStore(state_directory)
        state_store.save_network_state(
            network=build_restart_safe_network(),
            run_date=RUN_DATE,
        )

        initial_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
        )
        assert initial_plan.queued_ai_action_count == 2

        initial_snapshot = state_store.load_snapshot()
        initial_node_ids = tuple(
            sorted(
                initial_snapshot.network.nodes["node_id"]
                .astype("string")
            )
        )
        initial_edge_ids = tuple(
            sorted(
                initial_snapshot.network.edges["edge_id"]
                .astype("string")
            )
        )
        initial_hashes = subject_hashes(initial_plan)
        initial_expansion_signature = frame_signature(
            initial_snapshot.expansion_ledger,
            sort_columns=["queue_item_id"],
        )

        initial_adapter = InitialMixedAdapter()
        initial_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=2,
            ),
            adapter_factory=lambda: initial_adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert initial_run.calls_executed == 2
        assert sorted(initial_adapter.calls) == sorted(
            COUNTERPARTY_KEYS
        )
        assert initial_run.final_plan.failed_closed_item_count == 1
        assert initial_run.final_plan.actionable_queue.empty

        status_counts = (
            initial_run.executed_actions["execution_status"]
            .value_counts()
            .to_dict()
        )
        assert status_counts == {
            "FAILED_CLOSED": 1,
            "COMPLETED": 1,
        }

        after_failure_store = CsvDailyStateStore(
            state_directory
        )
        after_failure_snapshot = after_failure_store.load_snapshot()
        failed_frontier = after_failure_snapshot.frontier_queue.loc[
            after_failure_snapshot.frontier_queue["queue_status"]
            .astype("string")
            .str.upper()
            .eq("FAILED_CLOSED")
        ]
        assert len(failed_frontier) == 1
        failed_item = failed_frontier.iloc[0]
        assert failed_item["subject_key"] == FAILED_KEY
        assert int(failed_item["attempt_count"]) == 1
        assert (
            failed_item["last_error_code"]
            == "SCENARIO_6C_SYNTHETIC_TIMEOUT"
        )
        assert failed_item["last_response_id"] == "resp_s6c_failed"
        assert failed_item["last_request_id"] == "req_s6c_failed"

        completed_decision = after_failure_snapshot.decision_store.loc[
            after_failure_snapshot.decision_store["subject_key"].eq(
                COMPLETED_KEY
            )
        ]
        assert len(completed_decision) == 1
        completed_decision_signature = frame_signature(
            completed_decision,
            sort_columns=["decision_id"],
        )

        initial_call_ledger = CsvAiCallLedger(
            state_directory
        ).load()
        assert len(initial_call_ledger) == 2
        assert (
            initial_call_ledger["call_status"]
            .value_counts()
            .to_dict()
            == {
                "FAILED_CLOSED": 1,
                "COMPLETED": 1,
            }
        )

        no_automatic_retry = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=10,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert no_automatic_retry.calls_before_run == 2
        assert no_automatic_retry.calls_executed == 0
        assert no_automatic_retry.final_plan.failed_closed_item_count == 1
        assert no_automatic_retry.final_plan.actionable_queue.empty

        queue_item_id = str(failed_item["queue_item_id"])
        requeue_result = requeue_failed_frontier_item(
            state_directory=state_directory,
            queue_item_id=queue_item_id,
            requeue_request_id=REQUEUE_REQUEST_ID,
            requested_at=REQUEUE_TIMESTAMP,
            requested_by="SCENARIO_6C_TEST_OPERATOR",
            requeue_reason=(
                "Retry after confirmed transient synthetic timeout."
            ),
            expected_attempt_count=1,
        )
        assert requeue_result.applied
        assert not requeue_result.already_recorded
        assert requeue_result.subject_key == FAILED_KEY
        assert requeue_result.prior_attempt_count == 1
        assert requeue_result.resulting_queue_status == "READY"

        duplicate_request = requeue_failed_frontier_item(
            state_directory=state_directory,
            queue_item_id=queue_item_id,
            requeue_request_id=REQUEUE_REQUEST_ID,
            requested_at=REQUEUE_TIMESTAMP,
            requested_by="SCENARIO_6C_TEST_OPERATOR",
            requeue_reason=(
                "Retry after confirmed transient synthetic timeout."
            ),
            expected_attempt_count=1,
        )
        assert not duplicate_request.applied
        assert duplicate_request.already_recorded
        assert (
            duplicate_request.requeue_event_id
            == requeue_result.requeue_event_id
        )

        try:
            requeue_failed_frontier_item(
                state_directory=state_directory,
                queue_item_id=queue_item_id,
                requeue_request_id="OPS-SCENARIO-6C-0002",
                requested_at="2026-07-21T13:01:00+00:00",
                requested_by="SCENARIO_6C_TEST_OPERATOR",
                requeue_reason="Invalid duplicate requeue request.",
                expected_attempt_count=1,
            )
        except TechnicalReprocessingError:
            pass
        else:
            raise AssertionError(
                "A READY item accepted another technical requeue."
            )

        reprocessing_ledger = CsvTechnicalReprocessingLedger(
            state_directory
        ).load()
        assert len(reprocessing_ledger) == 1
        assert (
            reprocessing_ledger.iloc[0]["prior_error_code"]
            == "SCENARIO_6C_SYNTHETIC_TIMEOUT"
        )
        assert (
            reprocessing_ledger.iloc[0]["prior_attempt_count"]
            == "1"
        )

        requeued_plan = build_incremental_daily_plan(
            state_store=CsvDailyStateStore(state_directory),
            run_date=RUN_DATE,
        )
        assert requeued_plan.failed_closed_item_count == 0
        assert len(requeued_plan.actionable_queue) == 1
        assert (
            requeued_plan.actionable_queue.iloc[0]["subject_key"]
            == FAILED_KEY
        )
        assert (
            requeued_plan.actionable_queue.iloc[0]["queue_item_id"]
            == queue_item_id
        )
        assert int(
            requeued_plan.frontier_queue.iloc[0]["attempt_count"]
        ) == 1
        assert (
            requeued_plan.frontier_queue.iloc[0]["last_error_code"]
            == "SCENARIO_6C_SYNTHETIC_TIMEOUT"
        )

        before_retry_snapshot = CsvDailyStateStore(
            state_directory
        ).load_snapshot()
        assert frame_signature(
            before_retry_snapshot.decision_store.loc[
                before_retry_snapshot.decision_store["subject_key"].eq(
                    COMPLETED_KEY
                )
            ],
            sort_columns=["decision_id"],
        ) == completed_decision_signature
        assert frame_signature(
            before_retry_snapshot.expansion_ledger,
            sort_columns=["queue_item_id"],
        ) == initial_expansion_signature
        assert tuple(
            sorted(
                before_retry_snapshot.network.nodes["node_id"]
                .astype("string")
            )
        ) == initial_node_ids
        assert tuple(
            sorted(
                before_retry_snapshot.network.edges["edge_id"]
                .astype("string")
            )
        ) == initial_edge_ids
        assert subject_hashes(requeued_plan) == initial_hashes

        retry_adapter = RetrySuccessAdapter()
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
        assert retry_adapter.calls == [FAILED_KEY]
        assert retry_run.final_plan.actionable_queue.empty
        assert retry_run.final_plan.failed_closed_item_count == 0

        final_store = CsvDailyStateStore(state_directory)
        final_snapshot = final_store.load_snapshot()
        final_call_ledger = CsvAiCallLedger(
            state_directory
        ).load()
        final_reprocessing_ledger = CsvTechnicalReprocessingLedger(
            state_directory
        ).load()

        assert len(final_call_ledger) == 3
        failed_subject_calls = final_call_ledger.loc[
            final_call_ledger["subject_key"].eq(FAILED_KEY)
        ].sort_values(by="attempted_at", kind="stable")
        assert len(failed_subject_calls) == 2
        assert set(failed_subject_calls["call_status"]) == {
            "FAILED_CLOSED",
            "COMPLETED",
        }
        assert len(final_snapshot.decision_store) == 2
        assert (
            final_snapshot.decision_store["subject_key"].nunique()
            == 2
        )
        assert frame_signature(
            final_snapshot.decision_store.loc[
                final_snapshot.decision_store["subject_key"].eq(
                    COMPLETED_KEY
                )
            ],
            sort_columns=["decision_id"],
        ) == completed_decision_signature
        assert frame_signature(
            final_snapshot.expansion_ledger,
            sort_columns=["queue_item_id"],
        ) == initial_expansion_signature
        assert tuple(
            sorted(
                final_snapshot.network.nodes["node_id"]
                .astype("string")
            )
        ) == initial_node_ids
        assert tuple(
            sorted(
                final_snapshot.network.edges["edge_id"]
                .astype("string")
            )
        ) == initial_edge_ids

        integrity = validate_persisted_operational_state(
            snapshot=final_snapshot,
            ai_call_ledger=final_call_ledger,
            technical_reprocessing_ledger=(
                final_reprocessing_ledger
            ),
        )
        assert integrity.decision_count == 2
        assert integrity.ai_call_count == 3
        assert integrity.completed_ai_outcome_count == 2
        assert integrity.technical_requeue_count == 1

        assert_integrity_rejects_duplicate_requeue(
            snapshot=final_snapshot,
            ai_call_ledger=final_call_ledger,
            reprocessing_ledger=final_reprocessing_ledger,
        )

        termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=None,
            group_ids=[GROUP_ID],
            source_entity_key=SEED_ENTITY_KEY,
        )
        assert termination.final_plan.actionable_queue.empty
        assert termination.final_plan.failed_closed_item_count == 0
        termination_row = termination.termination_status.iloc[0]
        assert termination_row["termination_status"] == "TERMINATED"
        assert termination_row["termination_reason"] == "FRONTIER_EXHAUSTED"

        unchanged_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=10,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert unchanged_run.calls_before_run == 3
        assert unchanged_run.calls_executed == 0
        assert unchanged_run.final_plan.actionable_queue.empty
        assert unchanged_run.final_plan.failed_closed_item_count == 0

    print("Scenario 6C controlled reprocessing smoke test passed.")
    print("Initial AI calls attempted: 2")
    print("Initial completed/failed-closed actions: 1/1")
    print("Automatic retries before explicit requeue: 0")
    print("Explicit failed items requeued: 1")
    print("Duplicate requeue request audit rows: 0")
    print("Unrelated decisions and graph state changed: 0")
    print("Retry AI calls executed: 1")
    print("Failed call history retained: passed")
    print("Successful retry decision persisted once: passed")
    print("Technical reprocessing ledger rows: 1 unique")
    print("Final unchanged run AI calls: 0")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
