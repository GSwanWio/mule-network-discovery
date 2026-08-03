"""Validate per-item fail-closed incremental AI execution."""

from __future__ import annotations

import hashlib
import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.incremental_processor import (
    execute_incremental_ai_actions,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionError,
)
from smoke_test_daily_changed_evidence import (
    add_day_two_counterparty_evidence,
    select_source_customer,
)
from smoke_test_daily_incremental_state import (
    DAY_ONE,
    DAY_TWO,
    run_day_one,
)


def stable_id(
    prefix: str,
    *values: object,
) -> str:
    """Create a deterministic test identifier."""
    canonical_value = "|".join(
        str(value)
        for value in values
    )

    digest = hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()[:16]

    return f"{prefix}{digest}"


class MixedResultAdapter:
    """Fail one subject and complete another."""

    def __init__(
        self,
        *,
        failed_key: str,
        completed_key: str,
    ) -> None:
        self.failed_key = failed_key
        self.completed_key = completed_key
        self.calls: list[str] = []
        self.last_call_metadata = {
            "model": "fail-closed-test-model",
            "prompt_version": (
                "fail-closed-test-prompt-v1"
            ),
        }

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
        """Return one success or one synthetic failure."""
        assert subject_type == "COUNTERPARTY"
        assert feature_payload_json

        self.calls.append(subject_key)

        if subject_key == self.failed_key:
            raise OpenAIDecisionError(
                "SYNTHETIC_API_FAILURE",
                "Synthetic per-item failure.",
            )

        assert subject_key == self.completed_key

        return {
            "decision_id": stable_id(
                "FD",
                subject_type,
                subject_key,
                feature_snapshot_hash,
                "LEGITIMATE_SUPPRESS",
            ),
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "decision": "LEGITIMATE_SUPPRESS",
            "reason_code": (
                "SYNTHETIC_LEGITIMATE_RELATIONSHIP"
            ),
            "decision_version": (
                "fail-closed-test-v1"
            ),
            "decided_at": (
                "2026-07-17 21:00:00"
            ),
            "source": "TEST_DECISION_ADAPTER",
        }


def select_counterparties(
    nodes: pd.DataFrame,
) -> pd.DataFrame:
    """Select two independently decided counterparties."""
    candidates = (
        nodes.loc[
            nodes["node_type"].eq(
                "COUNTERPARTY"
            )
            & nodes["applied_decision_id"]
            .astype("string")
            .str.strip()
            .ne("")
        ]
        .sort_values(
            by=[
                "counterparty_key",
                "group_id",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=["counterparty_key"],
            keep="first",
        )
        .head(2)
        .reset_index(drop=True)
    )

    if len(candidates) != 2:
        raise AssertionError(
            "The test requires two decided "
            "counterparties."
        )

    return candidates


def main() -> None:
    """Ensure one failure does not stop another item."""
    day_one_result = run_day_one()

    with TemporaryDirectory() as directory:
        state_store = CsvDailyStateStore(
            Path(directory)
        )

        state_store.commit_recursive_result(
            result=day_one_result,
            run_date=DAY_ONE,
        )

        snapshot = state_store.load_snapshot()

        selected = select_counterparties(
            snapshot.network.nodes
        )

        changed_network = snapshot.network

        for counterparty in (
            selected.to_dict(
                orient="records"
            )
        ):
            counterparty_row = pd.Series(
                counterparty
            )

            source_customer = (
                select_source_customer(
                    nodes=changed_network.nodes,
                    group_id=str(
                        counterparty_row[
                            "group_id"
                        ]
                    ),
                )
            )

            changed_network = (
                add_day_two_counterparty_evidence(
                    network=changed_network,
                    counterparty_node=(
                        counterparty_row
                    ),
                    source_customer=(
                        source_customer
                    ),
                )
            )

        state_store.save_network_state(
            network=changed_network,
            run_date=DAY_TWO,
        )

        initial_plan = (
            build_incremental_daily_plan(
                state_store=state_store,
                run_date=DAY_TWO,
            )
        )

        ai_queue = initial_plan.actionable_queue.loc[
            initial_plan.actionable_queue[
                "action_type"
            ].eq("RUN_COUNTERPARTY_AI")
        ]

        assert len(ai_queue) == 2

        subject_keys = sorted(
            ai_queue[
                "subject_key"
            ].astype("string")
        )

        failed_key = subject_keys[0]
        completed_key = subject_keys[1]

        adapter = MixedResultAdapter(
            failed_key=failed_key,
            completed_key=completed_key,
        )

        result = execute_incremental_ai_actions(
            state_store=state_store,
            decision_adapter=adapter,
            run_date=DAY_TWO,
            max_ai_calls=2,
        )

        assert sorted(adapter.calls) == subject_keys

        status_counts = (
            result.executed_actions[
                "execution_status"
            ]
            .value_counts()
            .to_dict()
        )

        assert status_counts == {
            "FAILED_CLOSED": 1,
            "COMPLETED": 1,
        }

        failed_execution = (
            result.executed_actions.loc[
                result.executed_actions[
                    "execution_status"
                ].eq("FAILED_CLOSED")
            ]
            .iloc[0]
        )

        assert (
            failed_execution["model"]
            == "fail-closed-test-model"
        )
        assert (
            failed_execution["prompt_version"]
            == "fail-closed-test-prompt-v1"
        )

        assert len(
            result.generated_decisions
        ) == 1

        assert (
            result.generated_decisions
            .iloc[0]["subject_key"]
            == completed_key
        )

        persisted_frontier = (
            state_store.load_frontier_queue()
        )

        failed_items = persisted_frontier.loc[
            persisted_frontier[
                "queue_status"
            ].eq("FAILED_CLOSED")
        ]

        assert len(failed_items) == 1

        failed_item = failed_items.iloc[0]

        assert (
            failed_item["subject_key"]
            == failed_key
        )

        assert (
            failed_item["last_error_code"]
            == "SYNTHETIC_API_FAILURE"
        )

        assert (
            int(failed_item["attempt_count"])
            == 1
        )

        assert (
            result.refreshed_plan
            .failed_closed_item_count
            == 1
        )

        assert (
            result.refreshed_plan
            .actionable_queue
            .empty
        )

        failed_node = (
            result.refreshed_plan
            .projection
            .nodes
            .loc[
                lambda frame: (
                    frame["counterparty_key"]
                    .eq(failed_key)
                )
            ]
        )

        assert not failed_node.empty

        assert failed_node[
            "node_status"
        ].eq(
            "COUNTERPARTY_AI_FAILED_CLOSED"
        ).all()

        completed_node = (
            result.refreshed_plan
            .projection
            .nodes
            .loc[
                lambda frame: (
                    frame["counterparty_key"]
                    .eq(completed_key)
                )
            ]
        )

        assert completed_node[
            "node_status"
        ].eq(
            "COUNTERPARTY_SUPPRESSED_LEGITIMATE"
        ).all()

        call_count_before_repeat = len(
            adapter.calls
        )

        repeated_result = (
            execute_incremental_ai_actions(
                state_store=state_store,
                decision_adapter=adapter,
                run_date=DAY_TWO,
                max_ai_calls=2,
            )
        )

        assert len(adapter.calls) == (
            call_count_before_repeat
        )

        assert (
            repeated_result
            .executed_actions
            .empty
        )

        assert (
            repeated_result
            .refreshed_plan
            .failed_closed_item_count
            == 1
        )

    print(
        "Incremental fail-closed smoke test passed."
    )

    print("Selected AI actions: 2")
    print("Completed AI actions: 1")
    print("Failed-closed AI actions: 1")
    print("Failed-call model identity: passed")
    print("Failed-call prompt identity: passed")
    print("Successful decision persisted: passed")
    print("Failed item decision created: 0")
    print("Unrelated item continued: passed")
    print("Failed item automatically retried: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
