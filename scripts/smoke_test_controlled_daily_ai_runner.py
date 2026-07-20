"""Validate explicit enablement and persisted daily AI caps."""

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

from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
    run_controlled_daily_ai,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
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
from smoke_test_incremental_fail_closed import (
    select_counterparties,
)


class SuppressingAdapter:
    """Return one valid synthetic suppression decision."""

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
        """Return one valid deterministic decision."""
        self.calls.append(subject_key)

        canonical_key = "|".join(
            [
                subject_type,
                subject_key,
                feature_snapshot_hash,
            ]
        )

        digest = hashlib.sha256(
            canonical_key.encode("utf-8")
        ).hexdigest()[:16]

        return {
            "decision_id": f"CR{digest}",
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "decision": "LEGITIMATE_SUPPRESS",
            "reason_code": (
                "SYNTHETIC_CONTROLLED_SUPPRESSION"
            ),
            "decision_version": (
                "controlled-runner-test-v1"
            ),
            "decided_at": (
                "2026-07-20 12:00:00"
            ),
            "source": "TEST_DECISION_ADAPTER",
        }


def forbidden_factory() -> object:
    """Fail if a disabled or exhausted run creates an adapter."""
    raise AssertionError(
        "The decision adapter must not be instantiated."
    )


def main() -> None:
    """Prove default-off behavior and the persisted cap."""
    day_one_result = run_day_one()

    with TemporaryDirectory() as directory:
        state_directory = Path(directory)

        state_store = CsvDailyStateStore(
            state_directory
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

        for counterparty in selected.to_dict(
            orient="records"
        ):
            counterparty_row = pd.Series(
                counterparty
            )

            source_customer = (
                select_source_customer(
                    nodes=changed_network.nodes,
                    group_id=str(
                        counterparty_row["group_id"]
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

        disabled_result = (
            run_controlled_daily_ai(
                state_directory=state_directory,
                run_date=DAY_TWO,
                settings=DailyAiSettings(
                    live_ai_enabled=False,
                    daily_call_limit=1,
                    run_call_limit=1,
                ),
                adapter_factory=forbidden_factory,
            )
        )

        assert (
            disabled_result.calls_executed
            == 0
        )

        assert (
            disabled_result
            .final_plan
            .queued_ai_action_count
            == 2
        )

        adapter = SuppressingAdapter()

        first_live_result = (
            run_controlled_daily_ai(
                state_directory=state_directory,
                run_date=DAY_TWO,
                settings=DailyAiSettings(
                    live_ai_enabled=True,
                    daily_call_limit=1,
                    run_call_limit=1,
                ),
                adapter_factory=lambda: adapter,
            )
        )

        assert (
            first_live_result.calls_executed
            == 1
        )

        assert len(adapter.calls) == 1

        assert (
            first_live_result
            .calls_remaining_today
            == 0
        )

        assert (
            first_live_result
            .final_plan
            .queued_ai_action_count
            == 1
        )

        second_live_result = (
            run_controlled_daily_ai(
                state_directory=state_directory,
                run_date=DAY_TWO,
                settings=DailyAiSettings(
                    live_ai_enabled=True,
                    daily_call_limit=1,
                    run_call_limit=1,
                ),
                adapter_factory=forbidden_factory,
            )
        )

        assert (
            second_live_result.calls_before_run
            == 1
        )

        assert (
            second_live_result.calls_executed
            == 0
        )

        assert (
            second_live_result
            .calls_remaining_today
            == 0
        )

        assert (
            second_live_result
            .final_plan
            .queued_ai_action_count
            == 1
        )

        ledger = CsvAiCallLedger(
            state_directory
        ).load()

        assert len(ledger) == 1

        assert ledger.iloc[0][
            "call_status"
        ] == "COMPLETED"

    print(
        "Controlled daily AI runner smoke test passed."
    )
    print("Default live AI state: disabled")
    print("Disabled-run API calls: 0")
    print("Queued AI items before execution: 2")
    print("Persisted daily call limit: 1")
    print("First-run AI calls: 1")
    print("Second-run AI calls: 0")
    print("Remaining queued AI items: 1")
    print("Adapter created after budget exhaustion: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
