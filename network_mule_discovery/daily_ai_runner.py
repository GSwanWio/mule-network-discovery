"""Controlled daily execution of queued AI decisions."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    DailyIncrementalPlan,
    build_incremental_daily_plan,
)
from network_mule_discovery.incremental_processor import (
    AI_ACTION_TYPES,
    IncrementalAiExecutionResult,
    execute_incremental_ai_actions,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionAdapter,
)
from network_mule_discovery.production_ai_runtime import (
    JsonProductionAiRuntimeStore,
    JsonProductionAiStartupFailureStore,
    ProductionAiStartupError,
    build_production_ai_runtime,
    build_production_ai_startup_failure,
)
from network_mule_discovery.schemas import (
    parse_run_date,
)


AI_CALL_LEDGER_FILENAME = "ai_call_ledger.csv"

AI_CALL_LEDGER_COLUMNS = (
    "ai_call_id",
    "run_date",
    "queue_item_id",
    "action_type",
    "subject_type",
    "subject_key",
    "feature_snapshot_hash",
    "call_status",
    "attempted_at",
    "generated_decision_id",
    "decision",
    "reason_code",
    "confidence",
    "rationale",
    "key_evidence_json",
    "model",
    "prompt_version",
    "error_code",
    "error_message",
    "response_id",
    "request_id",
    "response_status",
    "incomplete_reason",
    "input_tokens",
    "output_tokens",
    "reasoning_tokens",
)


@dataclass(frozen=True)
class DailyAiSettings:
    """Safety settings for one controlled daily run."""

    live_ai_enabled: bool
    daily_call_limit: int
    run_call_limit: int


@dataclass(frozen=True)
class ControlledDailyAiRunResult:
    """Results from one controlled daily execution."""

    initial_plan: DailyIncrementalPlan
    final_plan: DailyIncrementalPlan
    executed_actions: pd.DataFrame
    calls_before_run: int
    calls_executed: int
    calls_remaining_today: int
    live_ai_enabled: bool


def load_daily_ai_settings() -> DailyAiSettings:
    """Load the explicit live-AI safety gates."""
    live_ai_enabled = (
        os.getenv(
            "MULE_NETWORK_ENABLE_LIVE_AI",
            "",
        ).strip()
        == "1"
    )

    daily_call_limit = int(
        os.getenv(
            "MULE_NETWORK_DAILY_AI_CALL_LIMIT",
            "0",
        )
    )

    run_call_limit = int(
        os.getenv(
            "MULE_NETWORK_RUN_AI_CALL_LIMIT",
            str(daily_call_limit),
        )
    )

    if daily_call_limit < 0:
        raise RuntimeError(
            "MULE_NETWORK_DAILY_AI_CALL_LIMIT "
            "cannot be negative."
        )

    if run_call_limit < 0:
        raise RuntimeError(
            "MULE_NETWORK_RUN_AI_CALL_LIMIT "
            "cannot be negative."
        )

    if live_ai_enabled and daily_call_limit == 0:
        raise RuntimeError(
            "Live AI is enabled but the daily call "
            "limit is zero."
        )

    if live_ai_enabled and run_call_limit == 0:
        raise RuntimeError(
            "Live AI is enabled but the per-run call "
            "limit is zero."
        )

    return DailyAiSettings(
        live_ai_enabled=live_ai_enabled,
        daily_call_limit=daily_call_limit,
        run_call_limit=run_call_limit,
    )


class CsvAiCallLedger:
    """Persist every attempted AI call for budget control."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state_directory = Path(
            state_directory
        )

        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.path = (
            self.state_directory
            / AI_CALL_LEDGER_FILENAME
        )

    def load(self) -> pd.DataFrame:
        """Load the complete call ledger."""
        if not self.path.exists():
            return pd.DataFrame(
                columns=list(
                    AI_CALL_LEDGER_COLUMNS
                )
            )

        frame = pd.read_csv(
            self.path,
            dtype="string",
            keep_default_na=False,
        )

        for column in AI_CALL_LEDGER_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""

        return frame[
            list(AI_CALL_LEDGER_COLUMNS)
        ]

    def count_calls(
        self,
        run_date: date | str,
    ) -> int:
        """Count all successful and failed calls that day."""
        resolved_run_date = str(
            parse_run_date(run_date)
        )

        ledger = self.load()

        return int(
            ledger["run_date"]
            .eq(resolved_run_date)
            .sum()
        )

    def append_executions(
        self,
        *,
        run_date: date | str,
        executed_actions: pd.DataFrame,
    ) -> pd.DataFrame:
        """Append newly attempted calls to the ledger."""
        if executed_actions.empty:
            return self.load()

        resolved_run_date = str(
            parse_run_date(run_date)
        )

        records: list[dict[str, str]] = []

        for row in executed_actions.itertuples(
            index=False
        ):
            attempted_at = str(
                getattr(
                    row,
                    "attempted_at",
                    "",
                )
            )

            canonical_key = "|".join(
                [
                    resolved_run_date,
                    str(row.queue_item_id),
                    attempted_at,
                ]
            )

            digest = hashlib.sha256(
                canonical_key.encode("utf-8")
            ).hexdigest()[:16]

            records.append(
                {
                    "ai_call_id": f"AC{digest}",
                    "run_date": resolved_run_date,
                    "queue_item_id": str(
                        row.queue_item_id
                    ),
                    "action_type": str(
                        row.action_type
                    ),
                    "subject_type": str(
                        row.subject_type
                    ),
                    "subject_key": str(
                        row.subject_key
                    ),
                    "feature_snapshot_hash": str(
                        row.feature_snapshot_hash
                    ),
                    "call_status": str(
                        row.execution_status
                    ),
                    "attempted_at": attempted_at,
                    "generated_decision_id": str(
                        getattr(
                            row,
                            "generated_decision_id",
                            "",
                        )
                    ),
                    "decision": str(
                        getattr(
                            row,
                            "decision",
                            "",
                        )
                    ),
                    "reason_code": str(
                        getattr(
                            row,
                            "reason_code",
                            "",
                        )
                    ),
                    "confidence": str(
                        getattr(
                            row,
                            "confidence",
                            "",
                        )
                    ),
                    "rationale": str(
                        getattr(
                            row,
                            "rationale",
                            "",
                        )
                    ),
                    "key_evidence_json": str(
                        getattr(
                            row,
                            "key_evidence_json",
                            "[]",
                        )
                    ),
                    "model": str(
                        getattr(
                            row,
                            "model",
                            "",
                        )
                    ),
                    "prompt_version": str(
                        getattr(
                            row,
                            "prompt_version",
                            "",
                        )
                    ),
                    "error_code": str(
                        getattr(
                            row,
                            "error_code",
                            "",
                        )
                    ),
                    "error_message": str(
                        getattr(
                            row,
                            "error_message",
                            "",
                        )
                    ),
                    "response_id": str(
                        getattr(
                            row,
                            "response_id",
                            "",
                        )
                    ),
                    "request_id": str(
                        getattr(
                            row,
                            "request_id",
                            "",
                        )
                    ),
                    "response_status": str(
                        getattr(
                            row,
                            "response_status",
                            "",
                        )
                    ),
                    "incomplete_reason": str(
                        getattr(
                            row,
                            "incomplete_reason",
                            "",
                        )
                    ),
                    "input_tokens": str(
                        getattr(
                            row,
                            "input_tokens",
                            "",
                        )
                    ),
                    "output_tokens": str(
                        getattr(
                            row,
                            "output_tokens",
                            "",
                        )
                    ),
                    "reasoning_tokens": str(
                        getattr(
                            row,
                            "reasoning_tokens",
                            "",
                        )
                    ),
                }
            )

        combined = pd.concat(
            [
                self.load(),
                pd.DataFrame(
                    records,
                    columns=list(
                        AI_CALL_LEDGER_COLUMNS
                    ),
                ),
            ],
            ignore_index=True,
        )

        combined = (
            combined
            .drop_duplicates(
                subset=["ai_call_id"],
                keep="last",
            )
            .sort_values(
                by=[
                    "run_date",
                    "attempted_at",
                    "ai_call_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        combined.to_csv(
            self.path,
            index=False,
        )

        return self.load()


def run_controlled_daily_ai(
    *,
    state_directory: Path | str,
    run_date: date | str,
    settings: DailyAiSettings,
    adapter_factory: Callable[
        [],
        object,
    ] | None = None,
    allowed_action_types: set[str] | frozenset[str] | None = None,
    supplemental_subject_payloads: pd.DataFrame | None = None,
) -> ControlledDailyAiRunResult:
    """Run only the number of calls allowed by both caps."""
    resolved_run_date = parse_run_date(
        run_date
    )

    state_store = CsvDailyStateStore(
        state_directory
    )

    call_ledger = CsvAiCallLedger(
        state_directory
    )

    resolved_action_types = (
        AI_ACTION_TYPES
        if allowed_action_types is None
        else frozenset(allowed_action_types)
    )

    unsupported_action_types = sorted(
        resolved_action_types
        - AI_ACTION_TYPES
    )

    if unsupported_action_types:
        raise ValueError(
            "allowed_action_types contains unsupported AI "
            f"actions: {unsupported_action_types}"
        )

    initial_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
    )

    calls_before_run = (
        call_ledger.count_calls(
            resolved_run_date
        )
    )

    daily_remaining = max(
        settings.daily_call_limit
        - calls_before_run,
        0,
    )

    queued_ai_count = int(
        initial_plan.actionable_queue[
            "action_type"
        ]
        .isin(resolved_action_types)
        .sum()
    )

    calls_allowed = min(
        settings.run_call_limit,
        daily_remaining,
        queued_ai_count,
    )

    if (
        not settings.live_ai_enabled
        or calls_allowed == 0
    ):
        return ControlledDailyAiRunResult(
            initial_plan=initial_plan,
            final_plan=initial_plan,
            executed_actions=pd.DataFrame(),
            calls_before_run=calls_before_run,
            calls_executed=0,
            calls_remaining_today=daily_remaining,
            live_ai_enabled=(
                settings.live_ai_enabled
            ),
        )

    if adapter_factory is None:
        try:
            production_runtime = (
                build_production_ai_runtime(
                    settings
                )
            )
            JsonProductionAiRuntimeStore(
                state_directory
            ).save(
                production_runtime
            )
            decision_adapter = (
                OpenAIDecisionAdapter
                .from_environment()
            )

        except Exception as exc:
            failure = (
                build_production_ai_startup_failure(
                    exc
                )
            )
            JsonProductionAiStartupFailureStore(
                state_directory
            ).save(failure)

            raise ProductionAiStartupError(
                "Production live-AI startup failed: "
                f"{failure.error_type}: "
                f"{failure.error_message}"
            ) from exc

    else:
        decision_adapter = adapter_factory()

    execution_result: IncrementalAiExecutionResult = (
        execute_incremental_ai_actions(
            state_store=state_store,
            decision_adapter=decision_adapter,
            run_date=resolved_run_date,
            max_ai_calls=calls_allowed,
            allowed_action_types=(
                resolved_action_types
            ),
            supplemental_subject_payloads=(
                supplemental_subject_payloads
            ),
        )
    )

    call_ledger.append_executions(
        run_date=resolved_run_date,
        executed_actions=(
            execution_result.executed_actions
        ),
    )

    calls_executed = len(
        execution_result.executed_actions
    )

    calls_after_run = (
        call_ledger.count_calls(
            resolved_run_date
        )
    )

    calls_remaining_today = max(
        settings.daily_call_limit
        - calls_after_run,
        0,
    )

    return ControlledDailyAiRunResult(
        initial_plan=initial_plan,
        final_plan=(
            execution_result.refreshed_plan
        ),
        executed_actions=(
            execution_result.executed_actions
        ),
        calls_before_run=calls_before_run,
        calls_executed=calls_executed,
        calls_remaining_today=(
            calls_remaining_today
        ),
        live_ai_enabled=True,
    )
