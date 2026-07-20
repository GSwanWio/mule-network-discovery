"""Execute newly queued incremental AI decisions safely."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Protocol

import pandas as pd

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    DailyIncrementalPlan,
    build_incremental_daily_plan,
)
from network_mule_discovery.decision_engine import (
    DECISION_REQUIRED_COLUMNS,
    apply_persisted_decisions,
    prepare_decisions,
)
from network_mule_discovery.schemas import (
    parse_run_date,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


AI_ACTION_TYPES = frozenset({
    "RUN_COUNTERPARTY_AI",
    "RUN_CUSTOMER_AI",
})


class IncrementalDecisionAdapter(Protocol):
    """Structured decision adapter."""

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
        """Return one structured decision."""


@dataclass(frozen=True)
class IncrementalAiExecutionResult:
    """Outputs from one bounded AI execution."""

    initial_plan: DailyIncrementalPlan
    executed_actions: pd.DataFrame
    generated_decisions: pd.DataFrame
    refreshed_plan: DailyIncrementalPlan


def _adapter_metadata(
    decision_adapter: object,
) -> dict[str, object]:
    """Read optional trace metadata from an adapter."""
    metadata = getattr(
        decision_adapter,
        "last_call_metadata",
        None,
    )

    if isinstance(metadata, dict):
        return metadata

    return {}


def execute_incremental_ai_actions(
    *,
    state_store: CsvDailyStateStore,
    decision_adapter: IncrementalDecisionAdapter,
    run_date: date | str,
    max_ai_calls: int,
) -> IncrementalAiExecutionResult:
    """Execute AI items independently and fail closed per item."""
    if max_ai_calls < 0:
        raise ValueError(
            "max_ai_calls cannot be negative."
        )

    resolved_run_date = parse_run_date(
        run_date
    )

    initial_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
    )

    ai_queue = (
        initial_plan.actionable_queue.loc[
            initial_plan.actionable_queue[
                "action_type"
            ].isin(AI_ACTION_TYPES)
        ]
        .sort_values(
            by=[
                "priority",
                "action_type",
                "subject_key",
                "queue_item_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    selected_actions = (
        ai_queue.head(max_ai_calls)
        .copy()
        .reset_index(drop=True)
    )

    subject_payloads = (
        initial_plan
        .projection
        .subject_snapshots[
            [
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
                "feature_payload_json",
            ]
        ]
        .copy()
    )

    selected_actions = selected_actions.merge(
        subject_payloads,
        how="left",
        on=[
            "subject_type",
            "subject_key",
            "feature_snapshot_hash",
        ],
        validate="one_to_one",
    )

    if (
        not selected_actions.empty
        and selected_actions[
            "feature_payload_json"
        ].isna().any()
    ):
        raise RuntimeError(
            "An AI queue item has no matching "
            "feature evidence payload."
        )

    decision_rows: list[
        dict[str, object]
    ] = []

    execution_rows: list[
        dict[str, object]
    ] = []

    failure_rows: list[
        dict[str, object]
    ] = []

    for sequence_number, row in enumerate(
        selected_actions.itertuples(
            index=False
        ),
        start=1,
    ):
        attempted_at = datetime.now(
            timezone.utc
        ).isoformat()

        execution_record = row._asdict()

        try:
            candidate_decision = (
                decision_adapter.decide(
                    subject_type=row.subject_type,
                    subject_key=row.subject_key,
                    feature_snapshot_hash=(
                        row.feature_snapshot_hash
                    ),
                    feature_payload_json=(
                        row.feature_payload_json
                    ),
                    run_date=resolved_run_date,
                    round_number=1,
                    sequence_number=(
                        sequence_number
                    ),
                )
            )

            validated = prepare_decisions(
                pd.DataFrame(
                    [candidate_decision]
                )
            )

            if len(validated) != 1:
                raise RuntimeError(
                    "The adapter did not return exactly "
                    "one valid decision."
                )

            decision_record = {
                column: validated.iloc[0][column]
                for column in (
                    DECISION_REQUIRED_COLUMNS
                )
            }

            decision_rows.append(
                decision_record
            )

            metadata = _adapter_metadata(
                decision_adapter
            )

            execution_record.update(
                {
                    "execution_status": "COMPLETED",
                    "generated_decision_id": (
                        decision_record[
                            "decision_id"
                        ]
                    ),
                    "error_code": "",
                    "error_message": "",
                    "response_id": metadata.get(
                        "response_id",
                        "",
                    ),
                    "request_id": metadata.get(
                        "request_id",
                        "",
                    ),
                }
            )

        except Exception as exc:
            error_code = str(
                getattr(
                    exc,
                    "code",
                    "DECISION_EXECUTION_ERROR",
                )
            )

            error_message = str(exc)[:1000]

            response_id = str(
                getattr(
                    exc,
                    "response_id",
                    "",
                )
                or ""
            )

            request_id = str(
                getattr(
                    exc,
                    "request_id",
                    "",
                )
                or ""
            )

            execution_record.update(
                {
                    "execution_status": (
                        "FAILED_CLOSED"
                    ),
                    "generated_decision_id": "",
                    "error_code": error_code,
                    "error_message": error_message,
                    "response_id": response_id,
                    "request_id": request_id,
                }
            )

            failure_rows.append(
                {
                    "queue_item_id": (
                        row.queue_item_id
                    ),
                    "error_code": error_code,
                    "error_message": error_message,
                    "attempted_at": attempted_at,
                    "response_id": response_id,
                    "request_id": request_id,
                }
            )

        execution_rows.append(
            execution_record
        )

    generated_decisions = pd.DataFrame(
        decision_rows,
        columns=list(
            DECISION_REQUIRED_COLUMNS
        ),
    )

    failures = pd.DataFrame(
        failure_rows,
        columns=[
            "queue_item_id",
            "error_code",
            "error_message",
            "attempted_at",
            "response_id",
            "request_id",
        ],
    )

    if not failures.empty:
        state_store.mark_frontier_items_failed(
            failures
        )

    if generated_decisions.empty:
        decision_store = (
            state_store.load_decision_store()
        )

    else:
        decision_store = (
            state_store.append_decisions(
                generated_decisions
            )
        )

    current_state = state_store.load_snapshot()

    projection = apply_persisted_decisions(
        unified_result=current_state.network,
        decisions=decision_store,
        run_date=resolved_run_date,
    )

    state_store.save_network_state(
        network=UnifiedGroupResult(
            groups=projection.groups,
            nodes=projection.nodes,
            edges=projection.edges,
        ),
        run_date=resolved_run_date,
    )

    refreshed_plan = (
        build_incremental_daily_plan(
            state_store=state_store,
            run_date=resolved_run_date,
        )
    )

    state_store.save_network_state(
        network=UnifiedGroupResult(
            groups=(
                refreshed_plan.projection.groups
            ),
            nodes=(
                refreshed_plan.projection.nodes
            ),
            edges=(
                refreshed_plan.projection.edges
            ),
        ),
        run_date=resolved_run_date,
    )

    executed_actions = pd.DataFrame(
        execution_rows
    )

    if executed_actions.empty:
        executed_actions = (
            selected_actions.copy()
        )

        for column in [
            "execution_status",
            "generated_decision_id",
            "error_code",
            "error_message",
            "response_id",
            "request_id",
        ]:
            executed_actions[column] = (
                pd.Series(dtype="string")
            )

    return IncrementalAiExecutionResult(
        initial_plan=initial_plan,
        executed_actions=executed_actions,
        generated_decisions=(
            generated_decisions
        ),
        refreshed_plan=refreshed_plan,
    )
