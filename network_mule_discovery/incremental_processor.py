"""Execute only newly queued incremental AI decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


AI_ACTION_TYPES = frozenset({
    "RUN_COUNTERPARTY_AI",
    "RUN_CUSTOMER_AI",
})


class IncrementalDecisionAdapter(Protocol):
    """Structured decision adapter used by incremental processing."""

    def decide(
        self,
        *,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        run_date: date,
        round_number: int,
        sequence_number: int,
    ) -> dict[str, str]:
        """Return one validated structured decision."""


@dataclass(frozen=True)
class IncrementalAiExecutionResult:
    """Outputs from one bounded incremental AI execution."""

    initial_plan: DailyIncrementalPlan
    executed_actions: pd.DataFrame
    generated_decisions: pd.DataFrame
    refreshed_plan: DailyIncrementalPlan


def execute_incremental_ai_actions(
    *,
    state_store: CsvDailyStateStore,
    decision_adapter: IncrementalDecisionAdapter,
    run_date: date | str,
    max_ai_calls: int,
) -> IncrementalAiExecutionResult:
    """
    Execute newly queued AI actions and persist their decisions.

    Relationship discovery and recursive expansion are deliberately
    excluded from this function. A new AI decision may expose customer
    assessment work, which remains in the refreshed frontier queue.
    """
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

    decision_rows: list[dict[str, str]] = []

    for sequence_number, row in enumerate(
        selected_actions.itertuples(
            index=False
        ),
        start=1,
    ):
        decision = decision_adapter.decide(
            subject_type=row.subject_type,
            subject_key=row.subject_key,
            feature_snapshot_hash=(
                row.feature_snapshot_hash
            ),
            run_date=resolved_run_date,
            round_number=1,
            sequence_number=sequence_number,
        )

        decision_rows.append(decision)

    generated_decisions = pd.DataFrame(
        decision_rows,
        columns=list(
            DECISION_REQUIRED_COLUMNS
        ),
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

    executed_actions = selected_actions.copy()

    if executed_actions.empty:
        executed_actions[
            "execution_status"
        ] = pd.Series(dtype="string")

        executed_actions[
            "generated_decision_id"
        ] = pd.Series(dtype="string")

    else:
        decision_id_map = {
            (
                row.subject_type,
                row.subject_key,
                row.feature_snapshot_hash,
            ): row.decision_id
            for row in (
                generated_decisions.itertuples(
                    index=False
                )
            )
        }

        executed_actions[
            "execution_status"
        ] = "COMPLETED"

        executed_actions[
            "generated_decision_id"
        ] = executed_actions.apply(
            lambda row: decision_id_map[
                (
                    row["subject_type"],
                    row["subject_key"],
                    row[
                        "feature_snapshot_hash"
                    ],
                )
            ],
            axis=1,
        )

    return IncrementalAiExecutionResult(
        initial_plan=initial_plan,
        executed_actions=executed_actions,
        generated_decisions=(
            generated_decisions
        ),
        refreshed_plan=refreshed_plan,
    )
