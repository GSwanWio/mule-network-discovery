"""Finalize groups whose AI and expansion frontiers are exhausted."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    DailyIncrementalPlan,
    build_incremental_daily_plan,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    build_guardrail_telemetry,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import UnifiedGroupResult


TERMINATION_REASON = "FRONTIER_EXHAUSTED"
TERMINATION_STATUS = "TERMINATED"


class FrontierTerminationError(RuntimeError):
    """A group cannot be terminated safely."""


@dataclass(frozen=True)
class FrontierTerminationResult:
    """Persisted state after generic frontier exhaustion."""

    final_plan: DailyIncrementalPlan
    termination_status: pd.DataFrame
    guardrail_telemetry: pd.DataFrame


def _resolve_group_ids(
    *,
    network: UnifiedGroupResult,
    group_ids: Iterable[str] | None,
) -> tuple[str, ...]:
    available = tuple(
        sorted(
            network.groups["group_id"]
            .astype("string")
            .dropna()
            .unique()
            .tolist()
        )
    )

    if group_ids is None:
        resolved = available
    else:
        resolved = tuple(
            sorted(
                {
                    str(group_id).strip()
                    for group_id in group_ids
                    if str(group_id).strip()
                }
            )
        )

    if not resolved:
        raise FrontierTerminationError(
            "At least one group is required for termination."
        )

    missing = sorted(set(resolved) - set(available))

    if missing:
        raise FrontierTerminationError(
            "Termination groups are missing from persisted state: "
            f"{missing}"
        )

    return resolved


def _mark_groups_terminated(
    *,
    network: UnifiedGroupResult,
    group_ids: tuple[str, ...],
    run_date: date,
) -> UnifiedGroupResult:
    groups = network.groups.copy()

    defaults: dict[str, Any] = {
        "termination_status": "",
        "termination_reason": "",
        "termination_run_date": "",
        "termination_frontier_width": "",
        "termination_failed_frontier_count": "",
    }

    for column, default in defaults.items():
        if column not in groups.columns:
            groups[column] = default

    mask = groups["group_id"].astype("string").isin(group_ids)

    groups.loc[mask, "termination_status"] = TERMINATION_STATUS
    groups.loc[mask, "termination_reason"] = TERMINATION_REASON
    groups.loc[mask, "termination_run_date"] = str(run_date)

    count_columns = [
        "termination_frontier_width",
        "termination_failed_frontier_count",
    ]

    for column in count_columns:
        groups[column] = pd.to_numeric(
            groups[column],
            errors="coerce",
        ).astype("Int64")

    groups.loc[mask, "termination_frontier_width"] = 0
    groups.loc[
        mask,
        "termination_failed_frontier_count",
    ] = 0

    return UnifiedGroupResult(
        groups=groups,
        nodes=network.nodes.copy(),
        edges=network.edges.copy(),
    )


def _build_termination_status(
    *,
    network: UnifiedGroupResult,
    group_ids: tuple[str, ...],
    source_entity_key: str,
    run_date: date,
    final_plan: DailyIncrementalPlan,
    completed_expansion_round_count: int,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for group_id in group_ids:
        group_nodes = network.nodes.loc[
            network.nodes["group_id"].eq(group_id)
        ]
        group_edges = network.edges.loc[
            network.edges["group_id"].eq(group_id)
        ]

        records.append(
            {
                "run_date": str(run_date),
                "group_id": group_id,
                "termination_status": TERMINATION_STATUS,
                "termination_reason": TERMINATION_REASON,
                "source_entity_key": source_entity_key,
                "completed_expansion_round_count": (
                    completed_expansion_round_count
                ),
                "ready_frontier_count": len(
                    final_plan.actionable_queue
                ),
                "failed_frontier_count": (
                    final_plan.failed_closed_item_count
                ),
                "total_node_count": len(group_nodes),
                "total_edge_count": len(group_edges),
                "guardrail_status": "TELEMETRY_ONLY",
            }
        )

    return pd.DataFrame(records)


def run_frontier_exhaustion_termination(
    *,
    state_directory: Path | str,
    run_date: date | str,
    supplemental_subject_payloads: pd.DataFrame,
    group_ids: Iterable[str] | None = None,
    source_entity_key: str = "",
) -> FrontierTerminationResult:
    """Mark selected groups terminated only when no work remains."""
    resolved_run_date = parse_run_date(run_date)
    state_store = CsvDailyStateStore(state_directory)

    try:
        snapshot = state_store.load_snapshot()
    except FileNotFoundError as exc:
        raise FrontierTerminationError(
            "Persisted network state is unavailable."
        ) from exc

    initial_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
    )

    if not initial_plan.actionable_queue.empty:
        remaining = initial_plan.actionable_queue[
            ["action_type", "subject_key"]
        ].to_dict("records")

        raise FrontierTerminationError(
            "Frontier is not exhausted: "
            f"{remaining}"
        )

    if initial_plan.failed_closed_item_count:
        raise FrontierTerminationError(
            "Frontier contains failed-closed work and cannot be "
            "marked terminated."
        )

    projected_network = UnifiedGroupResult(
        groups=initial_plan.projection.groups,
        nodes=initial_plan.projection.nodes,
        edges=initial_plan.projection.edges,
    )
    resolved_group_ids = _resolve_group_ids(
        network=projected_network,
        group_ids=group_ids,
    )
    terminated_network = _mark_groups_terminated(
        network=projected_network,
        group_ids=resolved_group_ids,
        run_date=resolved_run_date,
    )

    state_store.save_network_state(
        network=terminated_network,
        run_date=resolved_run_date,
    )

    final_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
    )

    if not final_plan.actionable_queue.empty:
        raise FrontierTerminationError(
            "Frontier reopened after termination state was saved."
        )

    if final_plan.failed_closed_item_count:
        raise FrontierTerminationError(
            "Failed-closed work appeared after termination state "
            "was saved."
        )

    final_network = UnifiedGroupResult(
        groups=final_plan.projection.groups,
        nodes=final_plan.projection.nodes,
        edges=final_plan.projection.edges,
    )
    completed_expansion_round_count = int(
        snapshot.expansion_ledger["expansion_status"]
        .astype("string")
        .str.upper()
        .eq("COMPLETED")
        .sum()
    )
    termination_status = _build_termination_status(
        network=final_network,
        group_ids=resolved_group_ids,
        source_entity_key=source_entity_key,
        run_date=resolved_run_date,
        final_plan=final_plan,
        completed_expansion_round_count=(
            completed_expansion_round_count
        ),
    )
    telemetry = build_guardrail_telemetry(
        network=final_network,
        frontier_queue=final_plan.frontier_queue,
        run_date=resolved_run_date,
        stage="FRONTIER_EXHAUSTION_TERMINATION",
        new_node_count=0,
        new_edge_count=0,
    )
    telemetry["termination_status"] = TERMINATION_STATUS
    telemetry["termination_reason"] = TERMINATION_REASON
    telemetry["ready_frontier_count"] = 0
    telemetry["failed_frontier_count"] = 0

    return FrontierTerminationResult(
        final_plan=final_plan,
        termination_status=termination_status,
        guardrail_telemetry=telemetry,
    )
