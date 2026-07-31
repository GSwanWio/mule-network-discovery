"""Finalize a group when recursive discovery finds no unseen relationships."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from network_mule_discovery.daily_state import (
    EXPANSION_LEDGER_COLUMNS,
    CsvDailyStateStore,
    DailyIncrementalPlan,
    build_incremental_daily_plan,
)
from network_mule_discovery.raw_source_adapter import (
    RawDiscoverySources,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    DISCOVERY_ACTION_TYPE,
    RecursiveCounterpartyDiscoveryResult,
    build_guardrail_telemetry,
    discover_recursive_counterparties,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import UnifiedGroupResult


TERMINATION_REASON = "FRONTIER_EXHAUSTED"
TERMINATION_STATUS = "TERMINATED"


class RecursiveTerminationError(RuntimeError):
    """The recursive workflow cannot be terminated safely."""


@dataclass(frozen=True)
class RecursiveTerminationResult:
    """Persisted state after one deduplicated termination round."""

    source_entity_key: str
    discovery: RecursiveCounterpartyDiscoveryResult
    discovery_performed: bool
    expansion_ledger_appended: bool
    final_plan: DailyIncrementalPlan
    expansion_ledger: pd.DataFrame
    termination_status: pd.DataFrame
    guardrail_telemetry: pd.DataFrame


def _empty_discovery(
    *,
    source_entity_key: str,
    source_group_ids: tuple[str, ...],
) -> RecursiveCounterpartyDiscoveryResult:
    """Return an empty idempotent discovery result for a terminated group."""
    return RecursiveCounterpartyDiscoveryResult(
        source_entity_key=source_entity_key,
        source_customer_id=(
            source_entity_key.split("|", 1)[-1]
        ),
        source_group_ids=source_group_ids,
        relationships=pd.DataFrame(
            columns=[
                "snapshot_date",
                "source_entity_key",
                "relationship_type",
                "counterparty_key",
                "counterparty_name",
                "target_entity_type",
                "target_entity_id",
                "target_entity_key",
                "evidence_key",
                "evidence_summary",
                "source_event_count",
                "candidate_event_count",
                "total_candidate_event_count",
                "source_total_amount",
                "candidate_total_amount",
            ]
        ),
        counterparty_summary=pd.DataFrame(
            columns=[
                "run_date",
                "source_entity_key",
                "source_customer_id",
                "group_ids",
                "counterparty_key",
                "counterparty_name",
                "source_event_count",
                "source_total_amount",
                "candidate_customer_count",
                "candidate_event_count",
                "candidate_total_amount",
                "source_first_event_timestamp",
                "source_last_event_timestamp",
                "candidate_first_event_timestamp",
                "candidate_last_event_timestamp",
            ]
        ),
        new_counterparty_keys=tuple(),
        skipped_existing_counterparty_keys=tuple(),
        unshared_counterparty_keys=tuple(),
    )


def _latest_zero_row_completion(
    expansion_ledger: pd.DataFrame,
) -> pd.Series | None:
    if expansion_ledger.empty:
        return None

    rows = expansion_ledger.loc[
        expansion_ledger["expansion_status"]
        .astype("string")
        .str.upper()
        .eq("COMPLETED")
        & pd.to_numeric(
            expansion_ledger["relationship_rows_found"],
            errors="coerce",
        )
        .fillna(-1)
        .eq(0)
    ].copy()

    if rows.empty:
        return None

    rows["round_number_value"] = pd.to_numeric(
        rows["round_number"],
        errors="coerce",
    ).fillna(0)

    return rows.sort_values(
        by=["round_number_value", "queue_item_id"],
        kind="stable",
    ).iloc[-1]


def _next_round_number(
    expansion_ledger: pd.DataFrame,
) -> int:
    if expansion_ledger.empty:
        return 1

    values = pd.to_numeric(
        expansion_ledger["round_number"],
        errors="coerce",
    ).dropna()

    return int(values.max()) + 1 if not values.empty else 1


def _mark_group_terminated(
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

    if int(mask.sum()) != len(set(group_ids)):
        raise RecursiveTerminationError(
            "Every termination group must exist in persisted network state."
        )

    groups.loc[mask, "termination_status"] = TERMINATION_STATUS
    groups.loc[mask, "termination_reason"] = TERMINATION_REASON
    groups.loc[mask, "termination_run_date"] = str(run_date)
    termination_count_columns = [
        "termination_frontier_width",
        "termination_failed_frontier_count",
    ]

    for column in termination_count_columns:
        if column not in groups.columns:
            groups[column] = pd.Series(
                pd.NA,
                index=groups.index,
                dtype="Int64",
            )
        else:
            groups[column] = (
                pd.to_numeric(
                    groups[column],
                    errors="coerce",
                )
                .astype("Int64")
            )

    groups.loc[
        mask,
        "termination_frontier_width",
    ] = 0

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
    expansion_ledger: pd.DataFrame,
    final_plan: DailyIncrementalPlan,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    ready_count = int(len(final_plan.actionable_queue))
    failed_count = int(final_plan.failed_closed_item_count)
    completed_rounds = int(
        expansion_ledger["expansion_status"]
        .astype("string")
        .str.upper()
        .eq("COMPLETED")
        .sum()
    )

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
                "completed_expansion_round_count": completed_rounds,
                "ready_frontier_count": ready_count,
                "failed_frontier_count": failed_count,
                "total_node_count": len(group_nodes),
                "total_edge_count": len(group_edges),
                "guardrail_status": "TELEMETRY_ONLY",
            }
        )

    return pd.DataFrame(records)


def run_recursive_termination(
    *,
    state_directory: Path | str,
    run_date: date | str,
    supplemental_subject_payloads: pd.DataFrame,
    source_directory: Path | str | None = None,
    raw_sources: RawDiscoverySources | None = None,
) -> RecursiveTerminationResult:
    """Consume the final expansion source and persist frontier exhaustion."""
    resolved_run_date = parse_run_date(run_date)
    state_store = CsvDailyStateStore(state_directory)

    try:
        snapshot = state_store.load_snapshot()
    except FileNotFoundError as exc:
        raise RecursiveTerminationError(
            "Persisted recursive customer state is unavailable."
        ) from exc

    initial_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=supplemental_subject_payloads,
    )
    ready_ai = initial_plan.actionable_queue.loc[
        initial_plan.actionable_queue["action_type"].isin(
            ["RUN_COUNTERPARTY_AI", "RUN_CUSTOMER_AI"]
        )
    ]

    if not ready_ai.empty:
        raise RecursiveTerminationError(
            "Termination cannot start while AI decisions remain ready: "
            f"{sorted(ready_ai['subject_key'].astype('string'))}"
        )

    ready_discovery = initial_plan.actionable_queue.loc[
        initial_plan.actionable_queue["action_type"].eq(
            DISCOVERY_ACTION_TYPE
        )
    ].copy()
    discovery_performed = False
    ledger_appended = False

    if len(ready_discovery) == 1:
        row = ready_discovery.iloc[0]
        source_entity_key = str(row["subject_key"])
        source_group_ids = tuple(
            value
            for value in str(row["group_ids"]).split("|")
            if value
        )
        discovery = discover_recursive_counterparties(
            source_directory=source_directory,
            raw_sources=raw_sources,
            observed_network=snapshot.network,
            source_entity_key=source_entity_key,
            group_ids=source_group_ids,
            run_date=resolved_run_date,
        )
        discovery_performed = True

        if discovery.new_counterparty_keys or not discovery.relationships.empty:
            raise RecursiveTerminationError(
                "Frontier is not exhausted; unseen shared counterparties "
                f"remain: {list(discovery.new_counterparty_keys)}"
            )

        round_number = _next_round_number(
            snapshot.expansion_ledger
        )
        ledger_row = pd.DataFrame(
            [
                {
                    "run_date": str(resolved_run_date),
                    "round_number": round_number,
                    "queue_item_id": str(row["queue_item_id"]),
                    "source_entity_key": source_entity_key,
                    "group_ids": "|".join(source_group_ids),
                    "relationship_rows_found": 0,
                    "expansion_status": "COMPLETED",
                }
            ],
            columns=list(EXPANSION_LEDGER_COLUMNS),
        )
        expansion_ledger = state_store.append_expansion_ledger(
            ledger_row
        )
        ledger_appended = True
    elif ready_discovery.empty:
        latest = _latest_zero_row_completion(
            snapshot.expansion_ledger
        )

        if latest is None:
            raise RecursiveTerminationError(
                "No ready final discovery action or completed zero-row "
                "termination round was found."
            )

        source_entity_key = str(latest["source_entity_key"])
        source_group_ids = tuple(
            value
            for value in str(latest["group_ids"]).split("|")
            if value
        )
        discovery = _empty_discovery(
            source_entity_key=source_entity_key,
            source_group_ids=source_group_ids,
        )
        expansion_ledger = snapshot.expansion_ledger.copy()
    else:
        raise RecursiveTerminationError(
            "Expected exactly one final recursive discovery action; "
            f"found {len(ready_discovery)}."
        )

    post_ledger_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=supplemental_subject_payloads,
    )

    if not post_ledger_plan.actionable_queue.empty:
        remaining = post_ledger_plan.actionable_queue[
            ["action_type", "subject_key"]
        ].to_dict("records")
        raise RecursiveTerminationError(
            "Frontier is not exhausted after completing the final "
            f"expansion: {remaining}"
        )

    if post_ledger_plan.failed_closed_item_count:
        raise RecursiveTerminationError(
            "Frontier contains failed-closed work and cannot be marked "
            "terminated."
        )

    projected_network = UnifiedGroupResult(
        groups=post_ledger_plan.projection.groups,
        nodes=post_ledger_plan.projection.nodes,
        edges=post_ledger_plan.projection.edges,
    )
    terminated_network = _mark_group_terminated(
        network=projected_network,
        group_ids=source_group_ids,
        run_date=resolved_run_date,
    )
    state_store.save_network_state(
        terminated_network,
        resolved_run_date,
    )
    final_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=supplemental_subject_payloads,
    )
    final_network = UnifiedGroupResult(
        groups=final_plan.projection.groups,
        nodes=final_plan.projection.nodes,
        edges=final_plan.projection.edges,
    )
    final_ledger = state_store.load_expansion_ledger()
    termination_status = _build_termination_status(
        network=final_network,
        group_ids=source_group_ids,
        source_entity_key=source_entity_key,
        run_date=resolved_run_date,
        expansion_ledger=final_ledger,
        final_plan=final_plan,
    )
    telemetry = build_guardrail_telemetry(
        network=final_network,
        frontier_queue=final_plan.frontier_queue,
        run_date=resolved_run_date,
        stage="RECURSIVE_TERMINATION",
        new_node_count=0,
        new_edge_count=0,
    )
    telemetry["termination_status"] = TERMINATION_STATUS
    telemetry["termination_reason"] = TERMINATION_REASON
    telemetry["ready_frontier_count"] = 0
    telemetry["failed_frontier_count"] = 0

    return RecursiveTerminationResult(
        source_entity_key=source_entity_key,
        discovery=discovery,
        discovery_performed=discovery_performed,
        expansion_ledger_appended=ledger_appended,
        final_plan=final_plan,
        expansion_ledger=final_ledger,
        termination_status=termination_status,
        guardrail_telemetry=telemetry,
    )
