"""Operational integrity checks for restart-safe persisted discovery state."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from network_mule_discovery.daily_state import DailyStateSnapshot


class OperationalStateIntegrityError(RuntimeError):
    """Persisted state violates a restart-safety invariant."""


@dataclass(frozen=True)
class OperationalStateIntegrityReport:
    """Counts recorded after persisted state passes integrity checks."""

    node_count: int
    edge_count: int
    decision_count: int
    expansion_ledger_count: int
    frontier_queue_count: int
    ai_call_count: int
    completed_ai_outcome_count: int


def _require_columns(
    *,
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    label: str,
) -> None:
    missing_columns = sorted(
        set(columns) - set(frame.columns)
    )

    if missing_columns:
        raise OperationalStateIntegrityError(
            f"{label} is missing columns: {missing_columns}"
        )


def _assert_unique(
    *,
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    label: str,
) -> None:
    if frame.empty:
        return

    _require_columns(
        frame=frame,
        columns=columns,
        label=label,
    )

    duplicate_mask = frame.duplicated(
        subset=list(columns),
        keep=False,
    )

    if not duplicate_mask.any():
        return

    duplicates = (
        frame.loc[
            duplicate_mask,
            list(columns),
        ]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    raise OperationalStateIntegrityError(
        f"{label} contains duplicate restart keys: {duplicates}"
    )


def validate_persisted_operational_state(
    *,
    snapshot: DailyStateSnapshot,
    ai_call_ledger: pd.DataFrame,
) -> OperationalStateIntegrityReport:
    """Validate uniqueness invariants required for safe restart."""
    _assert_unique(
        frame=snapshot.network.nodes,
        columns=("node_id",),
        label="network nodes",
    )
    _assert_unique(
        frame=snapshot.network.edges,
        columns=("edge_id",),
        label="network edges",
    )
    _assert_unique(
        frame=snapshot.decision_store,
        columns=(
            "subject_type",
            "subject_key",
            "feature_snapshot_hash",
        ),
        label="decision store",
    )
    _assert_unique(
        frame=snapshot.expansion_ledger,
        columns=("queue_item_id",),
        label="expansion ledger",
    )
    _assert_unique(
        frame=snapshot.frontier_queue,
        columns=("queue_item_id",),
        label="frontier queue",
    )
    _assert_unique(
        frame=ai_call_ledger,
        columns=("ai_call_id",),
        label="AI call ledger",
    )

    if ai_call_ledger.empty:
        completed_calls = ai_call_ledger.copy()
    else:
        _require_columns(
            frame=ai_call_ledger,
            columns=(
                "call_status",
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ),
            label="AI call ledger",
        )
        completed_calls = ai_call_ledger.loc[
            ai_call_ledger["call_status"]
            .astype("string")
            .str.upper()
            .eq("COMPLETED")
        ].copy()

    _assert_unique(
        frame=completed_calls,
        columns=(
            "subject_type",
            "subject_key",
            "feature_snapshot_hash",
        ),
        label="completed AI outcomes",
    )

    return OperationalStateIntegrityReport(
        node_count=len(snapshot.network.nodes),
        edge_count=len(snapshot.network.edges),
        decision_count=len(snapshot.decision_store),
        expansion_ledger_count=len(
            snapshot.expansion_ledger
        ),
        frontier_queue_count=len(
            snapshot.frontier_queue
        ),
        ai_call_count=len(ai_call_ledger),
        completed_ai_outcome_count=len(
            completed_calls
        ),
    )
