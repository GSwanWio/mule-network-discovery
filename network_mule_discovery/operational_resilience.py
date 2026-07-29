"""Operational integrity checks for restart-safe persisted discovery state."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from network_mule_discovery.daily_state import DailyStateSnapshot


OPERATIONAL_RESILIENCE_GATE_FILENAME = (
    "operational_resilience_gate.json"
)


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
    technical_requeue_count: int = 0


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
    technical_reprocessing_ledger: pd.DataFrame | None = None,
) -> OperationalStateIntegrityReport:
    """Validate uniqueness invariants required for safe restart."""
    _assert_unique(
        frame=snapshot.network.nodes,
        columns=("node_id",),
        label="network nodes",
    )
    _assert_unique(
        frame=snapshot.network.nodes,
        columns=(
            "group_id",
            "node_key",
        ),
        label="logical network nodes",
    )
    _assert_unique(
        frame=snapshot.network.edges,
        columns=("edge_id",),
        label="network edges",
    )
    _assert_unique(
        frame=snapshot.network.edges,
        columns=(
            "group_id",
            "edge_type",
            "source_node_key",
            "target_node_key",
        ),
        label="logical network edges",
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

    if technical_reprocessing_ledger is None:
        technical_reprocessing_ledger = pd.DataFrame()

    _assert_unique(
        frame=technical_reprocessing_ledger,
        columns=("requeue_event_id",),
        label="technical reprocessing ledger",
    )
    _assert_unique(
        frame=technical_reprocessing_ledger,
        columns=(
            "queue_item_id",
            "prior_attempt_count",
        ),
        label="technical reprocessing failure attempts",
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
        technical_requeue_count=len(
            technical_reprocessing_ledger
        ),
    )


@dataclass(frozen=True)
class OperationalResilienceGateReport:
    """Persistable summary of the final operational-resilience gate."""

    run_date: str
    gate_status: str
    termination_status: str
    termination_reason: str
    guardrail_status: str
    node_count: int
    edge_count: int
    decision_count: int
    expansion_ledger_count: int
    frontier_queue_count: int
    ai_call_count: int
    completed_ai_outcome_count: int
    failed_ai_call_count: int
    technical_requeue_count: int
    repeated_ai_call_count: int
    repeated_expansion_count: int
    stable_node_ids: bool
    stable_edge_ids: bool
    stable_feature_snapshot_hashes: bool

    def to_record(self) -> dict[str, object]:
        """Return a JSON-serializable gate record."""
        return asdict(self)


def _normalized_values(
    *,
    frame: pd.DataFrame,
    column: str,
    label: str,
) -> set[str]:
    _require_columns(
        frame=frame,
        columns=(column,),
        label=label,
    )

    return {
        str(value).strip()
        for value in frame[column].astype("string")
        if str(value).strip()
    }


def _subject_hash_keys(
    frame: pd.DataFrame,
) -> tuple[tuple[str, str, str], ...]:
    if frame.empty:
        return ()

    columns = (
        "subject_type",
        "subject_key",
        "feature_snapshot_hash",
    )
    _require_columns(
        frame=frame,
        columns=columns,
        label="subject snapshots",
    )
    _assert_unique(
        frame=frame,
        columns=columns,
        label="subject snapshots",
    )

    rows = frame[list(columns)].astype("string")

    return tuple(
        sorted(
            tuple(row)
            for row in rows.itertuples(
                index=False,
                name=None,
            )
        )
    )


def build_operational_resilience_gate_report(
    *,
    run_date: str,
    snapshot: DailyStateSnapshot,
    ai_call_ledger: pd.DataFrame,
    technical_reprocessing_ledger: pd.DataFrame,
    termination_status: pd.DataFrame,
    guardrail_telemetry: pd.DataFrame,
    current_subject_snapshots: pd.DataFrame,
    expected_node_ids: Iterable[str],
    expected_edge_ids: Iterable[str],
    expected_subject_hashes: Iterable[tuple[str, str, str]],
    repeated_ai_call_count: int,
    repeated_expansion_count: int,
) -> OperationalResilienceGateReport:
    """Validate and summarize the consolidated resilience milestone."""
    if repeated_ai_call_count != 0:
        raise OperationalStateIntegrityError(
            "The terminal unchanged run repeated AI calls."
        )

    if repeated_expansion_count != 0:
        raise OperationalStateIntegrityError(
            "The replayed graph created new nodes or edges."
        )

    integrity = validate_persisted_operational_state(
        snapshot=snapshot,
        ai_call_ledger=ai_call_ledger,
        technical_reprocessing_ledger=(
            technical_reprocessing_ledger
        ),
    )

    if not snapshot.frontier_queue.empty:
        raise OperationalStateIntegrityError(
            "The final frontier queue is not empty."
        )

    termination_states = _normalized_values(
        frame=termination_status,
        column="termination_status",
        label="termination status",
    )
    termination_reasons = _normalized_values(
        frame=termination_status,
        column="termination_reason",
        label="termination status",
    )
    ready_frontier_counts = _normalized_values(
        frame=termination_status,
        column="ready_frontier_count",
        label="termination status",
    )
    failed_frontier_counts = _normalized_values(
        frame=termination_status,
        column="failed_frontier_count",
        label="termination status",
    )

    if termination_states != {"TERMINATED"}:
        raise OperationalStateIntegrityError(
            "The final termination status is not TERMINATED."
        )

    if termination_reasons != {"FRONTIER_EXHAUSTED"}:
        raise OperationalStateIntegrityError(
            "The final termination reason is not FRONTIER_EXHAUSTED."
        )

    if ready_frontier_counts != {"0"}:
        raise OperationalStateIntegrityError(
            "Ready frontier work remains at termination."
        )

    if failed_frontier_counts != {"0"}:
        raise OperationalStateIntegrityError(
            "Failed-closed frontier work remains at termination."
        )

    guardrail_states = _normalized_values(
        frame=guardrail_telemetry,
        column="guardrail_status",
        label="guardrail telemetry",
    )
    telemetry_termination_states = _normalized_values(
        frame=guardrail_telemetry,
        column="termination_status",
        label="guardrail telemetry",
    )
    telemetry_termination_reasons = _normalized_values(
        frame=guardrail_telemetry,
        column="termination_reason",
        label="guardrail telemetry",
    )

    if guardrail_states != {"TELEMETRY_ONLY"}:
        raise OperationalStateIntegrityError(
            "The final guardrail telemetry status is unexpected."
        )

    if telemetry_termination_states != {"TERMINATED"}:
        raise OperationalStateIntegrityError(
            "Guardrail telemetry does not retain termination status."
        )

    if telemetry_termination_reasons != {"FRONTIER_EXHAUSTED"}:
        raise OperationalStateIntegrityError(
            "Guardrail telemetry does not retain termination reason."
        )

    current_node_ids = tuple(
        sorted(
            snapshot.network.nodes["node_id"]
            .astype("string")
            .tolist()
        )
    )
    current_edge_ids = tuple(
        sorted(
            snapshot.network.edges["edge_id"]
            .astype("string")
            .tolist()
        )
    )
    stable_node_ids = current_node_ids == tuple(
        sorted(str(value) for value in expected_node_ids)
    )
    stable_edge_ids = current_edge_ids == tuple(
        sorted(str(value) for value in expected_edge_ids)
    )
    stable_subject_hashes = (
        _subject_hash_keys(current_subject_snapshots)
        == tuple(sorted(expected_subject_hashes))
    )

    if not stable_node_ids:
        raise OperationalStateIntegrityError(
            "Node identifiers changed during the resilience flow."
        )

    if not stable_edge_ids:
        raise OperationalStateIntegrityError(
            "Edge identifiers changed during the resilience flow."
        )

    if not stable_subject_hashes:
        raise OperationalStateIntegrityError(
            "Feature snapshot hashes changed without new evidence."
        )

    if ai_call_ledger.empty:
        failed_ai_call_count = 0
    else:
        _require_columns(
            frame=ai_call_ledger,
            columns=("call_status",),
            label="AI call ledger",
        )
        failed_ai_call_count = int(
            ai_call_ledger["call_status"]
            .astype("string")
            .str.upper()
            .eq("FAILED_CLOSED")
            .sum()
        )

    if integrity.completed_ai_outcome_count != integrity.decision_count:
        raise OperationalStateIntegrityError(
            "Completed AI outcomes and final decisions are inconsistent."
        )

    if integrity.technical_requeue_count < 1:
        raise OperationalStateIntegrityError(
            "The final gate contains no explicit technical requeue audit."
        )

    if failed_ai_call_count < 1:
        raise OperationalStateIntegrityError(
            "The failed-closed call history was not preserved."
        )

    return OperationalResilienceGateReport(
        run_date=str(run_date),
        gate_status="PASSED",
        termination_status="TERMINATED",
        termination_reason="FRONTIER_EXHAUSTED",
        guardrail_status="TELEMETRY_ONLY",
        node_count=integrity.node_count,
        edge_count=integrity.edge_count,
        decision_count=integrity.decision_count,
        expansion_ledger_count=integrity.expansion_ledger_count,
        frontier_queue_count=integrity.frontier_queue_count,
        ai_call_count=integrity.ai_call_count,
        completed_ai_outcome_count=(
            integrity.completed_ai_outcome_count
        ),
        failed_ai_call_count=failed_ai_call_count,
        technical_requeue_count=integrity.technical_requeue_count,
        repeated_ai_call_count=repeated_ai_call_count,
        repeated_expansion_count=repeated_expansion_count,
        stable_node_ids=stable_node_ids,
        stable_edge_ids=stable_edge_ids,
        stable_feature_snapshot_hashes=stable_subject_hashes,
    )


def persist_operational_resilience_gate_report(
    *,
    state_directory: Path | str,
    report: OperationalResilienceGateReport,
) -> Path:
    """Persist the final gate report atomically as compact JSON."""
    directory = Path(state_directory)
    directory.mkdir(parents=True, exist_ok=True)
    destination = (
        directory / OPERATIONAL_RESILIENCE_GATE_FILENAME
    )
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(
            report.to_record(),
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    temporary.replace(destination)

    return destination
