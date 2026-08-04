"""Node-by-node analyst review queue for one investigation."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
)
from network_mule_discovery.analyst_feedback import (
    ANALYST_FEEDBACK_COLUMNS,
)


REVIEWABLE_DECISION_CATEGORIES = frozenset({
    "CONTINUE",
    "STOP",
    "DETERMINISTIC",
})
REVIEW_STATUS_VALUES = frozenset({
    "UNREVIEWED",
    "REVIEWED_CORRECT",
    "REVIEWED_INCORRECT",
})
REVIEW_QUEUE_REQUIRED_NODE_COLUMNS = (
    "node_id",
    "node_type",
    "entity_key",
    "counterparty_key",
    "display_label",
    "decision_category",
    "ai_decision",
    "decision_label",
    "confidence",
    "rationale",
    "key_evidence",
    "discovered_via",
    "is_seed",
)
ANALYST_REVIEW_QUEUE_COLUMNS = (
    "review_order",
    "node_id",
    "subject_type",
    "subject_key",
    "display_label",
    "decision_category",
    "ai_decision",
    "decision_label",
    "review_outcome",
    "confidence",
    "rationale",
    "key_evidence",
    "discovered_via",
    "evidence_status",
    "review_status",
    "latest_feedback",
    "latest_analyst_notes",
    "latest_analyst_id",
    "latest_submitted_at",
)


@dataclass(frozen=True)
class AnalystReviewQueue:
    """One analyst's required node-decision review queue."""

    run_id: str
    group_id: str
    analyst_id: str
    rows: pd.DataFrame
    total_required: int
    reviewed_count: int
    unreviewed_count: int
    correct_count: int
    incorrect_count: int
    completion_percentage: float
    review_complete: bool


def _clean_text(value: object) -> str:
    """Return normalized text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _validate_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...],
    *,
    frame_name: str,
) -> None:
    """Validate required queue input columns."""
    missing_columns = sorted(
        set(required_columns)
        - set(frame.columns)
    )

    if missing_columns:
        raise AnalystApplicationStateError(
            f"{frame_name} is missing columns: "
            f"{missing_columns}"
        )


def _subject_key(
    row: object,
) -> tuple[str, str]:
    """Return the persisted review subject for one node."""
    subject_type = _clean_text(
        getattr(row, "node_type")
    ).upper()

    if subject_type == "CUSTOMER":
        subject_key = _clean_text(
            getattr(row, "entity_key")
        )
    elif subject_type == "COUNTERPARTY":
        subject_key = _clean_text(
            getattr(row, "counterparty_key")
        )
    else:
        subject_key = ""

    if not subject_type or not subject_key:
        raise AnalystApplicationStateError(
            "A reviewable node has no valid "
            "subject type or subject key."
        )

    return subject_type, subject_key


def _review_outcome(
    decision_category: str,
) -> str:
    """Map the final node outcome to simple analyst meaning."""
    normalized = _clean_text(
        decision_category
    ).upper()

    if normalized in {
        "CONTINUE",
        "DETERMINISTIC",
    }:
        return "SUSPICIOUS"

    if normalized == "STOP":
        return "NON_SUSPICIOUS"

    raise AnalystApplicationStateError(
        "Unsupported reviewable decision category: "
        f"{decision_category}"
    )


def _evidence_status(
    *,
    decision_category: str,
    rationale: str,
    key_evidence: str,
    discovered_via: str,
) -> str:
    """Return whether the decision has corroborating evidence."""
    normalized = _clean_text(
        decision_category
    ).upper()

    if normalized == "DETERMINISTIC":
        available = bool(
            _clean_text(discovered_via)
        )
    else:
        available = bool(
            _clean_text(rationale)
            or _clean_text(key_evidence)
        )

    return (
        "AVAILABLE"
        if available
        else "MISSING"
    )


def _latest_feedback_by_node(
    feedback: pd.DataFrame,
    *,
    run_id: str,
    group_id: str,
    analyst_id: str,
) -> pd.DataFrame:
    """Return the latest feedback event for each node."""
    if feedback.empty:
        return pd.DataFrame(
            columns=list(
                ANALYST_FEEDBACK_COLUMNS
            )
        )

    _validate_columns(
        feedback,
        ANALYST_FEEDBACK_COLUMNS,
        frame_name="Analyst feedback",
    )

    scoped = feedback.loc[
        feedback["run_id"]
        .astype("string")
        .str.strip()
        .eq(run_id)
        & feedback["group_id"]
        .astype("string")
        .str.strip()
        .eq(group_id)
        & feedback["analyst_id"]
        .astype("string")
        .str.strip()
        .eq(analyst_id)
    ].copy()

    if scoped.empty:
        return scoped

    return (
        scoped.sort_values(
            by=[
                "submitted_at",
                "feedback_id",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=["node_id"],
            keep="last",
        )
        .reset_index(drop=True)
    )


def build_analyst_review_queue(
    *,
    run_id: str,
    group_id: str,
    nodes: pd.DataFrame,
    feedback: pd.DataFrame,
    analyst_id: str,
) -> AnalystReviewQueue:
    """Build the mandatory node-decision review queue."""
    normalized_run_id = _clean_text(
        run_id
    )
    normalized_group_id = _clean_text(
        group_id
    )
    normalized_analyst_id = (
        _clean_text(analyst_id)
        or "UNSPECIFIED"
    )

    if not normalized_run_id:
        raise AnalystApplicationStateError(
            "run_id cannot be empty."
        )

    if not normalized_group_id:
        raise AnalystApplicationStateError(
            "group_id cannot be empty."
        )

    _validate_columns(
        nodes,
        REVIEW_QUEUE_REQUIRED_NODE_COLUMNS,
        frame_name="Investigation nodes",
    )

    prepared = nodes.copy()
    prepared["decision_category"] = (
        prepared["decision_category"]
        .astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
    )
    prepared["ai_decision"] = (
        prepared["ai_decision"]
        .astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
    )
    prepared["is_seed"] = (
        prepared["is_seed"]
        .fillna(False)
        .astype(bool)
    )

    reviewable = prepared.loc[
        ~prepared["is_seed"]
        & prepared[
            "decision_category"
        ].isin(
            REVIEWABLE_DECISION_CATEGORIES
        )
        & prepared["ai_decision"].ne("")
        & prepared[
            "ai_decision"
        ].ne("PENDING")
    ].copy()

    if reviewable.empty:
        empty = pd.DataFrame(
            columns=list(
                ANALYST_REVIEW_QUEUE_COLUMNS
            )
        )

        return AnalystReviewQueue(
            run_id=normalized_run_id,
            group_id=normalized_group_id,
            analyst_id=(
                normalized_analyst_id
            ),
            rows=empty,
            total_required=0,
            reviewed_count=0,
            unreviewed_count=0,
            correct_count=0,
            incorrect_count=0,
            completion_percentage=100.0,
            review_complete=True,
        )

    records: list[dict[str, object]] = []

    for row in reviewable.itertuples(
        index=False
    ):
        subject_type, subject_key = (
            _subject_key(row)
        )
        decision_category = _clean_text(
            row.decision_category
        ).upper()
        rationale = _clean_text(
            row.rationale
        )
        key_evidence = _clean_text(
            row.key_evidence
        )
        discovered_via = _clean_text(
            row.discovered_via
        )

        records.append(
            {
                "node_id": _clean_text(
                    row.node_id
                ),
                "subject_type": subject_type,
                "subject_key": subject_key,
                "display_label": (
                    _clean_text(
                        row.display_label
                    )
                    or subject_key
                ),
                "decision_category": (
                    decision_category
                ),
                "ai_decision": _clean_text(
                    row.ai_decision
                ).upper(),
                "decision_label": (
                    _clean_text(
                        row.decision_label
                    )
                    or _clean_text(
                        row.ai_decision
                    )
                ),
                "review_outcome": (
                    _review_outcome(
                        decision_category
                    )
                ),
                "confidence": _clean_text(
                    row.confidence
                ),
                "rationale": rationale,
                "key_evidence": (
                    key_evidence
                ),
                "discovered_via": (
                    discovered_via
                ),
                "evidence_status": (
                    _evidence_status(
                        decision_category=(
                            decision_category
                        ),
                        rationale=rationale,
                        key_evidence=(
                            key_evidence
                        ),
                        discovered_via=(
                            discovered_via
                        ),
                    )
                ),
            }
        )

    queue = pd.DataFrame(records)

    if queue["node_id"].eq("").any():
        raise AnalystApplicationStateError(
            "A reviewable node has no node ID."
        )

    duplicated_node_ids = sorted(
        queue.loc[
            queue["node_id"].duplicated(
                keep=False
            ),
            "node_id",
        ].unique()
    )

    if duplicated_node_ids:
        raise AnalystApplicationStateError(
            "The review queue contains duplicate "
            f"node IDs: {duplicated_node_ids}"
        )

    latest = _latest_feedback_by_node(
        feedback,
        run_id=normalized_run_id,
        group_id=normalized_group_id,
        analyst_id=(
            normalized_analyst_id
        ),
    )

    if latest.empty:
        queue["latest_feedback"] = ""
        queue["latest_analyst_notes"] = ""
        queue["latest_analyst_id"] = ""
        queue["latest_submitted_at"] = ""
    else:
        latest_columns = latest[
            [
                "node_id",
                "feedback",
                "analyst_notes",
                "analyst_id",
                "submitted_at",
            ]
        ].rename(
            columns={
                "feedback": (
                    "latest_feedback"
                ),
                "analyst_notes": (
                    "latest_analyst_notes"
                ),
                "analyst_id": (
                    "latest_analyst_id"
                ),
                "submitted_at": (
                    "latest_submitted_at"
                ),
            }
        )

        queue = queue.merge(
            latest_columns,
            how="left",
            on="node_id",
            validate="one_to_one",
        )

        for column in (
            "latest_feedback",
            "latest_analyst_notes",
            "latest_analyst_id",
            "latest_submitted_at",
        ):
            queue[column] = (
                queue[column]
                .astype("string")
                .fillna("")
                .str.strip()
            )

    queue["review_status"] = (
        queue["latest_feedback"].map(
            {
                "AI_CORRECT": (
                    "REVIEWED_CORRECT"
                ),
                "AI_INCORRECT": (
                    "REVIEWED_INCORRECT"
                ),
            }
        )
        .fillna("UNREVIEWED")
    )

    invalid_review_status = sorted(
        set(queue["review_status"])
        - set(REVIEW_STATUS_VALUES)
    )

    if invalid_review_status:
        raise AnalystApplicationStateError(
            "The review queue contains invalid "
            "review statuses: "
            f"{invalid_review_status}"
        )

    status_order = {
        "UNREVIEWED": 0,
        "REVIEWED_INCORRECT": 1,
        "REVIEWED_CORRECT": 2,
    }
    outcome_order = {
        "SUSPICIOUS": 0,
        "NON_SUSPICIOUS": 1,
    }

    queue["_status_order"] = (
        queue["review_status"].map(
            status_order
        )
    )
    queue["_outcome_order"] = (
        queue["review_outcome"].map(
            outcome_order
        )
    )

    queue = (
        queue.sort_values(
            by=[
                "_status_order",
                "_outcome_order",
                "display_label",
                "node_id",
            ],
            kind="stable",
        )
        .drop(
            columns=[
                "_status_order",
                "_outcome_order",
            ]
        )
        .reset_index(drop=True)
    )

    queue.insert(
        0,
        "review_order",
        range(1, len(queue) + 1),
    )

    queue = queue[
        list(
            ANALYST_REVIEW_QUEUE_COLUMNS
        )
    ]

    total_required = len(queue)
    correct_count = int(
        queue["review_status"]
        .eq("REVIEWED_CORRECT")
        .sum()
    )
    incorrect_count = int(
        queue["review_status"]
        .eq("REVIEWED_INCORRECT")
        .sum()
    )
    reviewed_count = (
        correct_count
        + incorrect_count
    )
    unreviewed_count = (
        total_required
        - reviewed_count
    )
    completion_percentage = (
        100.0
        if total_required == 0
        else round(
            reviewed_count
            / total_required
            * 100.0,
            1,
        )
    )

    return AnalystReviewQueue(
        run_id=normalized_run_id,
        group_id=normalized_group_id,
        analyst_id=normalized_analyst_id,
        rows=queue,
        total_required=total_required,
        reviewed_count=reviewed_count,
        unreviewed_count=unreviewed_count,
        correct_count=correct_count,
        incorrect_count=incorrect_count,
        completion_percentage=(
            completion_percentage
        ),
        review_complete=(
            unreviewed_count == 0
        ),
    )
