"""Relationship and AI evidence for one analyst-selected group."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
)
from network_mule_discovery.analyst_group_network import (
    AnalystGroupNetworkStore,
)
from network_mule_discovery.daily_ai_runner import (
    AI_CALL_LEDGER_COLUMNS,
)
from network_mule_discovery.decision_engine import (
    DECISION_REQUIRED_COLUMNS,
)


RELATIONSHIP_SOURCE_COLUMNS = (
    "edge_id",
    "source_node_id",
    "target_node_id",
    "edge_type",
    "relationship_status",
    "evidence_key",
    "evidence_summary",
    "source_event_count",
    "candidate_event_count",
    "first_seen_date",
    "last_seen_date",
)

ANALYST_RELATIONSHIP_EVIDENCE_COLUMNS = (
    "edge_id",
    "source_node_id",
    "target_node_id",
    "source_display_label",
    "target_display_label",
    "edge_type",
    "relationship_status",
    "evidence_key",
    "evidence_summary",
    "source_event_count",
    "candidate_event_count",
    "first_seen_date",
    "last_seen_date",
)

ANALYST_DECISION_EVIDENCE_COLUMNS = (
    "subject_display_label",
    *DECISION_REQUIRED_COLUMNS,
)

ANALYST_AI_CALL_EVIDENCE_COLUMNS = (
    "subject_display_label",
    *AI_CALL_LEDGER_COLUMNS,
)


@dataclass(frozen=True)
class AnalystGroupEvidenceSnapshot:
    """Auditable evidence associated with one persisted group."""

    run_id: str
    group_id: str
    relationship_evidence: pd.DataFrame
    decision_evidence: pd.DataFrame
    ai_call_evidence: pd.DataFrame


def _validate_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...],
    *,
    frame_name: str,
) -> None:
    """Validate one persisted evidence frame."""
    missing_columns = sorted(
        set(required_columns) - set(frame.columns)
    )

    if missing_columns:
        raise AnalystApplicationStateError(
            f"{frame_name} is missing columns: "
            f"{missing_columns}"
        )


def _clean_text(value: object) -> str:
    """Return a normalized display string."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _subject_labels(
    nodes: pd.DataFrame,
) -> dict[tuple[str, str], str]:
    """Map persisted decision subjects to display labels."""
    labels: dict[tuple[str, str], str] = {}

    for row in nodes.itertuples(index=False):
        subject_type = _clean_text(
            row.node_type
        ).upper()
        display_label = (
            _clean_text(row.display_label)
            or _clean_text(row.node_key)
        )

        if subject_type == "CUSTOMER":
            subject_key = _clean_text(
                row.entity_key
            )
        elif subject_type == "COUNTERPARTY":
            subject_key = _clean_text(
                row.counterparty_key
            )
        else:
            continue

        if subject_key:
            labels[
                (
                    subject_type,
                    subject_key,
                )
            ] = display_label

    return labels


class AnalystGroupEvidenceStore:
    """Read-only evidence loader for one persisted group."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.network_store = (
            AnalystGroupNetworkStore(
                state_directory
            )
        )

    def load(
        self,
        *,
        run_id: str,
        group_id: str,
    ) -> AnalystGroupEvidenceSnapshot:
        """Load relationship, decision, and AI-call evidence."""
        network = self.network_store.load(
            run_id=run_id,
            group_id=group_id,
        )
        run_snapshot = (
            self.network_store
            .application
            .load_run(run_id)
        )

        _validate_columns(
            network.edges,
            RELATIONSHIP_SOURCE_COLUMNS,
            frame_name="Persisted relationship evidence",
        )

        node_labels = (
            network.nodes[
                [
                    "node_id",
                    "display_label",
                ]
            ]
            .drop_duplicates(
                subset=["node_id"],
                keep="first",
            )
        )

        relationship_evidence = (
            network.edges[
                list(
                    RELATIONSHIP_SOURCE_COLUMNS
                )
            ]
            .merge(
                node_labels.rename(
                    columns={
                        "node_id": "source_node_id",
                        "display_label": (
                            "source_display_label"
                        ),
                    }
                ),
                how="left",
                on="source_node_id",
                validate="many_to_one",
            )
            .merge(
                node_labels.rename(
                    columns={
                        "node_id": "target_node_id",
                        "display_label": (
                            "target_display_label"
                        ),
                    }
                ),
                how="left",
                on="target_node_id",
                validate="many_to_one",
            )
        )

        relationship_evidence = (
            relationship_evidence[
                list(
                    ANALYST_RELATIONSHIP_EVIDENCE_COLUMNS
                )
            ]
            .sort_values(
                by=[
                    "edge_type",
                    "source_display_label",
                    "target_display_label",
                    "edge_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        if (
            relationship_evidence[
                "source_display_label"
            ].map(_clean_text).eq("").any()
            or relationship_evidence[
                "target_display_label"
            ].map(_clean_text).eq("").any()
        ):
            raise AnalystApplicationStateError(
                "Relationship evidence contains an "
                "unresolved node label."
            )

        subject_labels = _subject_labels(
            network.nodes
        )

        decisions = network.decisions.copy()

        if decisions.empty:
            decision_evidence = pd.DataFrame(
                columns=list(
                    ANALYST_DECISION_EVIDENCE_COLUMNS
                )
            )
        else:
            _validate_columns(
                decisions,
                DECISION_REQUIRED_COLUMNS,
                frame_name="Persisted decision evidence",
            )

            decision_evidence = decisions.copy()
            decision_evidence.insert(
                0,
                "subject_display_label",
                [
                    subject_labels.get(
                        (
                            _clean_text(
                                row.subject_type
                            ).upper(),
                            _clean_text(
                                row.subject_key
                            ),
                        ),
                        _clean_text(
                            row.subject_key
                        ),
                    )
                    for row
                    in decision_evidence.itertuples(
                        index=False
                    )
                ],
            )
            decision_evidence = (
                decision_evidence[
                    list(
                        ANALYST_DECISION_EVIDENCE_COLUMNS
                    )
                ]
                .sort_values(
                    by=[
                        "subject_type",
                        "subject_key",
                        "decided_at",
                        "decision_id",
                    ],
                    kind="stable",
                )
                .reset_index(drop=True)
            )

        calls = run_snapshot.ai_call_ledger.copy()

        if calls.empty:
            ai_call_evidence = pd.DataFrame(
                columns=list(
                    ANALYST_AI_CALL_EVIDENCE_COLUMNS
                )
            )
        else:
            _validate_columns(
                calls,
                AI_CALL_LEDGER_COLUMNS,
                frame_name="Persisted AI-call evidence",
            )

            call_mask = pd.Series(
                [
                    (
                        _clean_text(
                            row.subject_type
                        ).upper(),
                        _clean_text(
                            row.subject_key
                        ),
                    )
                    in subject_labels
                    for row in calls.itertuples(
                        index=False
                    )
                ],
                index=calls.index,
            )

            ai_call_evidence = (
                calls.loc[call_mask]
                .copy()
            )
            ai_call_evidence.insert(
                0,
                "subject_display_label",
                [
                    subject_labels[
                        (
                            _clean_text(
                                row.subject_type
                            ).upper(),
                            _clean_text(
                                row.subject_key
                            ),
                        )
                    ]
                    for row
                    in ai_call_evidence.itertuples(
                        index=False
                    )
                ],
            )
            ai_call_evidence = (
                ai_call_evidence[
                    list(
                        ANALYST_AI_CALL_EVIDENCE_COLUMNS
                    )
                ]
                .sort_values(
                    by=[
                        "attempted_at",
                        "ai_call_id",
                    ],
                    kind="stable",
                )
                .reset_index(drop=True)
            )

        return AnalystGroupEvidenceSnapshot(
            run_id=network.run_id,
            group_id=network.group_id,
            relationship_evidence=(
                relationship_evidence
            ),
            decision_evidence=decision_evidence,
            ai_call_evidence=ai_call_evidence,
        )
