"""Read-only selected-group network contract for the analyst app."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
    AnalystApplicationStateStore,
    AnalystGroupSummary,
)


ANALYST_NETWORK_NODE_REQUIRED_COLUMNS = (
    "group_id",
    "node_id",
    "node_key",
    "node_type",
    "entity_key",
    "counterparty_key",
    "display_label",
    "node_roles",
    "node_status",
    "customer_assessment_status",
    "customer_discovery_allowed_flag",
    "expansion_source_flag",
)

ANALYST_NETWORK_EDGE_REQUIRED_COLUMNS = (
    "group_id",
    "edge_id",
    "source_node_id",
    "target_node_id",
    "source_node_key",
    "target_node_key",
    "edge_type",
    "relationship_status",
    "evidence_key",
    "evidence_summary",
)


@dataclass(frozen=True)
class AnalystGroupNetworkSnapshot:
    """All persisted analyst data for one selected group."""

    run_id: str
    group_id: str
    summary: AnalystGroupSummary
    nodes: pd.DataFrame
    edges: pd.DataFrame
    decisions: pd.DataFrame
    frontier_queue: pd.DataFrame
    expansion_ledger: pd.DataFrame


def _normalize_identifier(
    value: object,
    *,
    field_name: str,
) -> str:
    """Return one required nonblank identifier."""
    normalized = str(value).strip()

    if not normalized:
        raise AnalystApplicationStateError(
            f"{field_name} must be nonblank."
        )

    return normalized


def _validate_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...],
    *,
    frame_name: str,
) -> None:
    """Validate one persisted group-network frame."""
    missing_columns = sorted(
        set(required_columns) - set(frame.columns)
    )

    if missing_columns:
        raise AnalystApplicationStateError(
            f"{frame_name} is missing columns: "
            f"{missing_columns}"
        )


def _contains_group_id(
    value: object,
    group_id: str,
) -> bool:
    """Return whether a pipe-separated field contains a group."""
    if value is None or pd.isna(value):
        return False

    return group_id in {
        part.strip()
        for part in str(value).split("|")
        if part.strip()
    }


def _nonblank_values(
    series: pd.Series,
) -> set[str]:
    """Return normalized nonblank values from one series."""
    values: set[str] = set()

    for value in series:
        if value is None or pd.isna(value):
            continue

        normalized = str(value).strip()

        if normalized:
            values.add(normalized)

    return values


class AnalystGroupNetworkStore:
    """Read-only loader for one persisted network group."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.application = (
            AnalystApplicationStateStore(
                state_directory
            )
        )

    def load(
        self,
        *,
        run_id: str,
        group_id: str,
    ) -> AnalystGroupNetworkSnapshot:
        """Load and validate one selected persisted group."""
        normalized_run_id = _normalize_identifier(
            run_id,
            field_name="run_id",
        )
        normalized_group_id = _normalize_identifier(
            group_id,
            field_name="group_id",
        )

        summaries = {
            summary.group_id: summary
            for summary
            in self.application.list_groups(
                normalized_run_id
            )
        }

        if normalized_group_id not in summaries:
            raise AnalystApplicationStateError(
                "Unknown persisted group for run: "
                f"{normalized_group_id}"
            )

        summary = summaries[normalized_group_id]
        snapshot = self.application.load_run(
            normalized_run_id
        )

        all_nodes = (
            snapshot.daily_state.network.nodes
            .copy()
        )
        all_edges = (
            snapshot.daily_state.network.edges
            .copy()
        )

        _validate_columns(
            all_nodes,
            ANALYST_NETWORK_NODE_REQUIRED_COLUMNS,
            frame_name="Persisted network nodes",
        )
        _validate_columns(
            all_edges,
            ANALYST_NETWORK_EDGE_REQUIRED_COLUMNS,
            frame_name="Persisted network edges",
        )

        nodes = (
            all_nodes.loc[
                all_nodes["group_id"]
                .astype("string")
                .str.strip()
                .eq(normalized_group_id)
            ]
            .sort_values(
                by=[
                    "node_type",
                    "node_key",
                    "node_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        edges = (
            all_edges.loc[
                all_edges["group_id"]
                .astype("string")
                .str.strip()
                .eq(normalized_group_id)
            ]
            .sort_values(
                by=[
                    "edge_type",
                    "source_node_key",
                    "target_node_key",
                    "edge_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        if len(nodes) != summary.total_node_count:
            raise AnalystApplicationStateError(
                "Persisted group node count does not "
                "match the group summary."
            )

        if len(edges) != summary.total_edge_count:
            raise AnalystApplicationStateError(
                "Persisted group edge count does not "
                "match the group summary."
            )

        node_ids = _nonblank_values(
            nodes["node_id"]
        )

        if len(node_ids) != len(nodes):
            raise AnalystApplicationStateError(
                "Persisted group contains duplicate or "
                "blank node IDs."
            )

        edge_ids = _nonblank_values(
            edges["edge_id"]
        )

        if len(edge_ids) != len(edges):
            raise AnalystApplicationStateError(
                "Persisted group contains duplicate or "
                "blank edge IDs."
            )

        endpoint_ids = (
            _nonblank_values(
                edges["source_node_id"]
            )
            | _nonblank_values(
                edges["target_node_id"]
            )
        )

        missing_endpoint_ids = sorted(
            endpoint_ids - node_ids
        )

        if missing_endpoint_ids:
            raise AnalystApplicationStateError(
                "Persisted group edges reference unknown "
                f"nodes: {missing_endpoint_ids}"
            )

        customer_keys = _nonblank_values(
            nodes.loc[
                nodes["node_type"].eq("CUSTOMER"),
                "entity_key",
            ]
        )
        counterparty_keys = _nonblank_values(
            nodes.loc[
                nodes["node_type"].eq(
                    "COUNTERPARTY"
                ),
                "counterparty_key",
            ]
        )

        decisions = (
            snapshot.daily_state.decision_store
            .copy()
        )

        if not decisions.empty:
            _validate_columns(
                decisions,
                (
                    "decision_id",
                    "subject_type",
                    "subject_key",
                    "decision",
                    "reason_code",
                    "decision_version",
                    "decided_at",
                    "source",
                ),
                frame_name="Persisted decisions",
            )

            decision_mask = (
                (
                    decisions["subject_type"]
                    .astype("string")
                    .str.upper()
                    .eq("CUSTOMER")
                    & decisions["subject_key"].isin(
                        customer_keys
                    )
                )
                |
                (
                    decisions["subject_type"]
                    .astype("string")
                    .str.upper()
                    .eq("COUNTERPARTY")
                    & decisions["subject_key"].isin(
                        counterparty_keys
                    )
                )
            )

            decisions = (
                decisions.loc[decision_mask]
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

        frontier_queue = (
            snapshot.daily_state.frontier_queue
            .copy()
        )

        if not frontier_queue.empty:
            _validate_columns(
                frontier_queue,
                (
                    "queue_item_id",
                    "group_ids",
                    "queue_status",
                    "action_type",
                    "subject_type",
                    "subject_key",
                    "priority",
                ),
                frame_name="Persisted frontier queue",
            )

            frontier_queue = (
                frontier_queue.loc[
                    frontier_queue["group_ids"].map(
                        lambda value: _contains_group_id(
                            value,
                            normalized_group_id,
                        )
                    )
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

        expansion_ledger = (
            snapshot.daily_state.expansion_ledger
            .copy()
        )

        if not expansion_ledger.empty:
            _validate_columns(
                expansion_ledger,
                (
                    "queue_item_id",
                    "round_number",
                    "group_ids",
                    "source_entity_key",
                    "relationship_rows_found",
                    "expansion_status",
                ),
                frame_name="Persisted expansion ledger",
            )

            expansion_ledger = (
                expansion_ledger.loc[
                    expansion_ledger[
                        "group_ids"
                    ].map(
                        lambda value: _contains_group_id(
                            value,
                            normalized_group_id,
                        )
                    )
                ]
                .sort_values(
                    by=[
                        "round_number",
                        "queue_item_id",
                    ],
                    kind="stable",
                )
                .reset_index(drop=True)
            )

        return AnalystGroupNetworkSnapshot(
            run_id=normalized_run_id,
            group_id=normalized_group_id,
            summary=summary,
            nodes=nodes,
            edges=edges,
            decisions=decisions,
            frontier_queue=frontier_queue,
            expansion_ledger=expansion_ledger,
        )
