"""Analyst display projection over the complete persisted graph."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
)


SUPPRESSED_COUNTERPARTY_PREFIX = (
    "COUNTERPARTY_SUPPRESSED"
)
SUPPRESSED_SHARED_EDGE_TYPE = (
    "SHARED_EXTERNAL_COUNTERPARTY"
)

COLLAPSED_COUNTERPARTY_COLUMNS = (
    "counterparty_node_id",
    "counterparty_key",
    "display_label",
    "node_status",
    "observed_linked_customer_count",
    "collapsed_customer_count",
    "visible_linked_customer_count",
)


@dataclass(frozen=True)
class AnalystNetworkDisplayProjection:
    """Concise graph plus suppressed-link summaries."""

    nodes: pd.DataFrame
    edges: pd.DataFrame
    collapsed_counterparties: pd.DataFrame
    hidden_node_count: int
    hidden_edge_count: int


def _validate_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...],
    *,
    frame_name: str,
) -> None:
    """Validate one graph frame."""
    missing_columns = sorted(
        set(required_columns) - set(frame.columns)
    )

    if missing_columns:
        raise AnalystApplicationStateError(
            f"{frame_name} is missing columns: "
            f"{missing_columns}"
        )


def _clean_text(value: object) -> str:
    """Return normalized text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _truthy(value: object) -> bool:
    """Return a stable boolean interpretation."""
    if isinstance(value, bool):
        return value

    return _clean_text(value).upper() in {
        "1",
        "TRUE",
        "T",
        "YES",
        "Y",
    }


def _is_protected_customer(
    row: pd.Series,
) -> bool:
    """Return whether a customer must remain visible."""
    assessment_status = _clean_text(
        row.get(
            "customer_assessment_status",
            "",
        )
    ).upper()

    node_roles = {
        role.strip().upper()
        for role in _clean_text(
            row.get("node_roles", "")
        ).split("|")
        if role.strip()
    }

    return (
        assessment_status
        in {
            "SEED_CONFIRMED",
            "MULE_LIKE",
        }
        or "SEED" in node_roles
        or _truthy(
            row.get(
                "expansion_source_flag",
                False,
            )
        )
    )


def build_analyst_network_display_projection(
    *,
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
) -> AnalystNetworkDisplayProjection:
    """Collapse customers linked only through suppressed hubs."""
    required_node_columns = (
        "node_id",
        "node_type",
        "counterparty_key",
        "display_label",
        "node_status",
        "customer_assessment_status",
        "node_roles",
        "expansion_source_flag",
    )
    required_edge_columns = (
        "edge_id",
        "source_node_id",
        "target_node_id",
        "edge_type",
        "relationship_status",
    )

    _validate_columns(
        nodes,
        required_node_columns,
        frame_name="Analyst network nodes",
    )
    _validate_columns(
        edges,
        required_edge_columns,
        frame_name="Analyst network edges",
    )

    source_nodes = nodes.copy()
    source_edges = edges.copy()

    node_types = (
        source_nodes["node_type"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    node_statuses = (
        source_nodes["node_status"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    suppressed_counterparties = (
        source_nodes.loc[
            node_types.eq("COUNTERPARTY")
            & node_statuses.str.startswith(
                SUPPRESSED_COUNTERPARTY_PREFIX,
                na=False,
            )
        ]
        .copy()
    )

    suppressed_counterparty_ids = set(
        suppressed_counterparties[
            "node_id"
        ]
        .astype("string")
        .str.strip()
    )

    edge_types = (
        source_edges["edge_type"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    relationship_statuses = (
        source_edges["relationship_status"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    suppressed_shared_edges = (
        source_edges.loc[
            edge_types.eq(
                SUPPRESSED_SHARED_EDGE_TYPE
            )
            & relationship_statuses.str.startswith(
                SUPPRESSED_COUNTERPARTY_PREFIX,
                na=False,
            )
            & (
                source_edges[
                    "source_node_id"
                ].isin(
                    suppressed_counterparty_ids
                )
                |
                source_edges[
                    "target_node_id"
                ].isin(
                    suppressed_counterparty_ids
                )
            )
        ]
        .copy()
    )

    customer_rows = source_nodes.loc[
        node_types.eq("CUSTOMER")
    ].set_index("node_id")

    candidate_customer_ids: set[str] = set()

    for edge in suppressed_shared_edges.itertuples(
        index=False
    ):
        source_id = _clean_text(
            edge.source_node_id
        )
        target_id = _clean_text(
            edge.target_node_id
        )

        for node_id in (
            source_id,
            target_id,
        ):
            if node_id in customer_rows.index:
                candidate_customer_ids.add(
                    node_id
                )

    suppressed_edge_ids = set(
        suppressed_shared_edges["edge_id"]
        .astype("string")
        .str.strip()
    )
    hidden_customer_ids: set[str] = set()

    for customer_id in sorted(
        candidate_customer_ids
    ):
        customer_row = customer_rows.loc[
            customer_id
        ]

        if isinstance(
            customer_row,
            pd.DataFrame,
        ):
            raise AnalystApplicationStateError(
                "Persisted graph contains duplicate "
                f"customer node ID: {customer_id}"
            )

        if _is_protected_customer(
            customer_row
        ):
            continue

        incident_edges = source_edges.loc[
            source_edges[
                "source_node_id"
            ].eq(customer_id)
            |
            source_edges[
                "target_node_id"
            ].eq(customer_id)
        ]

        incident_edge_ids = set(
            incident_edges["edge_id"]
            .astype("string")
            .str.strip()
        )

        if (
            incident_edge_ids
            and incident_edge_ids.issubset(
                suppressed_edge_ids
            )
        ):
            hidden_customer_ids.add(
                customer_id
            )

    visible_nodes = (
        source_nodes.loc[
            ~source_nodes["node_id"].isin(
                hidden_customer_ids
            )
        ]
        .copy()
        .reset_index(drop=True)
    )
    visible_node_ids = set(
        visible_nodes["node_id"]
    )

    visible_edges = (
        source_edges.loc[
            source_edges[
                "source_node_id"
            ].isin(visible_node_ids)
            & source_edges[
                "target_node_id"
            ].isin(visible_node_ids)
        ]
        .copy()
        .reset_index(drop=True)
    )

    collapsed_records: list[
        dict[str, object]
    ] = []

    for counterparty in (
        suppressed_counterparties
        .itertuples(index=False)
    ):
        counterparty_node_id = _clean_text(
            counterparty.node_id
        )

        counterparty_edges = (
            suppressed_shared_edges.loc[
                suppressed_shared_edges[
                    "source_node_id"
                ].eq(counterparty_node_id)
                |
                suppressed_shared_edges[
                    "target_node_id"
                ].eq(counterparty_node_id)
            ]
        )

        linked_customer_ids: set[str] = set()

        for edge in counterparty_edges.itertuples(
            index=False
        ):
            for node_id in (
                _clean_text(
                    edge.source_node_id
                ),
                _clean_text(
                    edge.target_node_id
                ),
            ):
                if node_id in customer_rows.index:
                    linked_customer_ids.add(
                        node_id
                    )

        collapsed_ids = (
            linked_customer_ids
            & hidden_customer_ids
        )

        collapsed_records.append(
            {
                "counterparty_node_id": (
                    counterparty_node_id
                ),
                "counterparty_key": (
                    _clean_text(
                        counterparty.counterparty_key
                    )
                ),
                "display_label": (
                    _clean_text(
                        counterparty.display_label
                    )
                ),
                "node_status": (
                    _clean_text(
                        counterparty.node_status
                    )
                ),
                "observed_linked_customer_count": (
                    len(linked_customer_ids)
                ),
                "collapsed_customer_count": (
                    len(collapsed_ids)
                ),
                "visible_linked_customer_count": (
                    len(
                        linked_customer_ids
                        - hidden_customer_ids
                    )
                ),
            }
        )

    collapsed_counterparties = (
        pd.DataFrame.from_records(
            collapsed_records,
            columns=(
                COLLAPSED_COUNTERPARTY_COLUMNS
            ),
        )
    )

    collapsed_count_by_node = {
        str(row.counterparty_node_id): int(
            row.collapsed_customer_count
        )
        for row
        in collapsed_counterparties.itertuples(
            index=False
        )
    }

    visible_nodes[
        "collapsed_customer_count"
    ] = (
        visible_nodes["node_id"]
        .map(collapsed_count_by_node)
        .fillna(0)
        .astype(int)
    )

    return AnalystNetworkDisplayProjection(
        nodes=visible_nodes,
        edges=visible_edges,
        collapsed_counterparties=(
            collapsed_counterparties
        ),
        hidden_node_count=(
            len(source_nodes)
            - len(visible_nodes)
        ),
        hidden_edge_count=(
            len(source_edges)
            - len(visible_edges)
        ),
    )
