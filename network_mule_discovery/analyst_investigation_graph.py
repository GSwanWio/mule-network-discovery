"""Interactive analyst graph presentation for an AI investigation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd
from streamlit_cytoscape import (
    EdgeStyle,
    Event,
    NodeStyle,
)

from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
)


REQUIRED_NODE_COLUMNS = (
    "node_id",
    "node_type",
    "display_label",
    "depth",
    "depth_label",
    "parent_node_id",
    "discovered_via",
    "is_seed",
    "ai_decision",
    "decision_label",
    "decision_category",
    "expansion_outcome",
    "confidence",
    "rationale",
    "key_evidence",
    "collapsed_customer_count",
)

NODE_SELECTED_EVENT = Event(
    "investigation_node_selected",
    "tap",
    "node",
)


@dataclass(frozen=True)
class AnalystInvestigationGraph:
    """Interactive breadth-and-depth graph configuration."""

    elements: dict[str, list[dict[str, Any]]]
    layout: dict[str, Any]
    seed_node_ids: tuple[str, ...]


def _clean_text(value: object) -> str:
    """Return normalized display text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _validate_nodes(nodes: pd.DataFrame) -> None:
    """Validate the investigation presentation input."""
    missing_columns = sorted(
        set(REQUIRED_NODE_COLUMNS)
        - set(nodes.columns)
    )

    if missing_columns:
        raise AnalystApplicationStateError(
            "Investigation graph nodes are missing "
            f"columns: {missing_columns}"
        )

    if nodes.empty:
        raise AnalystApplicationStateError(
            "Investigation graph contains no nodes."
        )

    node_ids = (
        nodes["node_id"]
        .astype("string")
        .str.strip()
    )

    if node_ids.eq("").any():
        raise AnalystApplicationStateError(
            "Investigation graph contains an empty node ID."
        )

    duplicated = sorted(
        node_ids.loc[
            node_ids.duplicated(keep=False)
        ].unique()
    )

    if duplicated:
        raise AnalystApplicationStateError(
            "Investigation graph contains duplicate "
            f"node IDs: {duplicated}"
        )


def _style_label(
    *,
    node_type: str,
    decision_category: str,
) -> str:
    """Return the visual category for one node."""
    normalized_type = node_type.upper()
    normalized_category = decision_category.upper()

    if normalized_category == "SEED":
        return "SEED_CUSTOMER"

    if normalized_category == "CONTINUE":
        if normalized_type == "CUSTOMER":
            return "EXPANDED_CUSTOMER"

        return "EXPANDED_COUNTERPARTY"

    if normalized_category == "STOP":
        if normalized_type == "CUSTOMER":
            return "STOPPED_CUSTOMER"

        return "STOPPED_COUNTERPARTY"

    if normalized_category == "FAILED":
        return "FAILED_NODE"

    return "PENDING_NODE"


def _edge_style_label(
    decision_category: str,
) -> str:
    """Return the visual category for one journey edge."""
    normalized = decision_category.upper()

    if normalized == "STOP":
        return "STOPPED_PATH"

    if normalized in {
        "FAILED",
        "PENDING",
    }:
        return "REVIEW_PATH"

    return "EXPANDED_PATH"


def _integer(value: object) -> int:
    """Return a safe integer display value."""
    converted = pd.to_numeric(
        pd.Series([value]),
        errors="coerce",
    ).fillna(0)

    return int(converted.iloc[0])


def build_analyst_investigation_graph(
    nodes: pd.DataFrame,
) -> AnalystInvestigationGraph:
    """Build the clickable AI-approved investigation journey."""
    _validate_nodes(nodes)

    prepared = nodes.copy()

    prepared["node_id"] = (
        prepared["node_id"]
        .astype("string")
        .str.strip()
    )
    prepared["parent_node_id"] = (
        prepared["parent_node_id"]
        .astype("string")
        .fillna("")
        .str.strip()
    )

    node_ids = set(prepared["node_id"])

    seed_node_ids = tuple(
        sorted(
            prepared.loc[
                prepared["is_seed"].astype(bool),
                "node_id",
            ]
        )
    )

    if not seed_node_ids:
        raise AnalystApplicationStateError(
            "Interactive investigation graph has no seed."
        )

    graph_nodes: list[dict[str, Any]] = []
    graph_edges: list[dict[str, Any]] = []

    for row in prepared.itertuples(index=False):
        node_id = _clean_text(row.node_id)
        node_type = _clean_text(
            row.node_type
        ).upper()
        decision_category = _clean_text(
            row.decision_category
        ).upper()
        display_label = (
            _clean_text(row.display_label)
            or node_id
        )
        collapsed_count = _integer(
            row.collapsed_customer_count
        )

        caption_lines = [
            display_label,
            (
                f"{_clean_text(row.depth_label)} · "
                f"{_clean_text(row.expansion_outcome)}"
            ),
        ]

        if collapsed_count > 0:
            caption_lines.append(
                f"{collapsed_count} linked customers — "
                "expansion stopped"
            )

        graph_nodes.append(
            {
                "selectable": False,
                "grabbable": False,
                "data": {
                    "id": node_id,
                    "label": _style_label(
                        node_type=node_type,
                        decision_category=(
                            decision_category
                        ),
                    ),
                    "_caption": "\n".join(
                        caption_lines
                    ),
                    "Entity": display_label,
                    "Type": node_type.title(),
                    "Depth": _clean_text(
                        row.depth_label
                    ),
                    "AI outcome": _clean_text(
                        row.expansion_outcome
                    ),
                    "AI decision": _clean_text(
                        row.decision_label
                    ),
                    "Confidence": _clean_text(
                        row.confidence
                    ),
                    "Discovered via": _clean_text(
                        row.discovered_via
                    ),
                    "Collapsed customers": (
                        collapsed_count
                    ),
                    "_node_id": node_id,
                    "_decision_category": (
                        decision_category
                    ),
                    "_rationale": _clean_text(
                        row.rationale
                    ),
                    "_key_evidence": _clean_text(
                        row.key_evidence
                    ),
                }
            }
        )

        if bool(row.is_seed):
            continue

        parent_node_id = _clean_text(
            row.parent_node_id
        )

        if not parent_node_id:
            raise AnalystApplicationStateError(
                "Non-seed investigation node has no "
                f"discovery parent: {node_id}"
            )

        if parent_node_id not in node_ids:
            raise AnalystApplicationStateError(
                "Investigation node references an unknown "
                f"parent: {node_id} -> {parent_node_id}"
            )

        graph_edges.append(
            {
                "data": {
                    "id": (
                        "JOURNEY|"
                        f"{parent_node_id}|{node_id}"
                    ),
                    "label": _edge_style_label(
                        decision_category
                    ),
                    "source": parent_node_id,
                    "target": node_id,
                    "_caption": _clean_text(
                        row.discovered_via
                    ),
                    "Relationship": _clean_text(
                        row.discovered_via
                    ),
                    "_child_decision_category": (
                        decision_category
                    ),
                }
            }
        )

    graph_nodes.sort(
        key=lambda element: (
            int(
                prepared.loc[
                    prepared["node_id"].eq(
                        element["data"]["id"]
                    ),
                    "depth",
                ].iloc[0]
            ),
            element["data"]["id"],
        )
    )

    graph_edges.sort(
        key=lambda element: element["data"]["id"]
    )

    return AnalystInvestigationGraph(
        elements={
            "nodes": graph_nodes,
            "edges": graph_edges,
        },
        layout={
            "name": "breadthfirst",
            "directed": True,
            "fit": True,
            "animate": False,
            "padding": 45,
            "spacingFactor": 1.60,
            "avoidOverlap": True,
            "nodeDimensionsIncludeLabels": True,
        },
        seed_node_ids=seed_node_ids,
    )


def analyst_node_styles() -> list[NodeStyle]:
    """Return the semantic analyst node styles."""
    common = {
        "width": 220,
        "height": 88,
        "text-wrap": "wrap",
        "text-max-width": 195,
        "text-valign": "center",
        "text-halign": "center",
        "font-size": 13,
        "font-weight": 600,
        "color": "#FFFFFF",
        "border-width": 3,
    }

    return [
        NodeStyle(
            "SEED_CUSTOMER",
            "#2563EB",
            "_caption",
            custom_styles={
                **common,
                "shape": "diamond",
                "border-color": "#1E3A8A",
            },
        ),
        NodeStyle(
            "EXPANDED_CUSTOMER",
            "#DC2626",
            "_caption",
            custom_styles={
                **common,
                "shape": "round-rectangle",
                "border-color": "#7F1D1D",
            },
        ),
        NodeStyle(
            "EXPANDED_COUNTERPARTY",
            "#F97316",
            "_caption",
            custom_styles={
                **common,
                "shape": "ellipse",
                "border-color": "#9A3412",
            },
        ),
        NodeStyle(
            "STOPPED_CUSTOMER",
            "#64748B",
            "_caption",
            custom_styles={
                **common,
                "shape": "round-rectangle",
                "border-color": "#334155",
            },
        ),
        NodeStyle(
            "STOPPED_COUNTERPARTY",
            "#64748B",
            "_caption",
            custom_styles={
                **common,
                "shape": "ellipse",
                "border-color": "#334155",
                "border-style": "dashed",
            },
        ),
        NodeStyle(
            "PENDING_NODE",
            "#D97706",
            "_caption",
            custom_styles={
                **common,
                "shape": "round-rectangle",
                "border-color": "#78350F",
                "border-style": "dashed",
            },
        ),
        NodeStyle(
            "FAILED_NODE",
            "#7F1D1D",
            "_caption",
            custom_styles={
                **common,
                "shape": "octagon",
                "border-color": "#450A0A",
            },
        ),
    ]


def analyst_edge_styles() -> list[EdgeStyle]:
    """Return semantic journey-edge styles."""
    common = {
        "font-size": 9,
        "text-wrap": "wrap",
        "text-max-width": 120,
        "text-background-opacity": 1,
        "text-background-padding": 3,
        "width": 3,
        "arrow-scale": 1.2,
    }

    return [
        EdgeStyle(
            "EXPANDED_PATH",
            "#475569",
            "_caption",
            directed=True,
            curve_style="bezier",
            custom_styles={
                **common,
                "line-style": "solid",
            },
        ),
        EdgeStyle(
            "STOPPED_PATH",
            "#94A3B8",
            "_caption",
            directed=True,
            curve_style="bezier",
            custom_styles={
                **common,
                "line-style": "dashed",
            },
        ),
        EdgeStyle(
            "REVIEW_PATH",
            "#D97706",
            "_caption",
            directed=True,
            curve_style="bezier",
            custom_styles={
                **common,
                "line-style": "dotted",
            },
        ),
    ]


def selected_investigation_node_id(
    component_value: object,
) -> str | None:
    """Extract a clicked node ID from the graph event."""
    if not isinstance(
        component_value,
        Mapping,
    ):
        return None

    if (
        component_value.get("action")
        != "investigation_node_selected"
    ):
        return None

    data = component_value.get("data")

    if not isinstance(data, Mapping):
        return None

    if data.get("target_group") != "nodes":
        return None

    node_id = _clean_text(
        data.get("target_id")
    )

    return node_id or None
