"""Unified seed-led mule network interface."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"

GROUPS_PATH = OUTPUT_DIRECTORY / "unified_groups.csv"
NODES_PATH = OUTPUT_DIRECTORY / "unified_group_nodes.csv"
EDGES_PATH = OUTPUT_DIRECTORY / "unified_group_edges.csv"


st.set_page_config(
    page_title="Mule Network Discovery",
    page_icon=None,
    layout="wide",
)


def _read_csv(path: Path) -> pd.DataFrame:
    """Read a required persisted output."""
    if not path.exists():
        raise FileNotFoundError(
            f"Missing output file: "
            f"{path.relative_to(PROJECT_ROOT)}"
        )

    return pd.read_csv(
        path,
        dtype="string",
        keep_default_na=False,
    )


def _parse_boolean(series: pd.Series) -> pd.Series:
    """Convert persisted boolean text to booleans."""
    return (
        series
        .astype("string")
        .str.strip()
        .str.lower()
        .eq("true")
    )


@st.cache_data
def load_unified_outputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Load the unified group projection."""
    groups = _read_csv(GROUPS_PATH)
    nodes = _read_csv(NODES_PATH)
    edges = _read_csv(EDGES_PATH)

    numeric_group_columns = [
        "seed_entity_count",
        "customer_count",
        "counterparty_count",
        "eid_link_count",
        "counterparty_candidate_count",
        "shared_counterparty_customer_count",
        "beneficiary_seed_link_count",
        "customer_assessment_pending_count",
        "counterparty_ai_pending_count",
        "recursive_expansion_source_count",
        "total_node_count",
        "total_edge_count",
    ]

    for column in numeric_group_columns:
        groups[column] = pd.to_numeric(
            groups[column],
            errors="raise",
        ).astype(int)

    node_boolean_columns = [
        "customer_discovery_allowed_flag",
        "expansion_source_flag",
    ]

    for column in node_boolean_columns:
        nodes[column] = _parse_boolean(
            nodes[column]
        )

    edge_boolean_columns = [
        "customer_discovery_allowed_flag",
        "recursive_expansion_allowed_flag",
    ]

    for column in edge_boolean_columns:
        edges[column] = _parse_boolean(
            edges[column]
        )

    return groups, nodes, edges


def _escape_dot(value: object) -> str:
    """Escape text for a Graphviz label."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _humanize(value: object) -> str:
    """Convert an internal status to display text."""
    return (
        str(value)
        .replace("_", " ")
        .title()
    )


def _build_node_statement(
    row: object,
) -> str:
    """Build one Graphviz node statement."""
    node_id = _escape_dot(row.node_id)

    if row.node_type == "COUNTERPARTY":
        label_lines = [
            row.display_label,
            "External counterparty",
            _humanize(row.node_status),
        ]

        shape = "ellipse"
        style = "dashed"
        penwidth = "1"

    else:
        label_lines = [
            row.entity_key,
            _humanize(row.node_status),
            _humanize(
                row.customer_assessment_status
            ),
        ]

        if row.expansion_source_flag:
            shape = "doublecircle"
            style = "solid"
            penwidth = "2"
        elif (
            row.node_status
            == "OBSERVED_PENDING_COUNTERPARTY_AI"
        ):
            shape = "box"
            style = "dashed"
            penwidth = "1"
        else:
            shape = "box"
            style = "solid"
            penwidth = "1"

    label = _escape_dot(
        "\n".join(label_lines)
    )

    return (
        f'"{node_id}" '
        f'[label="{label}", '
        f'shape="{shape}", '
        f'style="{style}", '
        f'penwidth="{penwidth}"];'
    )


def _edge_display_properties(
    edge_type: str,
) -> tuple[str, str, str]:
    """Return label, style, and direction."""
    properties = {
        "SAME_EMIRATES_ID": (
            "Same Emirates ID",
            "solid",
            "none",
        ),
        "SEED_COUNTERPARTY_EVIDENCE": (
            "Seed transfer evidence",
            "dashed",
            "forward",
        ),
        "SHARED_EXTERNAL_COUNTERPARTY": (
            "Shared counterparty",
            "dashed",
            "forward",
        ),
        "BENEFICIARY_ADDED_SEED_ACCOUNT": (
            "Added seed as beneficiary",
            "solid",
            "forward",
        ),
    }

    return properties.get(
        edge_type,
        (
            _humanize(edge_type),
            "dashed",
            "forward",
        ),
    )


def build_group_graphviz_dot(
    group_nodes: pd.DataFrame,
    group_edges: pd.DataFrame,
) -> str:
    """Build one unified observed-evidence graph."""
    lines = [
        "digraph network {",
        "rankdir=LR;",
        'graph [bgcolor="transparent", '
        'pad="0.2", nodesep="0.8", '
        'ranksep="1.0"];',
        'node [fontname="Arial", fontsize="10"];',
        'edge [fontname="Arial", fontsize="9"];',
    ]

    for row in group_nodes.itertuples(
        index=False
    ):
        lines.append(
            _build_node_statement(row)
        )

    for row in group_edges.itertuples(
        index=False
    ):
        source_node_id = _escape_dot(
            row.source_node_id
        )

        target_node_id = _escape_dot(
            row.target_node_id
        )

        (
            edge_label,
            edge_style,
            edge_direction,
        ) = _edge_display_properties(
            row.edge_type
        )

        label = _escape_dot(edge_label)

        lines.append(
            f'"{source_node_id}" -> '
            f'"{target_node_id}" '
            f'[label="{label}", '
            f'style="{edge_style}", '
            f'dir="{edge_direction}"];'
        )

    lines.append("}")

    return "\n".join(lines)


def render_group_summary(
    selected_group: pd.Series,
) -> None:
    """Render selected group metrics."""
    first_row = st.columns(6)

    first_row[0].metric(
        "Seed",
        selected_group[
            "group_anchor_seed_entity_key"
        ],
    )

    first_row[1].metric(
        "Customers",
        int(selected_group["customer_count"]),
    )

    first_row[2].metric(
        "Counterparties",
        int(
            selected_group[
                "counterparty_count"
            ]
        ),
    )

    first_row[3].metric(
        "EID links",
        int(selected_group["eid_link_count"]),
    )

    first_row[4].metric(
        "Shared-counterparty links",
        int(
            selected_group[
                "shared_counterparty_customer_count"
            ]
        ),
    )

    first_row[5].metric(
        "Beneficiary links",
        int(
            selected_group[
                "beneficiary_seed_link_count"
            ]
        ),
    )

    second_row = st.columns(4)

    second_row[0].metric(
        "Customer AI pending",
        int(
            selected_group[
                "customer_assessment_pending_count"
            ]
        ),
    )

    second_row[1].metric(
        "Counterparty AI pending",
        int(
            selected_group[
                "counterparty_ai_pending_count"
            ]
        ),
    )

    second_row[2].metric(
        "Expansion sources",
        int(
            selected_group[
                "recursive_expansion_source_count"
            ]
        ),
    )

    second_row[3].metric(
        "Total evidence edges",
        int(selected_group["total_edge_count"]),
    )


def render_group_tables(
    selected_nodes: pd.DataFrame,
    selected_edges: pd.DataFrame,
) -> None:
    """Render detailed node and edge evidence."""
    customer_nodes = selected_nodes.loc[
        selected_nodes["node_type"]
        == "CUSTOMER"
    ].copy()

    counterparty_nodes = selected_nodes.loc[
        selected_nodes["node_type"]
        == "COUNTERPARTY"
    ].copy()

    with st.expander(
        "Customer nodes",
        expanded=False,
    ):
        st.dataframe(
            customer_nodes[
                [
                    "entity_key",
                    "node_roles",
                    "node_status",
                    "customer_assessment_status",
                    "customer_discovery_allowed_flag",
                    "expansion_source_flag",
                    "first_seen_date",
                    "last_seen_date",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "External counterparty nodes",
        expanded=False,
    ):
        st.dataframe(
            counterparty_nodes[
                [
                    "counterparty_key",
                    "display_label",
                    "node_roles",
                    "node_status",
                    "customer_discovery_allowed_flag",
                    "first_seen_date",
                    "last_seen_date",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "Relationship evidence",
        expanded=False,
    ):
        st.dataframe(
            selected_edges[
                [
                    "edge_id",
                    "source_node_key",
                    "target_node_key",
                    "edge_type",
                    "relationship_status",
                    "customer_discovery_allowed_flag",
                    "recursive_expansion_allowed_flag",
                    "evidence_summary",
                    "source_event_count",
                    "candidate_event_count",
                    "first_seen_date",
                    "last_seen_date",
                ]
            ],
            width="stretch",
            hide_index=True,
        )


def main() -> None:
    """Render the unified discovery interface."""
    st.title("Mule Network Discovery")

    st.caption(
        "One seed-led network containing Emirates ID, "
        "transfer-counterparty, and beneficiary evidence."
    )

    if st.sidebar.button(
        "Reload output files",
        type="secondary",
    ):
        st.cache_data.clear()
        st.rerun()

    try:
        groups, nodes, edges = (
            load_unified_outputs()
        )
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.code(
            "python scripts/run_unified_group_demo.py",
            language="bash",
        )
        return

    if groups.empty:
        st.warning(
            "No unified seed groups are available."
        )
        return

    groups_sorted = (
        groups
        .sort_values(
            by=[
                "total_node_count",
                "group_id",
            ],
            ascending=[False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    st.subheader("Seed-led groups")

    group_table = groups_sorted[
        [
            "group_id",
            "group_anchor_seed_entity_key",
            "customer_count",
            "counterparty_count",
            "eid_link_count",
            "shared_counterparty_customer_count",
            "beneficiary_seed_link_count",
            "customer_assessment_pending_count",
            "counterparty_ai_pending_count",
            "total_node_count",
            "total_edge_count",
        ]
    ].rename(
        columns={
            "group_id": "Group ID",
            "group_anchor_seed_entity_key": (
                "Anchor seed"
            ),
            "customer_count": "Customers",
            "counterparty_count": (
                "Counterparties"
            ),
            "eid_link_count": "EID links",
            "shared_counterparty_customer_count": (
                "Shared-counterparty links"
            ),
            "beneficiary_seed_link_count": (
                "Beneficiary links"
            ),
            "customer_assessment_pending_count": (
                "Customer AI pending"
            ),
            "counterparty_ai_pending_count": (
                "Counterparty AI pending"
            ),
            "total_node_count": "Nodes",
            "total_edge_count": "Edges",
        }
    )

    st.dataframe(
        group_table,
        width="stretch",
        hide_index=True,
    )

    group_options = (
        groups_sorted["group_id"].tolist()
    )

    selected_group_id = st.selectbox(
        "Select a group",
        options=group_options,
        format_func=lambda group_id: (
            f"{group_id} — "
            f"{groups_sorted.loc[
                groups_sorted['group_id']
                == group_id,
                'group_anchor_seed_entity_key'
            ].iloc[0]}"
        ),
    )

    selected_group = (
        groups_sorted.loc[
            groups_sorted["group_id"]
            == selected_group_id
        ]
        .iloc[0]
    )

    selected_nodes = (
        nodes.loc[
            nodes["group_id"]
            == selected_group_id
        ]
        .sort_values(
            by=[
                "expansion_source_flag",
                "node_type",
                "node_key",
            ],
            ascending=[False, True, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    selected_edges = (
        edges.loc[
            edges["group_id"]
            == selected_group_id
        ]
        .sort_values(
            by=[
                "edge_type",
                "source_node_key",
                "target_node_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    st.divider()
    st.subheader(
        f"Unified network: {selected_group_id}"
    )

    render_group_summary(selected_group)

    st.warning(
        "This is the observed evidence graph. "
        "Counterparty branches remain blocked until "
        "the counterparty AI decision approves them, "
        "and only approved mule-like customers will "
        "become recursive expansion sources."
    )

    st.caption(
        "Double circle: current expansion source. "
        "Solid box: deterministic or assessment-ready "
        "customer. Dashed box or ellipse: blocked "
        "pending counterparty AI."
    )

    st.graphviz_chart(
        build_group_graphviz_dot(
            group_nodes=selected_nodes,
            group_edges=selected_edges,
        ),
        width="stretch",
    )

    render_group_tables(
        selected_nodes=selected_nodes,
        selected_edges=selected_edges,
    )


if __name__ == "__main__":
    main()
