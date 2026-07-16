"""Basic interface for viewing discovered mule-linkage groups."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"

GROUPS_PATH = OUTPUT_DIRECTORY / "discovered_groups.csv"
NODES_PATH = OUTPUT_DIRECTORY / "discovered_group_nodes.csv"
EDGES_PATH = OUTPUT_DIRECTORY / "discovered_group_edges.csv"


st.set_page_config(
    page_title="Mule Network Discovery",
    page_icon=None,
    layout="wide",
)


def _parse_boolean(series: pd.Series) -> pd.Series:
    """Convert persisted boolean values into Python booleans."""
    return (
        series
        .astype("string")
        .str.strip()
        .str.lower()
        .eq("true")
    )


@st.cache_data
def load_graph_outputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Load persisted Section 1 graph outputs."""
    required_paths = [
        GROUPS_PATH,
        NODES_PATH,
        EDGES_PATH,
    ]

    missing_paths = [
        path
        for path in required_paths
        if not path.exists()
    ]

    if missing_paths:
        missing_text = ", ".join(
            str(path.relative_to(PROJECT_ROOT))
            for path in missing_paths
        )

        raise FileNotFoundError(
            f"Missing graph output files: {missing_text}"
        )

    groups = pd.read_csv(
        GROUPS_PATH,
        dtype={
            "run_id": "string",
            "run_date": "string",
            "group_id": "string",
            "group_type": "string",
        },
        keep_default_na=False,
    )

    nodes = pd.read_csv(
        NODES_PATH,
        dtype="string",
        keep_default_na=False,
    )

    edges = pd.read_csv(
        EDGES_PATH,
        dtype="string",
        keep_default_na=False,
    )

    group_count_columns = [
        "seed_entity_count",
        "discovered_entity_count",
        "total_entity_count",
        "eid_count",
        "edge_count",
    ]

    for column in group_count_columns:
        groups[column] = pd.to_numeric(
            groups[column],
            errors="raise",
        ).astype(int)

    nodes["seed_flag"] = _parse_boolean(
        nodes["seed_flag"]
    )

    nodes["discovered_flag"] = _parse_boolean(
        nodes["discovered_flag"]
    )

    edges["deterministic_flag"] = _parse_boolean(
        edges["deterministic_flag"]
    )

    return groups, nodes, edges


def _escape_dot(value: object) -> str:
    """Escape a value for use inside a Graphviz label."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def build_graphviz_dot(
    group_nodes: pd.DataFrame,
    group_edges: pd.DataFrame,
) -> str:
    """Build an undirected Graphviz representation of one group."""
    lines = [
        "graph network {",
        "rankdir=LR;",
        'graph [bgcolor="transparent", pad="0.2", '
        'nodesep="0.8", ranksep="1.0"];',
        'node [fontname="Arial", fontsize="11"];',
        'edge [fontname="Arial", fontsize="9"];',
    ]

    for row in group_nodes.itertuples(index=False):
        role = "Seed" if row.seed_flag else "Discovered"

        label = _escape_dot(
            "\n".join(
                [
                    row.entity_type,
                    row.entity_id,
                    role,
                ]
            )
        )

        node_id = _escape_dot(row.node_id)

        if row.seed_flag:
            shape = "doublecircle"
            pen_width = "2"
        else:
            shape = "box"
            pen_width = "1"

        lines.append(
            f'"{node_id}" '
            f'[label="{label}", '
            f'shape="{shape}", '
            f'penwidth="{pen_width}"];'
        )

    for row in group_edges.itertuples(index=False):
        source_node_id = _escape_dot(
            row.source_node_id
        )

        target_node_id = _escape_dot(
            row.target_node_id
        )

        lines.append(
            f'"{source_node_id}" -- '
            f'"{target_node_id}" '
            '[label="Same EID"];'
        )

    lines.append("}")

    return "\n".join(lines)


def main() -> None:
    """Render the Section 1 discovery interface."""
    st.title("Mule Network Discovery")
    st.caption(
        "Read-only visualization of the Section 1 "
        "EID discovery outputs."
    )

    if st.sidebar.button(
        "Reload output files",
        type="secondary",
    ):
        st.cache_data.clear()
        st.rerun()

    try:
        groups, nodes, edges = load_graph_outputs()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.write(
            "Generate the output files before starting "
            "the interface:"
        )
        st.code(
            "python scripts/run_eid_demo.py",
            language="bash",
        )
        st.stop()

    if groups.empty:
        st.warning("No discovered groups are available.")
        st.stop()

    run_dates = sorted(
        groups["run_date"].drop_duplicates()
    )

    summary_columns = st.columns(4)

    summary_columns[0].metric(
        "Run date",
        run_dates[-1],
    )

    summary_columns[1].metric(
        "Groups",
        len(groups),
    )

    summary_columns[2].metric(
        "Entities",
        nodes["entity_key"].nunique(),
    )

    summary_columns[3].metric(
        "EID links",
        len(edges),
    )

    st.subheader("Discovered groups")
    st.caption(
        "Select a row to display that group's network."
    )

    groups_sorted = (
        groups
        .sort_values(
            by=[
                "total_entity_count",
                "group_id",
            ],
            ascending=[False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    group_table = groups_sorted[
        [
            "group_id",
            "group_type",
            "seed_entity_count",
            "discovered_entity_count",
            "total_entity_count",
            "eid_count",
            "edge_count",
        ]
    ].rename(
        columns={
            "group_id": "Group ID",
            "group_type": "Group type",
            "seed_entity_count": "Seeds",
            "discovered_entity_count": "Discovered",
            "total_entity_count": "Entities",
            "eid_count": "EIDs",
            "edge_count": "Edges",
        }
    )

    selection_event = st.dataframe(
        group_table,
        width="stretch",
        hide_index=True,
        key="group_selection",
        on_select="rerun",
        selection_mode="single-row-required",
    )

    selected_rows = list(
        selection_event.selection.rows
    )

    selected_position = (
        selected_rows[0]
        if selected_rows
        else 0
    )

    selected_group = groups_sorted.iloc[
        selected_position
    ]

    selected_group_id = selected_group["group_id"]

    selected_nodes = (
        nodes.loc[
            nodes["group_id"] == selected_group_id
        ]
        .sort_values(
            by=[
                "seed_flag",
                "entity_type",
                "entity_id",
            ],
            ascending=[False, True, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    selected_edges = (
        edges.loc[
            edges["group_id"] == selected_group_id
        ]
        .sort_values(
            by=[
                "source_entity_key",
                "target_entity_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    st.divider()
    st.subheader(f"Selected group: {selected_group_id}")

    group_summary_columns = st.columns(5)

    group_summary_columns[0].metric(
        "Type",
        selected_group["group_type"],
    )

    group_summary_columns[1].metric(
        "Seeds",
        int(selected_group["seed_entity_count"]),
    )

    group_summary_columns[2].metric(
        "Discovered",
        int(
            selected_group[
                "discovered_entity_count"
            ]
        ),
    )

    group_summary_columns[3].metric(
        "Entities",
        int(selected_group["total_entity_count"]),
    )

    group_summary_columns[4].metric(
        "EID links",
        int(selected_group["edge_count"]),
    )

    st.caption(
        "Seed entities use a double-circle shape. "
        "Discovered entities use a box shape."
    )

    graphviz_dot = build_graphviz_dot(
        group_nodes=selected_nodes,
        group_edges=selected_edges,
    )

    st.graphviz_chart(
        graphviz_dot,
        width="stretch",
    )

    with st.expander(
        "Group entities",
        expanded=False,
    ):
        node_table = selected_nodes[
            [
                "entity_type",
                "entity_id",
                "entity_key",
                "seed_flag",
                "discovered_flag",
                "discovery_reason_code",
                "seed_sources",
                "entity_created_at",
            ]
        ].rename(
            columns={
                "entity_type": "Entity type",
                "entity_id": "Entity ID",
                "entity_key": "Entity key",
                "seed_flag": "Seed",
                "discovered_flag": "Discovered",
                "discovery_reason_code": "Discovery reason",
                "seed_sources": "Seed source",
                "entity_created_at": "Created at",
            }
        )

        st.dataframe(
            node_table,
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "EID edges",
        expanded=False,
    ):
        edge_table = selected_edges[
            [
                "source_entity_key",
                "target_entity_key",
                "emirates_id_number",
                "seed_individual_ids",
                "candidate_individual_ids",
                "reason_code",
            ]
        ].rename(
            columns={
                "source_entity_key": "Seed entity",
                "target_entity_key": "Linked entity",
                "emirates_id_number": "Emirates ID",
                "seed_individual_ids": "Seed individual",
                "candidate_individual_ids": "Linked individual",
                "reason_code": "Reason",
            }
        )

        st.dataframe(
            edge_table,
            width="stretch",
            hide_index=True,
        )


if __name__ == "__main__":
    main()
