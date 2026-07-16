"""Basic interface for viewing mule-network discovery outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"

GROUPS_PATH = OUTPUT_DIRECTORY / "discovered_groups.csv"
NODES_PATH = OUTPUT_DIRECTORY / "discovered_group_nodes.csv"
EDGES_PATH = OUTPUT_DIRECTORY / "discovered_group_edges.csv"

COUNTERPARTY_CANDIDATES_PATH = (
    OUTPUT_DIRECTORY / "counterparty_candidates.csv"
)

COUNTERPARTY_LINKS_PATH = (
    OUTPUT_DIRECTORY / "counterparty_candidate_links.csv"
)

BENEFICIARY_LINKS_PATH = (
    OUTPUT_DIRECTORY / "beneficiary_seed_links.csv"
)


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


def _read_csv(
    path: Path,
    dtype: str | dict[str, str] = "string",
) -> pd.DataFrame:
    """Read one required discovery output."""
    if not path.exists():
        raise FileNotFoundError(
            f"Missing output file: "
            f"{path.relative_to(PROJECT_ROOT)}"
        )

    return pd.read_csv(
        path,
        dtype=dtype,
        keep_default_na=False,
    )


@st.cache_data
def load_eid_outputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Load persisted Section 1 graph outputs."""
    groups = _read_csv(
        GROUPS_PATH,
        dtype={
            "run_id": "string",
            "run_date": "string",
            "group_id": "string",
            "group_type": "string",
        },
    )

    nodes = _read_csv(NODES_PATH)
    edges = _read_csv(EDGES_PATH)

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


@st.cache_data
def load_counterparty_outputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Load persisted Section 2 candidate outputs."""
    candidates = _read_csv(
        COUNTERPARTY_CANDIDATES_PATH
    )

    links = _read_csv(
        COUNTERPARTY_LINKS_PATH
    )

    beneficiary_links = _read_csv(
        BENEFICIARY_LINKS_PATH
    )

    candidate_numeric_columns = [
        "candidate_customer_count",
        "seed_event_count",
        "candidate_event_count",
    ]

    for column in candidate_numeric_columns:
        candidates[column] = pd.to_numeric(
            candidates[column],
            errors="raise",
        ).astype(int)

    candidates["expansion_allowed_flag"] = (
        _parse_boolean(
            candidates["expansion_allowed_flag"]
        )
    )

    links["expansion_allowed_flag"] = (
        _parse_boolean(
            links["expansion_allowed_flag"]
        )
    )

    beneficiary_links[
        "expansion_allowed_flag"
    ] = _parse_boolean(
        beneficiary_links[
            "expansion_allowed_flag"
        ]
    )

    return candidates, links, beneficiary_links


def _escape_dot(value: object) -> str:
    """Escape a value for use inside a Graphviz label."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def build_eid_graphviz_dot(
    group_nodes: pd.DataFrame,
    group_edges: pd.DataFrame,
) -> str:
    """Build an undirected EID group graph."""
    lines = [
        "graph network {",
        "rankdir=LR;",
        'graph [bgcolor="transparent", pad="0.2", '
        'nodesep="0.8", ranksep="1.0"];',
        'node [fontname="Arial", fontsize="11"];',
        'edge [fontname="Arial", fontsize="9"];',
    ]

    for row in group_nodes.itertuples(index=False):
        role = (
            "Seed"
            if row.seed_flag
            else "Discovered"
        )

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


def build_counterparty_graphviz_dot(
    candidate: pd.Series,
    candidate_links: pd.DataFrame,
) -> str:
    """Build one unexpanded shared-counterparty candidate graph."""
    seed_node_id = "seed"
    counterparty_node_id = "counterparty"

    seed_label = _escape_dot(
        "\n".join(
            [
                candidate["seed_entity_type"],
                candidate["seed_entity_id"],
                "Seed mule",
            ]
        )
    )

    counterparty_name = (
        candidate["counterparty_names"]
        or candidate["counterparty_key"]
    )

    counterparty_label = _escape_dot(
        "\n".join(
            [
                str(counterparty_name),
                candidate["counterparty_key_type"],
                "Candidate only",
            ]
        )
    )

    lines = [
        "digraph candidate {",
        "rankdir=LR;",
        'graph [bgcolor="transparent", pad="0.2", '
        'nodesep="0.8", ranksep="1.0"];',
        'node [fontname="Arial", fontsize="11"];',
        'edge [fontname="Arial", fontsize="9"];',
        (
            f'"{seed_node_id}" '
            f'[label="{seed_label}", '
            'shape="doublecircle", penwidth="2"];'
        ),
        (
            f'"{counterparty_node_id}" '
            f'[label="{counterparty_label}", '
            'shape="ellipse", style="dashed"];'
        ),
        (
            f'"{seed_node_id}" -> '
            f'"{counterparty_node_id}" '
            '[label="Seed transfer evidence"];'
        ),
    ]

    for index, row in enumerate(
        candidate_links.itertuples(index=False),
        start=1,
    ):
        candidate_node_id = f"candidate_{index}"

        candidate_label = _escape_dot(
            "\n".join(
                [
                    row.candidate_entity_type,
                    row.candidate_entity_id,
                    "Candidate customer",
                ]
            )
        )

        event_label = _escape_dot(
            row.candidate_event_types
        )

        lines.append(
            f'"{candidate_node_id}" '
            f'[label="{candidate_label}", '
            'shape="box", style="dashed"];'
        )

        lines.append(
            f'"{counterparty_node_id}" -> '
            f'"{candidate_node_id}" '
            f'[label="{event_label}"];'
        )

    lines.append("}")

    return "\n".join(lines)


def build_beneficiary_graphviz_dot(
    beneficiary_link: pd.Series,
) -> str:
    """Build one beneficiary-to-seed candidate graph."""
    seed_label = _escape_dot(
        "\n".join(
            [
                beneficiary_link[
                    "seed_entity_type"
                ],
                beneficiary_link[
                    "seed_entity_id"
                ],
                "Seed mule",
            ]
        )
    )

    candidate_label = _escape_dot(
        "\n".join(
            [
                beneficiary_link[
                    "candidate_entity_type"
                ],
                beneficiary_link[
                    "candidate_entity_id"
                ],
                "Candidate customer",
            ]
        )
    )

    return "\n".join(
        [
            "digraph beneficiary {",
            "rankdir=LR;",
            'graph [bgcolor="transparent", pad="0.2", '
            'nodesep="0.8", ranksep="1.0"];',
            'node [fontname="Arial", fontsize="11"];',
            'edge [fontname="Arial", fontsize="9"];',
            (
                '"candidate" '
                f'[label="{candidate_label}", '
                'shape="box", style="dashed"];'
            ),
            (
                '"seed" '
                f'[label="{seed_label}", '
                'shape="doublecircle", penwidth="2"];'
            ),
            (
                '"candidate" -> "seed" '
                '[label="Added seed account '
                'as beneficiary", style="dashed"];'
            ),
            "}",
        ]
    )


def render_eid_groups() -> None:
    """Render Section 1 EID groups."""
    try:
        groups, nodes, edges = load_eid_outputs()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.code(
            "python scripts/run_eid_demo.py",
            language="bash",
        )
        return

    if groups.empty:
        st.warning("No EID groups are available.")
        return

    summary_columns = st.columns(4)

    summary_columns[0].metric(
        "Run date",
        sorted(
            groups["run_date"].drop_duplicates()
        )[-1],
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

    st.subheader("Discovered EID groups")
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
        key="eid_group_selection",
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

    selected_group_id = selected_group[
        "group_id"
    ]

    selected_nodes = (
        nodes.loc[
            nodes["group_id"]
            == selected_group_id
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
            edges["group_id"]
            == selected_group_id
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
    st.subheader(
        f"Selected group: {selected_group_id}"
    )

    group_summary_columns = st.columns(5)

    group_summary_columns[0].metric(
        "Type",
        selected_group["group_type"],
    )

    group_summary_columns[1].metric(
        "Seeds",
        int(
            selected_group[
                "seed_entity_count"
            ]
        ),
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
        int(
            selected_group[
                "total_entity_count"
            ]
        ),
    )

    group_summary_columns[4].metric(
        "EID links",
        int(selected_group["edge_count"]),
    )

    st.caption(
        "Seed entities use a double circle. "
        "Discovered entities use a box."
    )

    st.graphviz_chart(
        build_eid_graphviz_dot(
            group_nodes=selected_nodes,
            group_edges=selected_edges,
        ),
        width="stretch",
    )

    with st.expander(
        "Group entities",
        expanded=False,
    ):
        st.dataframe(
            selected_nodes[
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
            ],
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "EID edges",
        expanded=False,
    ):
        st.dataframe(
            selected_edges[
                [
                    "source_entity_key",
                    "target_entity_key",
                    "emirates_id_number",
                    "seed_individual_ids",
                    "candidate_individual_ids",
                    "reason_code",
                ]
            ],
            width="stretch",
            hide_index=True,
        )


def render_counterparty_candidates() -> None:
    """Render Section 2 unexpanded candidate relationships."""
    try:
        (
            candidates,
            links,
            beneficiary_links,
        ) = load_counterparty_outputs()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.code(
            "python scripts/run_counterparty_demo.py",
            language="bash",
        )
        return

    summary_columns = st.columns(4)

    summary_columns[0].metric(
        "Counterparties",
        len(candidates),
    )

    summary_columns[1].metric(
        "Shared-customer links",
        len(links),
    )

    summary_columns[2].metric(
        "Beneficiary links",
        len(beneficiary_links),
    )

    summary_columns[3].metric(
        "Approved expansions",
        int(
            candidates[
                "expansion_allowed_flag"
            ].sum()
        ),
    )

    st.warning(
        "All Section 2 counterparty relationships are "
        "candidate evidence only. No branch is approved "
        "for expansion."
    )

    st.subheader("Shared external counterparties")

    if candidates.empty:
        st.info(
            "No shared-counterparty candidates "
            "are available."
        )
    else:
        candidates_sorted = (
            candidates
            .sort_values(
                by=[
                    "candidate_customer_count",
                    "counterparty_candidate_id",
                ],
                ascending=[False, True],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        candidate_table = candidates_sorted[
            [
                "counterparty_candidate_id",
                "seed_entity_key",
                "counterparty_names",
                "counterparty_key_type",
                "counterparty_key_quality",
                "candidate_customer_count",
                "seed_event_count",
                "candidate_event_count",
                "candidate_status",
            ]
        ].rename(
            columns={
                "counterparty_candidate_id": (
                    "Candidate ID"
                ),
                "seed_entity_key": "Seed entity",
                "counterparty_names": (
                    "Counterparty"
                ),
                "counterparty_key_type": (
                    "Key type"
                ),
                "counterparty_key_quality": (
                    "Key quality"
                ),
                "candidate_customer_count": (
                    "Linked customers"
                ),
                "seed_event_count": (
                    "Seed events"
                ),
                "candidate_event_count": (
                    "Candidate events"
                ),
                "candidate_status": "Status",
            }
        )

        selection_event = st.dataframe(
            candidate_table,
            width="stretch",
            hide_index=True,
            key="counterparty_selection",
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

        selected_candidate = (
            candidates_sorted.iloc[
                selected_position
            ]
        )

        selected_links = (
            links.loc[
                (
                    links["seed_entity_key"]
                    == selected_candidate[
                        "seed_entity_key"
                    ]
                )
                & (
                    links["counterparty_key"]
                    == selected_candidate[
                        "counterparty_key"
                    ]
                )
            ]
            .sort_values(
                by="candidate_entity_key",
                kind="stable",
            )
            .reset_index(drop=True)
        )

        st.divider()
        st.subheader(
            "Selected counterparty candidate"
        )

        detail_columns = st.columns(4)

        detail_columns[0].metric(
            "Seed",
            selected_candidate[
                "seed_entity_key"
            ],
        )

        detail_columns[1].metric(
            "Linked customers",
            int(
                selected_candidate[
                    "candidate_customer_count"
                ]
            ),
        )

        detail_columns[2].metric(
            "Key quality",
            selected_candidate[
                "counterparty_key_quality"
            ],
        )

        detail_columns[3].metric(
            "Expansion allowed",
            "No",
        )

        st.graphviz_chart(
            build_counterparty_graphviz_dot(
                candidate=selected_candidate,
                candidate_links=selected_links,
            ),
            width="stretch",
        )

        with st.expander(
            "Candidate relationship evidence",
            expanded=False,
        ):
            st.dataframe(
                selected_links[
                    [
                        "relationship_id",
                        "seed_entity_key",
                        "candidate_entity_key",
                        "counterparty_key",
                        "seed_event_types",
                        "candidate_event_types",
                        "seed_event_count",
                        "candidate_event_count",
                        "seed_first_event_timestamp",
                        "seed_last_event_timestamp",
                        "candidate_first_event_timestamp",
                        "candidate_last_event_timestamp",
                        "candidate_status",
                    ]
                ],
                width="stretch",
                hide_index=True,
            )

    st.divider()
    st.subheader(
        "Customers that added a seed account "
        "as beneficiary"
    )

    if beneficiary_links.empty:
        st.info(
            "No beneficiary-to-seed links "
            "are available."
        )
        return

    beneficiary_sorted = (
        beneficiary_links
        .sort_values(
            by=[
                "seed_entity_key",
                "candidate_entity_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    beneficiary_table = beneficiary_sorted[
        [
            "relationship_id",
            "candidate_entity_key",
            "seed_entity_key",
            "beneficiary_id",
            "beneficiary_added_timestamp",
            "beneficiary_status",
            "candidate_status",
        ]
    ].rename(
        columns={
            "relationship_id": "Relationship ID",
            "candidate_entity_key": (
                "Customer"
            ),
            "seed_entity_key": "Seed mule",
            "beneficiary_id": "Beneficiary ID",
            "beneficiary_added_timestamp": (
                "Beneficiary added"
            ),
            "beneficiary_status": (
                "Beneficiary status"
            ),
            "candidate_status": "Status",
        }
    )

    beneficiary_selection = st.dataframe(
        beneficiary_table,
        width="stretch",
        hide_index=True,
        key="beneficiary_selection",
        on_select="rerun",
        selection_mode="single-row-required",
    )

    selected_rows = list(
        beneficiary_selection.selection.rows
    )

    selected_position = (
        selected_rows[0]
        if selected_rows
        else 0
    )

    selected_beneficiary = (
        beneficiary_sorted.iloc[
            selected_position
        ]
    )

    beneficiary_summary = st.columns(3)

    beneficiary_summary[0].metric(
        "Customer",
        selected_beneficiary[
            "candidate_entity_key"
        ],
    )

    beneficiary_summary[1].metric(
        "Seed mule",
        selected_beneficiary[
            "seed_entity_key"
        ],
    )

    beneficiary_summary[2].metric(
        "Expansion allowed",
        "No",
    )

    st.graphviz_chart(
        build_beneficiary_graphviz_dot(
            selected_beneficiary
        ),
        width="stretch",
    )


def main() -> None:
    """Render the discovery interface."""
    st.title("Mule Network Discovery")
    st.caption(
        "Read-only visualization of deterministic "
        "EID groups and unexpanded counterparty "
        "candidate relationships."
    )

    if st.sidebar.button(
        "Reload output files",
        type="secondary",
    ):
        st.cache_data.clear()
        st.rerun()

    eid_tab, counterparty_tab = st.tabs(
        [
            "EID groups",
            "Counterparty candidates",
        ]
    )

    with eid_tab:
        render_eid_groups()

    with counterparty_tab:
        render_counterparty_candidates()


if __name__ == "__main__":
    main()
