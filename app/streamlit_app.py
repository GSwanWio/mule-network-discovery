"""Decision-aware unified mule network interface."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"

GROUPS_PATH = OUTPUT_DIRECTORY / "decision_groups.csv"
NODES_PATH = OUTPUT_DIRECTORY / "decision_group_nodes.csv"
EDGES_PATH = OUTPUT_DIRECTORY / "decision_group_edges.csv"
QUEUE_PATH = OUTPUT_DIRECTORY / "expansion_queue.csv"
APPLIED_DECISIONS_PATH = (
    OUTPUT_DIRECTORY / "applied_decisions.csv"
)
IGNORED_DECISIONS_PATH = (
    OUTPUT_DIRECTORY / "ignored_decisions.csv"
)


st.set_page_config(
    page_title="Mule Network Discovery",
    page_icon=None,
    layout="wide",
)


def _read_csv(path: Path) -> pd.DataFrame:
    """Read one required decision output."""
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
def load_decision_outputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Load decision-aware group and queue outputs."""
    groups = _read_csv(GROUPS_PATH)
    nodes = _read_csv(NODES_PATH)
    edges = _read_csv(EDGES_PATH)
    queue = _read_csv(QUEUE_PATH)
    applied_decisions = _read_csv(
        APPLIED_DECISIONS_PATH
    )
    ignored_decisions = _read_csv(
        IGNORED_DECISIONS_PATH
    )

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
        "approved_suspicious_counterparty_count",
        "suppressed_counterparty_count",
        "mule_like_customer_count",
        "queued_action_count",
    ]

    for column in numeric_group_columns:
        groups[column] = pd.to_numeric(
            groups[column],
            errors="raise",
        ).astype(int)

    for column in [
        "customer_discovery_allowed_flag",
        "expansion_source_flag",
        "decision_reuse_flag",
    ]:
        nodes[column] = _parse_boolean(
            nodes[column]
        )

    for column in [
        "customer_discovery_allowed_flag",
        "recursive_expansion_allowed_flag",
    ]:
        edges[column] = _parse_boolean(
            edges[column]
        )

    if "priority" in queue.columns:
        queue["priority"] = pd.to_numeric(
            queue["priority"],
            errors="raise",
        ).astype(int)

    return (
        groups,
        nodes,
        edges,
        queue,
        applied_decisions,
        ignored_decisions,
    )


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


def _build_node_statement(row: object) -> str:
    """Build one decision-aware Graphviz node."""
    node_id = _escape_dot(row.node_id)

    if row.node_type == "COUNTERPARTY":
        label_lines = [
            row.display_label,
            "External counterparty",
            _humanize(row.node_status),
        ]

        shape = "ellipse"

        if (
            row.node_status
            == "COUNTERPARTY_APPROVED_SUSPICIOUS"
        ):
            style = "solid"
            penwidth = "2"
        elif str(row.node_status).startswith(
            "COUNTERPARTY_SUPPRESSED"
        ):
            style = "dotted"
            penwidth = "1"
        else:
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

        if (
            row.customer_assessment_status
            == "SEED_CONFIRMED"
        ):
            shape = "doublecircle"
            style = "solid"
            penwidth = "2"

        elif (
            row.customer_assessment_status
            == "MULE_LIKE"
        ):
            shape = "doubleoctagon"
            style = "solid"
            penwidth = "2"

        elif (
            row.customer_assessment_status
            == "BLOCKED_PENDING_COUNTERPARTY_AI"
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
    row: object,
) -> tuple[str, str, str]:
    """Return the edge label, style, and direction."""
    labels = {
        "SAME_EMIRATES_ID": "Same Emirates ID",
        "SEED_COUNTERPARTY_EVIDENCE": (
            "Seed transfer evidence"
        ),
        "SHARED_EXTERNAL_COUNTERPARTY": (
            "Shared counterparty"
        ),
        "BENEFICIARY_ADDED_SEED_ACCOUNT": (
            "Added seed as beneficiary"
        ),
    }

    label = labels.get(
        row.edge_type,
        _humanize(row.edge_type),
    )

    if row.edge_type == "SAME_EMIRATES_ID":
        return label, "solid", "none"

    if (
        row.relationship_status
        == "COUNTERPARTY_APPROVED_SUSPICIOUS"
    ):
        return label, "solid", "forward"

    if str(row.relationship_status).startswith(
        "COUNTERPARTY_SUPPRESSED"
    ):
        return label, "dotted", "forward"

    if (
        row.edge_type
        == "BENEFICIARY_ADDED_SEED_ACCOUNT"
    ):
        return label, "solid", "forward"

    return label, "dashed", "forward"


def build_group_graphviz_dot(
    group_nodes: pd.DataFrame,
    group_edges: pd.DataFrame,
) -> str:
    """Build one unified decision-aware graph."""
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
        (
            edge_label,
            edge_style,
            edge_direction,
        ) = _edge_display_properties(row)

        source_node_id = _escape_dot(
            row.source_node_id
        )

        target_node_id = _escape_dot(
            row.target_node_id
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
    """Render decision-aware group metrics."""
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

    second_row = st.columns(6)

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
        "Approved suspicious",
        int(
            selected_group[
                "approved_suspicious_counterparty_count"
            ]
        ),
    )

    second_row[3].metric(
        "Suppressed counterparties",
        int(
            selected_group[
                "suppressed_counterparty_count"
            ]
        ),
    )

    second_row[4].metric(
        "Mule-like customers",
        int(
            selected_group[
                "mule_like_customer_count"
            ]
        ),
    )

    second_row[5].metric(
        "Queued actions",
        int(
            selected_group[
                "queued_action_count"
            ]
        ),
    )


def _filter_queue_for_group(
    queue: pd.DataFrame,
    group_id: str,
) -> pd.DataFrame:
    """Return queue items associated with one group."""
    if queue.empty:
        return queue.copy()

    return (
        queue.loc[
            queue["group_ids"].map(
                lambda value: group_id
                in str(value).split("|")
            )
        ]
        .sort_values(
            by=[
                "priority",
                "action_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _filter_decisions_for_group(
    decisions: pd.DataFrame,
    selected_nodes: pd.DataFrame,
) -> pd.DataFrame:
    """Return decisions for subjects visible in a group."""
    customer_keys = set(
        selected_nodes.loc[
            selected_nodes["node_type"]
            == "CUSTOMER",
            "entity_key",
        ]
    )

    counterparty_keys = set(
        selected_nodes.loc[
            selected_nodes["node_type"]
            == "COUNTERPARTY",
            "counterparty_key",
        ]
    )

    return (
        decisions.loc[
            (
                decisions["subject_type"]
                .eq("CUSTOMER")
                & decisions["subject_key"].isin(
                    customer_keys
                )
            )
            | (
                decisions["subject_type"]
                .eq("COUNTERPARTY")
                & decisions["subject_key"].isin(
                    counterparty_keys
                )
            )
        ]
        .sort_values(
            by=[
                "subject_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def render_group_tables(
    selected_nodes: pd.DataFrame,
    selected_edges: pd.DataFrame,
    selected_queue: pd.DataFrame,
    selected_applied_decisions: pd.DataFrame,
    selected_ignored_decisions: pd.DataFrame,
) -> None:
    """Render detailed graph, decision, and queue evidence."""
    customer_nodes = selected_nodes.loc[
        selected_nodes["node_type"]
        == "CUSTOMER"
    ].copy()

    counterparty_nodes = selected_nodes.loc[
        selected_nodes["node_type"]
        == "COUNTERPARTY"
    ].copy()

    st.subheader("Remaining work queue")

    if selected_queue.empty:
        st.info(
            "No unresolved actions remain for this group."
        )
    else:
        st.dataframe(
            selected_queue[
                [
                    "action_type",
                    "subject_type",
                    "subject_key",
                    "queue_reason",
                    "priority",
                    "queue_status",
                    "trigger_decision_id",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

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
                    "expansion_source_flag",
                    "applied_decision",
                    "decision_reason_code",
                    "decision_reuse_flag",
                    "feature_snapshot_hash",
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
                    "node_status",
                    "customer_discovery_allowed_flag",
                    "applied_decision",
                    "decision_reason_code",
                    "decision_reuse_flag",
                    "feature_snapshot_hash",
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
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "Reused decisions",
        expanded=False,
    ):
        st.dataframe(
            selected_applied_decisions,
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "Ignored stale decisions",
        expanded=False,
    ):
        st.dataframe(
            selected_ignored_decisions,
            width="stretch",
            hide_index=True,
        )


def main() -> None:
    """Render the decision-aware discovery interface."""
    st.title("Mule Network Discovery")

    st.caption(
        "One seed-led network combining deterministic "
        "links, counterparty decisions, customer "
        "assessments, and incremental expansion work."
    )

    if st.sidebar.button(
        "Reload output files",
        type="secondary",
    ):
        st.cache_data.clear()
        st.rerun()

    try:
        (
            groups,
            nodes,
            edges,
            queue,
            applied_decisions,
            ignored_decisions,
        ) = load_decision_outputs()

    except FileNotFoundError as exc:
        st.error(str(exc))
        st.code(
            "python scripts/run_decision_demo.py",
            language="bash",
        )
        return

    if groups.empty:
        st.warning(
            "No decision-aware groups are available."
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
            "mule_like_customer_count",
            "approved_suspicious_counterparty_count",
            "suppressed_counterparty_count",
            "customer_assessment_pending_count",
            "counterparty_ai_pending_count",
            "queued_action_count",
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
            "mule_like_customer_count": (
                "Mule-like customers"
            ),
            "approved_suspicious_counterparty_count": (
                "Approved suspicious"
            ),
            "suppressed_counterparty_count": (
                "Suppressed"
            ),
            "customer_assessment_pending_count": (
                "Customer AI pending"
            ),
            "counterparty_ai_pending_count": (
                "Counterparty AI pending"
            ),
            "queued_action_count": (
                "Queued actions"
            ),
        }
    )

    st.dataframe(
        group_table,
        width="stretch",
        hide_index=True,
    )

    group_label_map = {
        row.group_id: (
            f"{row.group_id} — "
            f"{row.group_anchor_seed_entity_key}"
        )
        for row in groups_sorted.itertuples(
            index=False
        )
    }

    selected_group_id = st.selectbox(
        "Select a group",
        options=list(group_label_map),
        format_func=lambda group_id: (
            group_label_map[group_id]
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

    selected_queue = _filter_queue_for_group(
        queue=queue,
        group_id=selected_group_id,
    )

    selected_applied_decisions = (
        _filter_decisions_for_group(
            decisions=applied_decisions,
            selected_nodes=selected_nodes,
        )
    )

    selected_ignored_decisions = (
        _filter_decisions_for_group(
            decisions=ignored_decisions,
            selected_nodes=selected_nodes,
        )
    )

    st.divider()
    st.subheader(
        f"Decision-aware network: "
        f"{selected_group_id}"
    )

    render_group_summary(selected_group)

    st.warning(
        "The graph reuses AI decisions only when the "
        "stored feature hash matches the current evidence. "
        "New or materially changed subjects remain queued."
    )

    st.caption(
        "Double circle: confirmed seed. "
        "Double octagon: mule-like expansion source. "
        "Solid ellipse: suspicious counterparty approved. "
        "Dotted ellipse or edge: suppressed. "
        "Dashed branch: pending AI."
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
        selected_queue=selected_queue,
        selected_applied_decisions=(
            selected_applied_decisions
        ),
        selected_ignored_decisions=(
            selected_ignored_decisions
        ),
    )


if __name__ == "__main__":
    main()
