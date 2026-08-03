"""Read-only persisted-state analyst interface."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
    AnalystApplicationStateStore,
)
from network_mule_discovery.analyst_group_evidence import (
    AnalystGroupEvidenceStore,
)
from network_mule_discovery.analyst_group_network import (
    AnalystGroupNetworkStore,
)
from network_mule_discovery.analyst_network_projection import (
    build_analyst_network_display_projection,
)
from network_mule_discovery.consolidated_state import (
    ConsolidatedStateError,
)


STATE_DIRECTORY_ENV_VAR = (
    "MULE_NETWORK_STATE_DIRECTORY"
)
DEFAULT_STATE_DIRECTORY = (
    PROJECT_ROOT / "data/state"
)


st.set_page_config(
    page_title="Mule Network Discovery",
    page_icon=None,
    layout="wide",
)


def resolve_state_directory() -> Path:
    """Resolve the persisted-state directory."""
    configured = os.getenv(
        STATE_DIRECTORY_ENV_VAR,
        "",
    ).strip()

    if not configured:
        return DEFAULT_STATE_DIRECTORY

    path = Path(configured).expanduser()

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path


def _clean_text(value: object) -> str:
    """Return normalized display text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _humanize(value: object) -> str:
    """Convert an internal value into display text."""
    return (
        _clean_text(value)
        .replace("_", " ")
        .title()
    )


def _escape_dot(value: object) -> str:
    """Escape text used in a Graphviz statement."""
    return (
        _clean_text(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _build_node_statement(row: object) -> str:
    """Build one analyst Graphviz node."""
    node_id = _escape_dot(row.node_id)
    node_type = _clean_text(
        row.node_type
    ).upper()
    node_status = _clean_text(
        row.node_status
    )
    assessment_status = _clean_text(
        row.customer_assessment_status
    )

    display_label = (
        _clean_text(row.display_label)
        or _clean_text(row.entity_key)
        or _clean_text(row.counterparty_key)
        or _clean_text(row.node_key)
    )

    if node_type == "CUSTOMER":
        label_lines = [
            display_label,
            "Customer",
            _humanize(assessment_status),
        ]
        shape = "box"
        style = "solid"
        penwidth = "1"

        if assessment_status == "SEED_CONFIRMED":
            shape = "doublecircle"
            penwidth = "2"

        elif assessment_status == "MULE_LIKE":
            shape = "doubleoctagon"
            penwidth = "2"

        elif (
            assessment_status
            == "BLOCKED_PENDING_COUNTERPARTY_AI"
        ):
            style = "dashed"

    elif node_type == "COUNTERPARTY":
        label_lines = [
            display_label,
            "External counterparty",
            _humanize(node_status),
        ]

        collapsed_customer_count = int(
            pd.to_numeric(
                pd.Series(
                    [
                        getattr(
                            row,
                            "collapsed_customer_count",
                            0,
                        )
                    ]
                ),
                errors="coerce",
            )
            .fillna(0)
            .iloc[0]
        )

        if collapsed_customer_count > 0:
            label_lines.append(
                f"{collapsed_customer_count} linked "
                "customers collapsed"
            )

        shape = "ellipse"
        style = "dashed"
        penwidth = "1"

        if (
            node_status
            == "COUNTERPARTY_APPROVED_SUSPICIOUS"
        ):
            style = "solid"
            penwidth = "2"

        elif node_status.startswith(
            "COUNTERPARTY_SUPPRESSED"
        ):
            style = "dotted"

    else:
        label_lines = [
            display_label,
            _humanize(node_type),
            _humanize(node_status),
        ]
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


def _edge_properties(
    row: object,
) -> tuple[str, str, str]:
    """Return edge label, style, and direction."""
    edge_type = _clean_text(row.edge_type)
    relationship_status = _clean_text(
        row.relationship_status
    )

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
        edge_type,
        _humanize(edge_type),
    )

    if edge_type == "SAME_EMIRATES_ID":
        return label, "solid", "none"

    if (
        relationship_status
        == "COUNTERPARTY_APPROVED_SUSPICIOUS"
    ):
        return label, "solid", "forward"

    if relationship_status.startswith(
        "COUNTERPARTY_SUPPRESSED"
    ):
        return label, "dotted", "forward"

    if (
        edge_type
        == "BENEFICIARY_ADDED_SEED_ACCOUNT"
    ):
        return label, "solid", "forward"

    return label, "dashed", "forward"


def build_group_graphviz_dot(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
) -> str:
    """Build one selected persisted network graph."""
    lines = [
        "digraph network {",
        "rankdir=LR;",
        'graph [bgcolor="transparent", '
        'pad="0.2", nodesep="0.8", '
        'ranksep="1.0"];',
        'node [fontname="Arial", fontsize="10"];',
        'edge [fontname="Arial", fontsize="9"];',
    ]

    for row in nodes.itertuples(index=False):
        lines.append(
            _build_node_statement(row)
        )

    for row in edges.itertuples(index=False):
        label, style, direction = (
            _edge_properties(row)
        )

        lines.append(
            f'"{_escape_dot(row.source_node_id)}" '
            f'-> '
            f'"{_escape_dot(row.target_node_id)}" '
            f'[label="{_escape_dot(label)}", '
            f'style="{style}", '
            f'dir="{direction}"];'
        )

    lines.append("}")

    return "\n".join(lines)


def render_run_summary(summary: object) -> None:
    """Render selected-run metrics."""
    columns = st.columns(6)

    columns[0].metric(
        "Run status",
        _humanize(summary.run_status),
    )
    columns[1].metric(
        "Run date",
        summary.run_date,
    )
    columns[2].metric(
        "Dataset",
        summary.dataset_id,
    )
    columns[3].metric(
        "Provider",
        summary.provider_name,
    )
    columns[4].metric(
        "Artifacts",
        (
            f"{summary.artifact_count}/"
            f"{summary.artifact_count + summary.missing_artifact_count}"
        ),
    )
    columns[5].metric(
        "Missing artifacts",
        summary.missing_artifact_count,
    )


def render_group_summary(summary: object) -> None:
    """Render selected-group metrics."""
    first = st.columns(4)
    first[0].metric(
        "Customers",
        summary.customer_count,
    )
    first[1].metric(
        "Counterparties",
        summary.counterparty_count,
    )
    first[2].metric(
        "Nodes",
        summary.total_node_count,
    )
    first[3].metric(
        "Edges",
        summary.total_edge_count,
    )

    second = st.columns(4)
    second[0].metric(
        "EID links",
        summary.eid_link_count,
    )
    second[1].metric(
        "Shared-counterparty links",
        summary.shared_counterparty_customer_count,
    )
    second[2].metric(
        "Beneficiary links",
        summary.beneficiary_seed_link_count,
    )
    second[3].metric(
        "Mule-like customers",
        summary.mule_like_customer_count,
    )

    third = st.columns(4)
    third[0].metric(
        "Approved suspicious",
        summary.approved_suspicious_counterparty_count,
    )
    third[1].metric(
        "Suppressed",
        summary.suppressed_counterparty_count,
    )
    third[2].metric(
        "Ready actions",
        summary.ready_action_count,
    )
    third[3].metric(
        "Failed closed",
        summary.failed_closed_action_count,
    )


def _display_frame(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    """Return available display columns only."""
    available = [
        column
        for column in columns
        if column in frame.columns
    ]

    return frame.loc[:, available].copy()


def main() -> None:
    """Render the persisted analyst interface."""
    st.title("Mule Network Discovery")

    st.caption(
        "Read-only analyst access to persisted runs, "
        "network groups, relationship evidence, AI "
        "decisions, and execution audit."
    )

    state_directory = resolve_state_directory()

    st.sidebar.caption(
        "Persisted state directory"
    )
    st.sidebar.code(str(state_directory))

    if st.sidebar.button(
        "Reload persisted state",
        type="secondary",
    ):
        st.rerun()

    try:
        application = (
            AnalystApplicationStateStore(
                state_directory
            )
        )
        run_summaries = application.list_runs()

    except Exception as exc:
        st.error(
            "Persisted run catalogue could not be loaded: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    if not run_summaries:
        st.warning(
            "No persisted network runs are available."
        )
        st.code(
            "export "
            f"{STATE_DIRECTORY_ENV_VAR}=/path/to/state",
            language="bash",
        )
        return

    run_labels = {
        summary.run_id: (
            f"{summary.run_date} — "
            f"{summary.dataset_id} — "
            f"{_humanize(summary.run_status)}"
        )
        for summary in run_summaries
    }

    selected_run_id = st.sidebar.selectbox(
        "Select run",
        options=list(run_labels),
        format_func=lambda run_id: (
            run_labels[run_id]
        ),
    )

    selected_run = next(
        summary
        for summary in run_summaries
        if summary.run_id == selected_run_id
    )

    st.subheader("Persisted runs")
    st.dataframe(
        application.run_table(),
        width="stretch",
        hide_index=True,
    )

    st.subheader("Selected run")
    render_run_summary(selected_run)

    if selected_run.termination_reason:
        st.caption(
            "Termination: "
            f"{_humanize(selected_run.termination_reason)}"
        )

    try:
        groups = application.group_table(
            selected_run_id
        )

    except (
        AnalystApplicationStateError,
        ConsolidatedStateError,
        FileNotFoundError,
        ValueError,
    ) as exc:
        st.error(
            "Selected run groups could not be loaded: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    if groups.empty:
        st.warning(
            "The selected run contains no network groups."
        )
        return

    groups = (
        groups.sort_values(
            by=[
                "total_node_count",
                "group_id",
            ],
            ascending=[False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    st.subheader("Network groups")
    st.dataframe(
        _display_frame(
            groups,
            (
                "group_id",
                "group_anchor_seed_entity_key",
                "group_status",
                "customer_count",
                "counterparty_count",
                "mule_like_customer_count",
                "approved_suspicious_counterparty_count",
                "suppressed_counterparty_count",
                "customer_assessment_pending_count",
                "counterparty_ai_pending_count",
                "ready_action_count",
                "failed_closed_action_count",
                "total_node_count",
                "total_edge_count",
            ),
        ),
        width="stretch",
        hide_index=True,
    )

    group_labels = {
        row.group_id: (
            f"{row.group_id} — "
            f"{row.group_anchor_seed_entity_key}"
        )
        for row in groups.itertuples(
            index=False
        )
    }

    selected_group_id = st.sidebar.selectbox(
        "Select group",
        options=list(group_labels),
        format_func=lambda group_id: (
            group_labels[group_id]
        ),
    )

    try:
        network = AnalystGroupNetworkStore(
            state_directory
        ).load(
            run_id=selected_run_id,
            group_id=selected_group_id,
        )
        evidence = AnalystGroupEvidenceStore(
            state_directory
        ).load(
            run_id=selected_run_id,
            group_id=selected_group_id,
        )
        display_projection = (
            build_analyst_network_display_projection(
                nodes=network.nodes,
                edges=network.edges,
            )
        )

    except (
        AnalystApplicationStateError,
        ConsolidatedStateError,
        FileNotFoundError,
        ValueError,
    ) as exc:
        st.error(
            "Selected group could not be loaded: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    st.divider()
    st.subheader(
        f"Network group: {selected_group_id}"
    )

    render_group_summary(network.summary)

    st.warning(
        "AI decisions are reused only when the stored "
        "feature hash matches the persisted evidence. "
        "New or materially changed subjects remain queued."
    )

    (
        network_tab,
        relationships_tab,
        decisions_tab,
        ai_audit_tab,
        work_tab,
    ) = st.tabs(
        [
            "Network",
            "Relationship evidence",
            "AI decisions",
            "AI call audit",
            "Expansion work",
        ]
    )

    with network_tab:
        st.caption(
            "Double circle: confirmed seed. "
            "Double octagon: mule-like customer. "
            "Solid ellipse: approved suspicious "
            "counterparty. Dotted: suppressed. "
            "Dashed: pending."
        )

        if display_projection.hidden_node_count > 0:
            st.info(
                f"{display_projection.hidden_node_count} "
                "non-actionable customer nodes are "
                "collapsed because they connect only "
                "through suppressed common/public "
                "counterparties. Raw relationships remain "
                "available in Relationship evidence."
            )

            st.dataframe(
                _display_frame(
                    display_projection
                    .collapsed_counterparties,
                    (
                        "display_label",
                        "node_status",
                        "observed_linked_customer_count",
                        "collapsed_customer_count",
                        "visible_linked_customer_count",
                    ),
                ),
                width="stretch",
                hide_index=True,
            )

        show_full_graph = st.checkbox(
            "Show full audit graph",
            value=False,
            help=(
                "Displays every persisted node and edge, "
                "including customers suppressed from the "
                "default analyst view."
            ),
        )

        if show_full_graph:
            graph_nodes = network.nodes
            graph_edges = network.edges
        else:
            graph_nodes = display_projection.nodes
            graph_edges = display_projection.edges

        st.graphviz_chart(
            build_group_graphviz_dot(
                nodes=graph_nodes,
                edges=graph_edges,
            ),
            width="stretch",
        )

        with st.expander(
            "Displayed network nodes",
            expanded=False,
        ):
            st.dataframe(
                _display_frame(
                    graph_nodes,
                    (
                        "node_id",
                        "node_type",
                        "display_label",
                        "entity_key",
                        "counterparty_key",
                        "collapsed_customer_count",
                        "node_roles",
                        "node_status",
                        "customer_assessment_status",
                        "customer_discovery_allowed_flag",
                        "expansion_source_flag",
                    ),
                ),
                width="stretch",
                hide_index=True,
            )

    with relationships_tab:
        st.dataframe(
            evidence.relationship_evidence,
            width="stretch",
            hide_index=True,
        )

    with decisions_tab:
        if evidence.decision_evidence.empty:
            st.info(
                "No persisted AI decisions exist "
                "for this group."
            )
        else:
            st.dataframe(
                evidence.decision_evidence,
                width="stretch",
                hide_index=True,
            )

    with ai_audit_tab:
        if evidence.ai_call_evidence.empty:
            st.info(
                "No AI-call audit rows exist "
                "for this group."
            )
        else:
            st.dataframe(
                evidence.ai_call_evidence,
                width="stretch",
                hide_index=True,
            )

    with work_tab:
        st.markdown("#### Frontier queue")

        if network.frontier_queue.empty:
            st.info(
                "No persisted frontier work exists "
                "for this group."
            )
        else:
            st.dataframe(
                network.frontier_queue,
                width="stretch",
                hide_index=True,
            )

        st.markdown("#### Expansion history")

        if network.expansion_ledger.empty:
            st.info(
                "No persisted expansion history exists "
                "for this group."
            )
        else:
            st.dataframe(
                network.expansion_ledger,
                width="stretch",
                hide_index=True,
            )


if __name__ == "__main__":
    main()
