"""Analyst-first AI network investigation interface."""

from __future__ import annotations

import html
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_cytoscape import streamlit_cytoscape


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
    AnalystApplicationStateStore,
)
from network_mule_discovery.analyst_feedback import (
    AnalystFeedbackError,
    CsvAnalystFeedbackStore,
)
from network_mule_discovery.analyst_group_evidence import (
    AnalystGroupEvidenceStore,
)
from network_mule_discovery.analyst_group_network import (
    AnalystGroupNetworkStore,
)
from network_mule_discovery.analyst_investigation_graph import (
    NODE_SELECTED_EVENT,
    analyst_edge_styles,
    analyst_node_styles,
    build_analyst_investigation_graph,
    selected_investigation_node_id,
)
from network_mule_discovery.analyst_investigation_view import (
    build_analyst_investigation_view,
)
from network_mule_discovery.consolidated_state import (
    ConsolidatedStateError,
)


STATE_DIRECTORY_ENV_VAR = "MULE_NETWORK_STATE_DIRECTORY"
ANALYST_ID_ENV_VAR = "MULE_ANALYST_ID"
DEFAULT_STATE_DIRECTORY = PROJECT_ROOT / "data/state"


st.set_page_config(
    page_title="Mule Network Investigation",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _clean_text(value: object) -> str:
    """Return normalized display text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _humanize(value: object) -> str:
    """Convert an internal value into analyst text."""
    return (
        _clean_text(value)
        .replace("_", " ")
        .strip()
        .title()
    )


def _integer(value: object) -> int:
    """Return a safe integer value."""
    converted = pd.to_numeric(
        pd.Series([value]),
        errors="coerce",
    ).fillna(0)

    return int(converted.iloc[0])


def _confidence_label(value: object) -> str:
    """Return confidence as an analyst-friendly percentage."""
    raw_value = _clean_text(value)

    if not raw_value:
        return "Not available"

    try:
        numeric = float(raw_value)
    except ValueError:
        return raw_value

    if 0 <= numeric <= 1:
        return f"{numeric:.0%}"

    return f"{numeric:.1f}"


def resolve_state_directory() -> Path:
    """Resolve the persisted application state directory."""
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


def _inject_styles() -> None:
    """Apply the analyst application presentation."""
    st.markdown(
        """
        <style>
        .block-container {
            max-width: 1600px;
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }

        [data-testid="stSidebar"] {
            background: #F8FAFC;
            border-right: 1px solid #E2E8F0;
        }

        [data-testid="stMetric"] {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 0.8rem 1rem;
        }

        .mule-kicker {
            color: #64748B;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            margin-bottom: 0.3rem;
            text-transform: uppercase;
        }

        .mule-status {
            border-radius: 999px;
            display: inline-block;
            font-size: 0.78rem;
            font-weight: 700;
            padding: 0.32rem 0.75rem;
        }

        .mule-status-complete {
            background: #DCFCE7;
            color: #166534;
        }

        .mule-status-progress {
            background: #FEF3C7;
            color: #92400E;
        }

        .mule-status-attention {
            background: #FEE2E2;
            color: #991B1B;
        }

        .mule-decision {
            border-radius: 999px;
            display: inline-block;
            font-size: 0.8rem;
            font-weight: 700;
            margin-bottom: 0.7rem;
            padding: 0.32rem 0.75rem;
        }

        .mule-decision-SEED {
            background: #DBEAFE;
            color: #1E40AF;
        }

        .mule-decision-CONTINUE {
            background: #FFEDD5;
            color: #9A3412;
        }

        .mule-decision-STOP {
            background: #E2E8F0;
            color: #334155;
        }

        .mule-decision-PENDING {
            background: #FEF3C7;
            color: #92400E;
        }

        .mule-decision-FAILED {
            background: #FEE2E2;
            color: #991B1B;
        }

        .mule-legend {
            align-items: center;
            display: flex;
            flex-wrap: wrap;
            gap: 0.8rem;
            margin: 0.4rem 0 1rem 0;
        }

        .mule-legend-item {
            align-items: center;
            color: #475569;
            display: inline-flex;
            font-size: 0.82rem;
            gap: 0.35rem;
        }

        .mule-dot {
            border-radius: 50%;
            display: inline-block;
            height: 0.72rem;
            width: 0.72rem;
        }

        .mule-dot-seed {
            background: #2563EB;
        }

        .mule-dot-customer {
            background: #DC2626;
        }

        .mule-dot-counterparty {
            background: #F97316;
        }

        .mule-dot-stop {
            background: #64748B;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _investigation_catalogue(
    application: AnalystApplicationStateStore,
) -> list[dict[str, object]]:
    """Build one concise list of persisted investigations."""
    records: list[dict[str, object]] = []

    for run_summary in application.list_runs():
        groups = application.group_table(
            run_summary.run_id
        )

        if groups.empty:
            continue

        groups = (
            groups.sort_values(
                by=[
                    "mule_like_customer_count",
                    "approved_suspicious_counterparty_count",
                    "total_node_count",
                    "group_id",
                ],
                ascending=[
                    False,
                    False,
                    False,
                    True,
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        for row in groups.itertuples(index=False):
            group_id = _clean_text(row.group_id)
            anchor = (
                _clean_text(
                    row.group_anchor_seed_entity_key
                )
                or group_id
            )

            records.append(
                {
                    "key": (
                        f"{run_summary.run_id}"
                        f"::{group_id}"
                    ),
                    "run_id": run_summary.run_id,
                    "run_date": run_summary.run_date,
                    "run_status": (
                        run_summary.run_status
                    ),
                    "group_id": group_id,
                    "group_status": _clean_text(
                        row.group_status
                    ),
                    "anchor": anchor,
                    "label": (
                        f"{anchor}  ·  "
                        f"{run_summary.run_date}  ·  "
                        f"{_humanize(row.group_status)}"
                    ),
                }
            )

    return records


def _status_presentation(
    status: str,
) -> tuple[str, str]:
    """Return an investigation status label and CSS class."""
    normalized = _clean_text(status).upper()

    if normalized == "AI_REVIEW_COMPLETE":
        return (
            "AI review complete",
            "mule-status-complete",
        )

    if normalized == "DETERMINISTIC_REVIEW_COMPLETE":
        return (
            "Deterministic review complete",
            "mule-status-complete",
        )

    if normalized == "NEEDS_ATTENTION":
        return (
            "Needs attention",
            "mule-status-attention",
        )

    return (
        "AI review in progress",
        "mule-status-progress",
    )


def _render_legend() -> None:
    """Render the semantic graph legend."""
    st.markdown(
        """
        <div class="mule-legend">
            <span class="mule-legend-item">
                <span class="mule-dot mule-dot-seed"></span>
                Confirmed seed
            </span>
            <span class="mule-legend-item">
                <span class="mule-dot mule-dot-counterparty"></span>
                AI expanded counterparty
            </span>
            <span class="mule-legend-item">
                <span class="mule-dot mule-dot-customer"></span>
                AI expanded customer
            </span>
            <span class="mule-legend-item">
                <span class="mule-dot mule-dot-stop"></span>
                Expansion stopped
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _feedback_subject(
    node: pd.Series,
) -> tuple[str, str]:
    """Return the AI subject represented by one node."""
    subject_type = _clean_text(
        node.get("node_type")
    ).upper()

    if subject_type == "CUSTOMER":
        subject_key = _clean_text(
            node.get("entity_key")
        )
    elif subject_type == "COUNTERPARTY":
        subject_key = _clean_text(
            node.get("counterparty_key")
        )
    else:
        subject_key = ""

    if not subject_type or not subject_key:
        raise AnalystFeedbackError(
            "The selected node has no reviewable "
            "AI subject."
        )

    return subject_type, subject_key


def _render_analyst_feedback(
    *,
    node: pd.Series,
    feedback_store: CsvAnalystFeedbackStore,
    run_id: str,
    group_id: str,
) -> None:
    """Render analyst validation without overriding AI."""
    decision_category = _clean_text(
        node.get("decision_category")
    ).upper()
    ai_decision = _clean_text(
        node.get("ai_decision")
    ).upper()

    if (
        bool(node.get("is_seed"))
        or decision_category in {
            "PENDING",
            "FAILED",
        }
        or not ai_decision
        or ai_decision == "PENDING"
    ):
        return

    node_id = _clean_text(
        node.get("node_id")
    )
    subject_type, subject_key = (
        _feedback_subject(node)
    )

    st.divider()
    st.markdown("#### Analyst review")
    st.caption(
        "Confirm whether the AI assessment is correct. "
        "This records feedback and does not change the "
        "AI decision or network expansion."
    )

    latest = feedback_store.latest_for_node(
        run_id=run_id,
        group_id=group_id,
        node_id=node_id,
    )

    if latest is not None:
        if latest.feedback == "AI_CORRECT":
            st.success(
                "Latest analyst review: "
                "AI marked correct"
            )
        else:
            st.warning(
                "Latest analyst review: "
                "AI marked incorrect"
            )

        if latest.analyst_notes:
            st.caption(
                f"Previous note: "
                f"{latest.analyst_notes}"
            )

    form_key = (
        f"analyst-feedback::"
        f"{run_id}::{group_id}::{node_id}"
    )

    with st.form(
        form_key,
        clear_on_submit=True,
    ):
        selected_feedback = st.radio(
            "Was the AI decision correct?",
            options=(
                "AI correct",
                "AI incorrect",
            ),
            index=None,
            horizontal=True,
        )
        analyst_notes = st.text_area(
            "Review note",
            placeholder=(
                "Optional context explaining the review."
            ),
            height=90,
        )
        submitted = st.form_submit_button(
            "Submit review",
            use_container_width=True,
        )

    if not submitted:
        return

    if selected_feedback is None:
        st.warning(
            "Select whether the AI decision was "
            "correct or incorrect."
        )
        return

    feedback_value = (
        "AI_CORRECT"
        if selected_feedback == "AI correct"
        else "AI_INCORRECT"
    )

    try:
        feedback_store.submit(
            run_id=run_id,
            group_id=group_id,
            node_id=node_id,
            subject_type=subject_type,
            subject_key=subject_key,
            ai_decision=ai_decision,
            feedback=feedback_value,
            analyst_notes=analyst_notes,
            analyst_id=(
                os.getenv(
                    ANALYST_ID_ENV_VAR,
                    "",
                ).strip()
                or "UNSPECIFIED"
            ),
        )
    except AnalystFeedbackError as exc:
        st.error(
            "The analyst review could not be saved: "
            f"{exc}"
        )
        return

    st.success(
        "Review saved. The AI decision remains "
        "unchanged."
    )
    st.rerun()


def _render_selected_node(
    *,
    node: pd.Series,
    feedback_store: CsvAnalystFeedbackStore,
    run_id: str,
    group_id: str,
) -> None:
    """Render a focused AI decision card."""
    label = (
        _clean_text(node.get("display_label"))
        or _clean_text(node.get("node_id"))
    )
    node_type = _humanize(
        node.get("node_type")
    )
    depth_label = _clean_text(
        node.get("depth_label")
    )
    decision_category = (
        _clean_text(
            node.get("decision_category")
        ).upper()
        or "PENDING"
    )
    decision_label = _clean_text(
        node.get("decision_label")
    )
    expansion_outcome = _clean_text(
        node.get("expansion_outcome")
    )

    st.markdown(
        '<div class="mule-kicker">'
        "Selected node"
        "</div>",
        unsafe_allow_html=True,
    )
    st.subheader(label)

    escaped_decision = html.escape(
        decision_label
        or expansion_outcome
        or "Pending"
    )

    st.markdown(
        (
            '<span class="mule-decision '
            f'mule-decision-{decision_category}">'
            f"{escaped_decision}"
            "</span>"
        ),
        unsafe_allow_html=True,
    )

    detail_columns = st.columns(3)

    detail_columns[0].caption("Type")
    detail_columns[0].write(node_type)

    detail_columns[1].caption("Journey stage")
    detail_columns[1].write(depth_label)

    detail_columns[2].caption("Confidence")
    detail_columns[2].write(
        _confidence_label(
            node.get("confidence")
        )
    )

    st.divider()

    if bool(node.get("is_seed")):
        st.markdown("#### Investigation starting point")
        st.write(
            "This customer triggered the network "
            "investigation and forms depth zero."
        )
    else:
        st.markdown("#### AI outcome")
        st.write(
            expansion_outcome
            or "The AI decision is still pending."
        )

        st.markdown("#### Why the AI decided this")
        st.write(
            _clean_text(node.get("rationale"))
            or (
                "No AI rationale is available for "
                "this persisted decision."
            )
        )

        st.markdown("#### Strongest evidence")
        key_evidence = _clean_text(
            node.get("key_evidence")
        )

        if key_evidence:
            st.markdown(key_evidence)
        else:
            st.caption(
                "No summarized AI evidence is "
                "available for this node."
            )

        st.markdown("#### Discovery path")

        parent_label = _clean_text(
            node.get("parent_display_label")
        )
        discovered_via = _clean_text(
            node.get("discovered_via")
        )

        if parent_label:
            st.write(
                f"Discovered from **{parent_label}** "
                f"through **{discovered_via}**."
            )
        else:
            st.write(
                discovered_via
                or "Discovery route unavailable."
            )

    collapsed_customer_count = _integer(
        node.get("collapsed_customer_count")
    )

    if collapsed_customer_count > 0:
        st.info(
            f"{collapsed_customer_count:,} linked "
            "customers were summarized here. "
            "The AI stopped expansion, so those "
            "customers are not displayed."
        )

    _render_analyst_feedback(
        node=node,
        feedback_store=feedback_store,
        run_id=run_id,
        group_id=group_id,
    )


def main() -> None:
    """Render the analyst-first investigation application."""
    _inject_styles()

    state_directory = resolve_state_directory()

    try:
        feedback_store = CsvAnalystFeedbackStore(
            state_directory
        )
        application = AnalystApplicationStateStore(
            state_directory
        )
        catalogue = _investigation_catalogue(
            application
        )
    except Exception as exc:
        st.error(
            "Investigations could not be loaded: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    st.sidebar.markdown("## Investigations")
    st.sidebar.caption(
        "Select a discovered network to review "
        "the AI expansion journey."
    )

    if st.sidebar.button(
        "Reload investigations",
        type="secondary",
        use_container_width=True,
    ):
        st.rerun()

    if not catalogue:
        st.title("Mule Network Investigation")
        st.info(
            "No persisted investigations are available."
        )
        return

    catalogue_by_key = {
        str(record["key"]): record
        for record in catalogue
    }

    selected_key = st.sidebar.selectbox(
        "Choose a network",
        options=list(catalogue_by_key),
        format_func=lambda key: str(
            catalogue_by_key[key]["label"]
        ),
        label_visibility="collapsed",
    )

    selected = catalogue_by_key[
        selected_key
    ]
    run_id = str(selected["run_id"])
    group_id = str(selected["group_id"])

    try:
        network = AnalystGroupNetworkStore(
            state_directory
        ).load(
            run_id=run_id,
            group_id=group_id,
        )
        evidence = AnalystGroupEvidenceStore(
            state_directory
        ).load(
            run_id=run_id,
            group_id=group_id,
        )
        investigation = (
            build_analyst_investigation_view(
                nodes=network.nodes,
                edges=network.edges,
                decisions=(
                    evidence.decision_evidence
                ),
                ai_calls=(
                    evidence.ai_call_evidence
                ),
            )
        )
        graph = (
            build_analyst_investigation_graph(
                investigation.nodes
            )
        )
    except (
        AnalystApplicationStateError,
        ConsolidatedStateError,
        FileNotFoundError,
        ValueError,
    ) as exc:
        st.error(
            "The selected investigation could not "
            "be prepared: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    status_label, status_class = (
        _status_presentation(
            investigation.investigation_status
        )
    )

    st.markdown(
        '<div class="mule-kicker">'
        "AI-guided network investigation"
        "</div>",
        unsafe_allow_html=True,
    )
    st.title("Mule Network Investigation")

    header_left, header_right = st.columns(
        [4, 1]
    )

    with header_left:
        st.write(
            f"Seed **{selected['anchor']}** · "
            f"{selected['run_date']} · "
            f"{_humanize(selected['group_status'])}"
        )

    with header_right:
        st.markdown(
            (
                '<div style="text-align:right;">'
                f'<span class="mule-status {status_class}">'
                f"{html.escape(status_label)}"
                "</span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    metric_columns = st.columns(5)

    metric_columns[0].metric(
        "Journey depth",
        investigation.max_depth,
    )
    metric_columns[1].metric(
        "AI expanded",
        investigation.expanded_node_count,
    )
    metric_columns[2].metric(
        "AI stopped",
        investigation.stopped_node_count,
    )
    metric_columns[3].metric(
        "Awaiting AI",
        investigation.pending_node_count,
    )
    metric_columns[4].metric(
        "Customers summarized",
        investigation.collapsed_customer_count,
    )

    st.markdown("")
    graph_column, decision_column = st.columns(
        [2.15, 1],
        gap="large",
    )

    selection_state_key = (
        f"selected-investigation-node::"
        f"{run_id}::{group_id}"
    )

    with graph_column:
        st.subheader("AI investigation path")
        st.caption(
            "Only AI-approved expansion paths and "
            "explicit stopping points are shown. "
            "Click a node to review the decision."
        )
        _render_legend()

        graph_event = streamlit_cytoscape(
            elements=graph.elements,
            layout=graph.layout,
            node_styles=analyst_node_styles(),
            edge_styles=analyst_edge_styles(),
            height=760,
            key=(
                f"investigation-graph::"
                f"{run_id}::{group_id}"
            ),
            events=[NODE_SELECTED_EVENT],
            node_actions=[],
            edge_actions=[],
            hide_underscore_attrs=True,
        )

        clicked_node_id = (
            selected_investigation_node_id(
                graph_event
            )
        )

        if clicked_node_id:
            st.session_state[
                selection_state_key
            ] = clicked_node_id

    selected_node_id = st.session_state.get(
        selection_state_key,
        graph.seed_node_ids[0],
    )

    available_node_ids = set(
        investigation.nodes["node_id"]
        .astype("string")
        .str.strip()
    )

    if selected_node_id not in available_node_ids:
        selected_node_id = graph.seed_node_ids[0]
        st.session_state[
            selection_state_key
        ] = selected_node_id

    selected_node = (
        investigation.nodes.loc[
            investigation.nodes[
                "node_id"
            ].astype("string").eq(
                selected_node_id
            )
        ]
        .iloc[0]
    )

    with decision_column:
        with st.container(border=True):
            _render_selected_node(
                node=selected_node,
                feedback_store=feedback_store,
                run_id=run_id,
                group_id=group_id,
            )


if __name__ == "__main__":
    main()
