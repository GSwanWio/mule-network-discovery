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
from network_mule_discovery.analyst_review_queue import (
    AnalystReviewQueue,
    build_analyst_review_queue,
)
from network_mule_discovery.consolidated_state import (
    ConsolidatedStateError,
)


STATE_DIRECTORY_ENV_VAR = "MULE_NETWORK_STATE_DIRECTORY"
ANALYST_ID_ENV_VAR = "MULE_ANALYST_ID"
DEFAULT_STATE_DIRECTORY = PROJECT_ROOT / "data/state"


def _analyst_id() -> str:
    """Return the analyst identity used for review progress."""
    return (
        os.getenv(
            ANALYST_ID_ENV_VAR,
            "",
        ).strip()
        or "UNSPECIFIED"
    )


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

        .mule-decision-DETERMINISTIC {
            background: #FEE2E2;
            color: #991B1B;
        }

        .mule-decision-CONTINUE {
            background: #FEE2E2;
            color: #991B1B;
        }

        .mule-decision-STOP {
            background: #DCFCE7;
            color: #166534;
        }

        .mule-decision-STOP-CUSTOMER {
            background: #FEF3C7;
            color: #92400E;
        }

        .mule-decision-STOP-COUNTERPARTY {
            background: #DCFCE7;
            color: #166534;
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

        .mule-dot-suspicious {
            background: #DC2626;
        }

        .mule-dot-legitimate {
            background: #16A34A;
        }

        .mule-dot-exposed-customer {
            background: #D97706;
        }

        .mule-review-stats {
            display: grid;
            gap: 0.45rem;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            margin: 0.8rem 0 0.9rem 0;
        }

        .mule-review-stat {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            min-width: 0;
            padding: 0.65rem 0.25rem;
            text-align: center;
        }

        .mule-review-stat-value {
            color: #0F172A;
            font-size: 1.7rem;
            font-weight: 700;
            line-height: 1;
        }

        .mule-review-stat-label {
            color: #64748B;
            font-size: 0.68rem;
            font-weight: 700;
            letter-spacing: 0.01em;
            line-height: 1.2;
            margin-top: 0.35rem;
            overflow-wrap: anywhere;
            text-transform: uppercase;
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
                <span class="mule-dot mule-dot-suspicious"></span>
                Suspicious or mule
            </span>
            <span class="mule-legend-item">
                <span class="mule-dot mule-dot-legitimate"></span>
                Legitimate counterparty
            </span>
            <span class="mule-legend-item">
                <span class="mule-dot mule-dot-exposed-customer"></span>
                Non-mule customer / potential victim
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _feedback_subject(
    node: pd.Series,
) -> tuple[str, str]:
    """Return the review subject represented by one node."""
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
            "The selected node has no reviewable subject."
        )

    return subject_type, subject_key


def _latest_feedback_for_analyst(
    *,
    feedback_store: CsvAnalystFeedbackStore,
    run_id: str,
    group_id: str,
    node_id: str,
    analyst_id: str,
) -> pd.Series | None:
    """Return this analyst's latest feedback for one node."""
    feedback = feedback_store.load()

    if feedback.empty:
        return None

    scoped = feedback.loc[
        feedback["run_id"]
        .astype("string")
        .str.strip()
        .eq(run_id)
        & feedback["group_id"]
        .astype("string")
        .str.strip()
        .eq(group_id)
        & feedback["node_id"]
        .astype("string")
        .str.strip()
        .eq(node_id)
        & feedback["analyst_id"]
        .astype("string")
        .str.strip()
        .eq(analyst_id)
    ].copy()

    if scoped.empty:
        return None

    return (
        scoped.sort_values(
            by=[
                "submitted_at",
                "feedback_id",
            ],
            kind="stable",
        )
        .iloc[-1]
    )


def _render_analyst_feedback(
    *,
    node: pd.Series,
    feedback_store: CsvAnalystFeedbackStore,
    run_id: str,
    group_id: str,
    analyst_id: str,
) -> None:
    """Render one mandatory node-decision review."""
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
        "Confirm whether this node decision is correct. "
        "The review is recorded as an immutable event and "
        "does not change the persisted decision or network."
    )

    latest = _latest_feedback_for_analyst(
        feedback_store=feedback_store,
        run_id=run_id,
        group_id=group_id,
        node_id=node_id,
        analyst_id=analyst_id,
    )

    if latest is not None:
        if _clean_text(latest.get("feedback")) == "AI_CORRECT":
            st.success(
                "Your latest review: decision marked correct"
            )
        else:
            st.warning(
                "Your latest review: decision marked incorrect"
            )

        previous_note = _clean_text(
            latest.get("analyst_notes")
        )
        if previous_note:
            st.caption(
                f"Your previous note: {previous_note}"
            )

    form_key = (
        "analyst-feedback::"
        f"{run_id}::{group_id}::{node_id}::{analyst_id}"
    )

    with st.form(
        form_key,
        clear_on_submit=True,
    ):
        selected_feedback = st.radio(
            "Was this node decision correct?",
            options=(
                "Decision correct",
                "Decision incorrect",
            ),
            index=None,
            horizontal=True,
        )
        analyst_notes = st.text_area(
            "Review note",
            placeholder=(
                "Optional context explaining how the "
                "evidence supports or contradicts the decision."
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
            "Select whether the node decision was "
            "correct or incorrect."
        )
        return

    feedback_value = (
        "AI_CORRECT"
        if selected_feedback == "Decision correct"
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
            analyst_id=analyst_id,
        )
    except AnalystFeedbackError as exc:
        st.error(
            "The analyst review could not be saved: "
            f"{exc}"
        )
        return

    st.success(
        "Review saved. The persisted node decision "
        "remains unchanged."
    )
    st.rerun()


def _review_filter_rows(
    queue: AnalystReviewQueue,
    selected_filter: str,
) -> pd.DataFrame:
    """Filter review rows for one analyst workflow view."""
    rows = queue.rows.copy()

    if selected_filter == "Unreviewed":
        rows = rows.loc[
            rows["review_status"].eq("UNREVIEWED")
        ]
    elif selected_filter == "Suspicious":
        rows = rows.loc[
            rows["review_outcome"].eq("SUSPICIOUS")
        ]
    elif selected_filter == "Non-suspicious":
        rows = rows.loc[
            rows["review_outcome"].eq("NON_SUSPICIOUS")
        ]
    elif selected_filter == "Disagreed":
        rows = rows.loc[
            rows["review_status"].eq(
                "REVIEWED_INCORRECT"
            )
        ]

    return rows.reset_index(drop=True)


def _next_unreviewed_node_id(
    queue: AnalystReviewQueue,
    selected_node_id: str,
) -> str | None:
    """Return the next unreviewed decision in queue order."""
    unreviewed = queue.rows.loc[
        queue.rows["review_status"].eq("UNREVIEWED")
    ].sort_values(
        by=["review_order", "node_id"],
        kind="stable",
    )

    if unreviewed.empty:
        return None

    selected_rows = queue.rows.loc[
        queue.rows["node_id"]
        .astype("string")
        .eq(selected_node_id)
    ]

    if selected_rows.empty:
        return str(unreviewed.iloc[0]["node_id"])

    selected_order = int(
        selected_rows.iloc[0]["review_order"]
    )
    later = unreviewed.loc[
        unreviewed["review_order"] > selected_order
    ]

    target = (
        later.iloc[0]
        if not later.empty
        else unreviewed.iloc[0]
    )
    return str(target["node_id"])


def _review_queue_button_label(
    row: object,
) -> str:
    """Return a compact direct-navigation queue label."""
    review_status = _clean_text(
        getattr(row, "review_status", "")
    ).upper()
    review_outcome = _clean_text(
        getattr(row, "review_outcome", "")
    ).upper()

    review_icon = {
        "UNREVIEWED": "○",
        "REVIEWED_CORRECT": "✓",
        "REVIEWED_INCORRECT": "!",
    }.get(review_status, "○")
    subject_type = _clean_text(
        getattr(row, "subject_type", "")
    ).upper()

    if review_outcome == "SUSPICIOUS":
        outcome_icon = "🔴"
    elif subject_type == "CUSTOMER":
        outcome_icon = "🟠"
    else:
        outcome_icon = "🟢"

    display_label = _clean_text(
        getattr(row, "display_label", "")
    )
    decision_label = _clean_text(
        getattr(row, "decision_label", "")
    )

    return (
        f"{review_icon} {outcome_icon} "
        f"{display_label} · {decision_label}"
    )


def _render_decision_review_queue(
    *,
    queue: AnalystReviewQueue,
    selection_state_key: str,
    selected_node_id: str,
) -> None:
    """Render one direct-click node review workflow."""
    st.subheader("Decision review")
    st.caption(
        "Select a decision to open it. The graph and "
        "evidence panel use the same selected node."
    )

    st.markdown(
        (
            '<div class="mule-review-stats">'
            '<div class="mule-review-stat">'
            '<div class="mule-review-stat-value">'
            f"{queue.reviewed_count}"
            "</div>"
            '<div class="mule-review-stat-label">'
            "Reviewed"
            "</div>"
            "</div>"
            '<div class="mule-review-stat">'
            '<div class="mule-review-stat-value">'
            f"{queue.unreviewed_count}"
            "</div>"
            '<div class="mule-review-stat-label">'
            "Remaining"
            "</div>"
            "</div>"
            '<div class="mule-review-stat">'
            '<div class="mule-review-stat-value">'
            f"{queue.incorrect_count}"
            "</div>"
            '<div class="mule-review-stat-label">'
            "Disagreed"
            "</div>"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    st.progress(
        queue.completion_percentage / 100.0,
        text=(
            f"{queue.reviewed_count} of "
            f"{queue.total_required} decisions reviewed "
            f"({queue.completion_percentage:.1f}%)"
        ),
    )

    if queue.review_complete:
        st.success(
            "Decision review complete for this analyst."
        )
    else:
        next_node_id = _next_unreviewed_node_id(
            queue,
            selected_node_id,
        )
        if next_node_id and st.button(
            "Next unreviewed",
            type="primary",
            use_container_width=True,
            key=(
                "decision-review-next::"
                f"{queue.run_id}::{queue.group_id}::"
                f"{queue.analyst_id}"
            ),
        ):
            st.session_state[
                selection_state_key
            ] = next_node_id
            st.rerun()

    selected_filter = st.radio(
        "Review filter",
        options=(
            "All",
            "Unreviewed",
            "Suspicious",
            "Non-suspicious",
            "Disagreed",
        ),
        horizontal=False,
        key=(
            "decision-review-filter::"
            f"{queue.run_id}::{queue.group_id}::"
            f"{queue.analyst_id}"
        ),
    )

    filtered = _review_filter_rows(
        queue,
        selected_filter,
    )

    if filtered.empty:
        st.info(
            "No decisions match the selected review filter."
        )
        return

    st.caption(
        "○ unreviewed · ✓ agreed · ! disagreed"
    )

    for row in filtered.itertuples(index=False):
        node_id = str(row.node_id)
        is_selected = node_id == selected_node_id
        if st.button(
            _review_queue_button_label(row),
            type=(
                "primary"
                if is_selected
                else "secondary"
            ),
            use_container_width=True,
            key=(
                "decision-review-row::"
                f"{queue.run_id}::{queue.group_id}::"
                f"{queue.analyst_id}::{node_id}"
            ),
            help=(
                f"{_humanize(row.review_status)} · "
                f"{_humanize(row.evidence_status)}"
            ),
        ):
            st.session_state[
                selection_state_key
            ] = node_id
            st.rerun()

def _render_selected_node(
    *,
    node: pd.Series,
    feedback_store: CsvAnalystFeedbackStore,
    run_id: str,
    group_id: str,
    analyst_id: str,
) -> None:
    """Render a focused investigation outcome card."""
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
    decision_style_category = decision_category

    if decision_category == "STOP":
        decision_style_category = (
            f"STOP-{_clean_text(node.get('node_type')).upper()}"
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
            f'mule-decision-{decision_style_category}">'
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
        "Not required"
        if decision_category == "DETERMINISTIC"
        else _confidence_label(
            node.get("confidence")
        )
    )

    st.divider()

    is_seed = bool(node.get("is_seed"))

    if is_seed:
        st.markdown("#### Investigation starting point")
        st.write(
            "This customer triggered the network "
            "investigation and forms depth zero."
        )
    elif decision_category == "DETERMINISTIC":
        st.markdown("#### Deterministic relationship")
        st.write(
            expansion_outcome
            or (
                "The customer was included through "
                "a deterministic identity link."
            )
        )

        st.markdown("#### Why no AI decision was required")
        st.write(
            "The customer shares the same valid "
            "Emirates ID as the confirmed seed. "
            "This relationship is included directly "
            "under the EID-linking contract."
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

    if not is_seed:
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
        analyst_id=analyst_id,
    )


def main() -> None:
    """Render the analyst-first investigation application."""
    _inject_styles()

    state_directory = resolve_state_directory()
    analyst_id = _analyst_id()

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
        "the investigation journey."
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
        review_queue = build_analyst_review_queue(
            run_id=run_id,
            group_id=group_id,
            nodes=investigation.nodes,
            feedback=feedback_store.load(),
            analyst_id=analyst_id,
        )
    except (
        AnalystApplicationStateError,
        AnalystFeedbackError,
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
        "Network investigation"
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

    metric_columns = st.columns(6)

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
        "Deterministic links",
        investigation.deterministic_node_count,
    )
    metric_columns[4].metric(
        "Awaiting AI",
        investigation.pending_node_count,
    )
    metric_columns[5].metric(
        "Customers summarized",
        investigation.collapsed_customer_count,
    )

    selection_state_key = (
        f"selected-investigation-node::"
        f"{run_id}::{group_id}"
    )

    available_node_ids = set(
        investigation.nodes["node_id"]
        .astype("string")
        .str.strip()
    )
    selected_node_id = str(
        st.session_state.get(
            selection_state_key,
            graph.seed_node_ids[0],
        )
    )

    if selected_node_id not in available_node_ids:
        selected_node_id = graph.seed_node_ids[0]
        st.session_state[
            selection_state_key
        ] = selected_node_id

    st.markdown("")
    queue_column, graph_column, decision_column = (
        st.columns(
            [1.05, 2.1, 1.15],
            gap="large",
        )
    )

    # Render the graph first so a graph click updates the
    # shared selected-node state before the queue and detail
    # panels are rendered on the next pass.
    with graph_column:
        st.subheader("Investigation path")
        st.caption(
            "Click any node to inspect its outcome. "
            "Suspicious paths continue; non-suspicious "
            "paths stop."
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

        if (
            clicked_node_id
            and clicked_node_id != selected_node_id
        ):
            st.session_state[
                selection_state_key
            ] = clicked_node_id
            st.rerun()

    selected_node_id = str(
        st.session_state.get(
            selection_state_key,
            graph.seed_node_ids[0],
        )
    )

    with queue_column:
        with st.container(border=True):
            _render_decision_review_queue(
                queue=review_queue,
                selection_state_key=(
                    selection_state_key
                ),
                selected_node_id=selected_node_id,
            )

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
                analyst_id=analyst_id,
            )


if __name__ == "__main__":
    main()
