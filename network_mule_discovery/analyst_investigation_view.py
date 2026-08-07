"""Analyst-first breadth-and-depth investigation journey."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass

import pandas as pd

from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateError,
)
from network_mule_discovery.analyst_network_projection import (
    build_analyst_network_display_projection,
)


NODE_REQUIRED_COLUMNS = (
    "node_id",
    "node_type",
    "entity_key",
    "counterparty_key",
    "display_label",
    "node_roles",
    "node_status",
    "customer_assessment_status",
    "expansion_source_flag",
)

EDGE_REQUIRED_COLUMNS = (
    "edge_id",
    "source_node_id",
    "target_node_id",
    "edge_type",
    "relationship_status",
)

DECISION_REQUIRED_COLUMNS = (
    "decision_id",
    "subject_type",
    "subject_key",
    "decision",
    "reason_code",
    "decided_at",
)

AI_CALL_REQUIRED_COLUMNS = (
    "ai_call_id",
    "subject_type",
    "subject_key",
    "call_status",
    "attempted_at",
    "decision",
    "reason_code",
    "confidence",
    "rationale",
    "key_evidence_json",
)

CONTINUE_DECISIONS = frozenset({
    "SUSPICIOUS_EXPAND",
    "MULE_LIKE",
})

STOP_DECISIONS = frozenset({
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
    "EXPOSED_VULNERABLE",
    "LOW_CONCERN",
    "INSUFFICIENT_EVIDENCE",
})

OUTCOME_BY_DECISION = {
    "SUSPICIOUS_EXPAND": "Expanded — suspicious counterparty",
    "MULE_LIKE": "Expanded — mule-like customer",
    "LEGITIMATE_SUPPRESS": "Stopped — legitimate counterparty",
    "COMMON_PUBLIC_SUPPRESS": "Stopped — common/public counterparty",
    "INSUFFICIENT_EVIDENCE_SUPPRESS": (
        "Stopped — insufficient evidence"
    ),
    "EXPOSED_VULNERABLE": "Stopped — potential victim",
    "LOW_CONCERN": "Stopped — low-concern customer",
    "INSUFFICIENT_EVIDENCE": (
        "Stopped — insufficient customer evidence"
    ),
}

EDGE_LABELS = {
    "SAME_EMIRATES_ID": "Same Emirates ID",
    "SEED_COUNTERPARTY_EVIDENCE": (
        "Seed transaction counterparty"
    ),
    "CUSTOMER_COUNTERPARTY_EVIDENCE": (
        "Customer transaction counterparty"
    ),
    "SHARED_EXTERNAL_COUNTERPARTY": (
        "Shared external counterparty"
    ),
    "BENEFICIARY_ADDED_SEED_ACCOUNT": (
        "Added seed account as beneficiary"
    ),
    "BENEFICIARY_ADDED_MULE_ACCOUNT": (
        "Added mule account as beneficiary"
    ),
}


@dataclass(frozen=True)
class AnalystInvestigationView:
    """Complete analyst-facing representation of one investigation."""

    nodes: pd.DataFrame
    edges: pd.DataFrame
    collapsed_counterparties: pd.DataFrame
    investigation_status: str
    max_depth: int
    seed_count: int
    expanded_node_count: int
    stopped_node_count: int
    deterministic_node_count: int
    pending_node_count: int
    failed_node_count: int
    collapsed_customer_count: int


def _clean_text(value: object) -> str:
    """Return normalized display text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _humanize(value: object) -> str:
    """Convert one internal value into analyst text."""
    return (
        _clean_text(value)
        .replace("_", " ")
        .strip()
        .title()
    )



def _analyst_decision_label(value: object) -> str:
    """Return analyst-facing text for one AI decision."""
    normalized = _clean_text(value).upper()

    labels = {
        "EXPOSED_VULNERABLE": "Potential victim",
        "INSUFFICIENT_EVIDENCE_SUPPRESS": (
            "Insufficient evidence"
        ),
    }

    return labels.get(
        normalized,
        _humanize(normalized),
    )


def _validate_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...],
    *,
    frame_name: str,
) -> None:
    """Validate one required source frame."""
    missing_columns = sorted(
        set(required_columns) - set(frame.columns)
    )

    if missing_columns:
        raise AnalystApplicationStateError(
            f"{frame_name} is missing columns: "
            f"{missing_columns}"
        )


def _is_seed(row: object) -> bool:
    """Return whether one node is an investigation seed."""
    roles = {
        role.strip().upper()
        for role in _clean_text(
            getattr(row, "node_roles", "")
        ).split("|")
        if role.strip()
    }

    status_values = {
        _clean_text(
            getattr(
                row,
                "node_status",
                "",
            )
        ).upper(),
        _clean_text(
            getattr(
                row,
                "customer_assessment_status",
                "",
            )
        ).upper(),
    }

    return (
        "SEED" in roles
        or any(
            "SEED_CONFIRMED" in value
            for value in status_values
        )
    )


def _subject_key(row: object) -> tuple[str, str]:
    """Return the AI subject represented by one node."""
    node_type = _clean_text(
        row.node_type
    ).upper()

    if node_type == "CUSTOMER":
        return (
            node_type,
            _clean_text(row.entity_key),
        )

    if node_type == "COUNTERPARTY":
        return (
            node_type,
            _clean_text(row.counterparty_key),
        )

    return node_type, ""


def _latest_records(
    frame: pd.DataFrame,
    *,
    required_columns: tuple[str, ...],
    timestamp_column: str,
    identifier_column: str,
    frame_name: str,
) -> dict[tuple[str, str], dict[str, object]]:
    """Return the latest persisted record for each AI subject."""
    if frame.empty:
        return {}

    _validate_columns(
        frame,
        required_columns,
        frame_name=frame_name,
    )

    prepared = frame.copy()

    prepared["subject_type"] = (
        prepared["subject_type"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    prepared["subject_key"] = (
        prepared["subject_key"]
        .astype("string")
        .str.strip()
    )

    prepared = (
        prepared.sort_values(
            by=[
                timestamp_column,
                identifier_column,
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "subject_type",
                "subject_key",
            ],
            keep="last",
        )
    )

    return {
        (
            _clean_text(row["subject_type"]),
            _clean_text(row["subject_key"]),
        ): row.to_dict()
        for _, row in prepared.iterrows()
    }


def _parse_key_evidence(value: object) -> str:
    """Convert structured AI evidence into concise display text."""
    raw_value = _clean_text(value)

    if not raw_value:
        return ""

    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return raw_value

    if isinstance(parsed, list):
        return "\n".join(
            f"• {_clean_text(item)}"
            for item in parsed
            if _clean_text(item)
        )

    if isinstance(parsed, dict):
        return "\n".join(
            f"• {_humanize(key)}: {_clean_text(item)}"
            for key, item in parsed.items()
            if _clean_text(item)
        )

    return _clean_text(parsed)


def _decision_from_status(
    *,
    node_type: str,
    node_status: str,
    assessment_status: str,
) -> str:
    """Infer an analyst decision when no decision row is available."""
    combined_status = (
        f"{node_status}|{assessment_status}"
    ).upper()

    if "COUNTERPARTY_APPROVED_SUSPICIOUS" in combined_status:
        return "SUSPICIOUS_EXPAND"

    if "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC" in combined_status:
        return "COMMON_PUBLIC_SUPPRESS"

    if "COUNTERPARTY_SUPPRESSED_LEGITIMATE" in combined_status:
        return "LEGITIMATE_SUPPRESS"

    if (
        "COUNTERPARTY_SUPPRESSED_INSUFFICIENT_EVIDENCE"
        in combined_status
    ):
        return "INSUFFICIENT_EVIDENCE_SUPPRESS"

    if "MULE_LIKE" in combined_status:
        return "MULE_LIKE"

    if "EXPOSED_VULNERABLE" in combined_status:
        return "EXPOSED_VULNERABLE"

    if "LOW_CONCERN" in combined_status:
        return "LOW_CONCERN"

    if (
        node_type == "CUSTOMER"
        and "INSUFFICIENT_EVIDENCE" in combined_status
    ):
        return "INSUFFICIENT_EVIDENCE"

    return ""


def _decision_presentation(
    *,
    is_seed: bool,
    deterministic_same_eid: bool,
    decision: str,
    node_status: str,
    assessment_status: str,
) -> tuple[str, str]:
    """Return the journey category and analyst outcome."""
    if is_seed:
        return "SEED", "Starting point"

    if deterministic_same_eid:
        return (
            "DETERMINISTIC",
            (
                "Final determination — mule due to direct "
                "Emirates ID link"
            ),
        )

    normalized_decision = decision.upper()
    combined_status = (
        f"{node_status}|{assessment_status}"
    ).upper()

    if (
        "INCLUDED_DETERMINISTIC" in combined_status
        and "NOT_APPLICABLE" in combined_status
    ):
        return (
            "DETERMINISTIC",
            (
                "Final determination — mule due to direct "
                "Emirates ID link"
            ),
        )

    if "FAILED_CLOSED" in combined_status:
        return "FAILED", "Stopped — AI decision unavailable"

    if normalized_decision in CONTINUE_DECISIONS:
        return (
            "CONTINUE",
            OUTCOME_BY_DECISION[normalized_decision],
        )

    if normalized_decision in STOP_DECISIONS:
        return (
            "STOP",
            OUTCOME_BY_DECISION[normalized_decision],
        )

    return "PENDING", "Awaiting AI decision"


def _journey_edge_priority(
    edge_type: str,
    relationship_status: str,
) -> int:
    """Prefer valid discovery paths over suppressed side-links."""
    normalized_type = _clean_text(edge_type).upper()
    normalized_status = _clean_text(
        relationship_status
    ).upper()

    if normalized_type == "SAME_EMIRATES_ID":
        return 0

    if normalized_status == (
        "COUNTERPARTY_APPROVED_SUSPICIOUS"
    ):
        return 1

    if normalized_status.startswith(
        "COUNTERPARTY_SUPPRESSED"
    ):
        return 3

    return 2


def _build_breadth_first_journey(
    *,
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
) -> tuple[
    dict[str, int],
    dict[str, str],
    dict[str, str],
]:
    """Compute deterministic depth and discovery parent from seeds."""
    node_ids = set(
        nodes["node_id"]
        .astype("string")
        .str.strip()
    )

    adjacency: dict[
        str,
        list[tuple[str, str, str, str]],
    ] = {
        node_id: []
        for node_id in node_ids
    }

    for edge in edges.itertuples(index=False):
        source_id = _clean_text(
            edge.source_node_id
        )
        target_id = _clean_text(
            edge.target_node_id
        )

        if (
            source_id not in node_ids
            or target_id not in node_ids
        ):
            raise AnalystApplicationStateError(
                "Visible investigation edge references "
                "an unavailable node."
            )

        relationship = _clean_text(
            edge.edge_type
        )
        relationship_status = _clean_text(
            edge.relationship_status
        )

        adjacency[source_id].append(
            (
                target_id,
                _clean_text(edge.edge_id),
                relationship,
                relationship_status,
            )
        )
        adjacency[target_id].append(
            (
                source_id,
                _clean_text(edge.edge_id),
                relationship,
                relationship_status,
            )
        )

    seed_ids = sorted(
        _clean_text(row.node_id)
        for row in nodes.itertuples(index=False)
        if _is_seed(row)
    )

    if not seed_ids:
        raise AnalystApplicationStateError(
            "The analyst investigation has no confirmed seed."
        )

    depth_by_node = {
        seed_id: 0
        for seed_id in seed_ids
    }
    parent_by_node = {
        seed_id: ""
        for seed_id in seed_ids
    }
    relationship_by_node = {
        seed_id: ""
        for seed_id in seed_ids
    }

    queue = deque(seed_ids)

    while queue:
        node_id = queue.popleft()

        for (
            neighbour_id,
            _,
            edge_type,
            relationship_status,
        ) in sorted(
            adjacency[node_id],
            key=lambda item: (
                _journey_edge_priority(
                    item[2],
                    item[3],
                ),
                item[0],
                item[1],
            ),
        ):
            if neighbour_id in depth_by_node:
                continue

            depth_by_node[neighbour_id] = (
                depth_by_node[node_id] + 1
            )
            parent_by_node[neighbour_id] = node_id
            relationship_by_node[neighbour_id] = (
                edge_type
            )
            queue.append(neighbour_id)

    unreachable_nodes = sorted(
        node_ids - set(depth_by_node)
    )

    if unreachable_nodes:
        raise AnalystApplicationStateError(
            "Visible investigation contains nodes that "
            "cannot be reached from a confirmed seed: "
            f"{unreachable_nodes}"
        )

    return (
        depth_by_node,
        parent_by_node,
        relationship_by_node,
    )


def build_analyst_investigation_view(
    *,
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    decisions: pd.DataFrame,
    ai_calls: pd.DataFrame,
) -> AnalystInvestigationView:
    """Build the final AI-decided analyst investigation journey."""
    _validate_columns(
        nodes,
        NODE_REQUIRED_COLUMNS,
        frame_name="Investigation nodes",
    )
    _validate_columns(
        edges,
        EDGE_REQUIRED_COLUMNS,
        frame_name="Investigation edges",
    )

    projection = (
        build_analyst_network_display_projection(
            nodes=nodes,
            edges=edges,
        )
    )

    visible_nodes = projection.nodes.copy()
    visible_edges = projection.edges.copy()

    (
        depth_by_node,
        parent_by_node,
        relationship_by_node,
    ) = _build_breadth_first_journey(
        nodes=visible_nodes,
        edges=visible_edges,
    )

    latest_decisions = _latest_records(
        decisions,
        required_columns=DECISION_REQUIRED_COLUMNS,
        timestamp_column="decided_at",
        identifier_column="decision_id",
        frame_name="Investigation decisions",
    )

    latest_ai_calls = _latest_records(
        ai_calls,
        required_columns=AI_CALL_REQUIRED_COLUMNS,
        timestamp_column="attempted_at",
        identifier_column="ai_call_id",
        frame_name="Investigation AI calls",
    )

    same_eid_edges = visible_edges.loc[
        visible_edges["edge_type"]
        .astype("string")
        .str.strip()
        .str.upper()
        .eq("SAME_EMIRATES_ID")
    ]
    deterministic_same_eid_node_ids = set(
        same_eid_edges["source_node_id"]
        .astype("string")
        .str.strip()
    ).union(
        set(
            same_eid_edges["target_node_id"]
            .astype("string")
            .str.strip()
        )
    )

    display_labels = {
        _clean_text(row.node_id): (
            _clean_text(row.display_label)
            or _clean_text(row.entity_key)
            or _clean_text(row.counterparty_key)
        )
        for row in visible_nodes.itertuples(
            index=False
        )
    }

    enriched_records: list[
        dict[str, object]
    ] = []

    for row in visible_nodes.itertuples(
        index=False
    ):
        record = row._asdict()
        node_id = _clean_text(row.node_id)
        node_type, subject_key = _subject_key(row)
        subject = (
            node_type,
            subject_key,
        )

        decision_record = latest_decisions.get(
            subject,
            {},
        )
        ai_call_record = latest_ai_calls.get(
            subject,
            {},
        )

        seed_flag = _is_seed(row)

        decision = (
            _clean_text(
                decision_record.get(
                    "decision",
                    "",
                )
            )
            or _clean_text(
                ai_call_record.get(
                    "decision",
                    "",
                )
            )
            or _decision_from_status(
                node_type=node_type,
                node_status=_clean_text(
                    row.node_status
                ),
                assessment_status=_clean_text(
                    row.customer_assessment_status
                ),
            )
        )

        (
            decision_category,
            expansion_outcome,
        ) = _decision_presentation(
            is_seed=seed_flag,
            deterministic_same_eid=(
                not seed_flag
                and node_id
                in deterministic_same_eid_node_ids
            ),
            decision=decision,
            node_status=_clean_text(
                row.node_status
            ),
            assessment_status=_clean_text(
                row.customer_assessment_status
            ),
        )

        parent_node_id = parent_by_node[node_id]
        discovery_edge_type = (
            relationship_by_node[node_id]
        )
        depth = depth_by_node[node_id]

        record.update(
            {
                "depth": depth,
                "depth_label": (
                    "Seed"
                    if depth == 0
                    else f"Depth {depth}"
                ),
                "parent_node_id": parent_node_id,
                "parent_display_label": (
                    display_labels.get(
                        parent_node_id,
                        "",
                    )
                ),
                "discovered_via": (
                    EDGE_LABELS.get(
                        discovery_edge_type,
                        _humanize(
                            discovery_edge_type
                        ),
                    )
                ),
                "is_seed": seed_flag,
                "ai_decision": (
                    "DETERMINISTIC_EID_LINK"
                    if decision_category == "DETERMINISTIC"
                    else (
                        decision
                        if decision
                        else "PENDING"
                    )
                ),
                "behavioral_decision": (
                    decision
                    if decision
                    else (
                        "NOT_ASSESSED"
                        if decision_category == "DETERMINISTIC"
                        else "PENDING"
                    )
                ),
                "behavioral_decision_label": (
                    _analyst_decision_label(decision)
                    if decision
                    else (
                        "Not assessed"
                        if decision_category == "DETERMINISTIC"
                        else "Pending"
                    )
                ),
                "final_decision": (
                    "MULE"
                    if decision_category == "DETERMINISTIC"
                    else (
                        "SEED_CONFIRMED"
                        if seed_flag
                        else (
                            decision
                            if decision
                            else "PENDING"
                        )
                    )
                ),
                "final_decision_basis": (
                    "DIRECT_EMIRATES_ID_LINK"
                    if decision_category == "DETERMINISTIC"
                    else (
                        "SEED"
                        if seed_flag
                        else "AI_ASSESSMENT"
                    )
                ),
                "decision_label": (
                    "Confirmed seed"
                    if seed_flag
                    else (
                        "Mule — direct Emirates ID link"
                        if decision_category == "DETERMINISTIC"
                        else _analyst_decision_label(
                            decision
                            if decision
                            else "Pending"
                        )
                    )
                ),
                "decision_category": (
                    decision_category
                ),
                "expansion_outcome": (
                    expansion_outcome
                ),
                "reason_code": (
                    _clean_text(
                        decision_record.get(
                            "reason_code",
                            "",
                        )
                    )
                    or _clean_text(
                        ai_call_record.get(
                            "reason_code",
                            "",
                        )
                    )
                ),
                "confidence": _clean_text(
                    ai_call_record.get(
                        "confidence",
                        "",
                    )
                ),
                "rationale": _clean_text(
                    ai_call_record.get(
                        "rationale",
                        "",
                    )
                ),
                "key_evidence": (
                    _parse_key_evidence(
                        ai_call_record.get(
                            "key_evidence_json",
                            "",
                        )
                    )
                ),
                "ai_call_status": _clean_text(
                    ai_call_record.get(
                        "call_status",
                        "",
                    )
                ),
            }
        )

        enriched_records.append(record)

    enriched_nodes = (
        pd.DataFrame.from_records(
            enriched_records
        )
        .sort_values(
            by=[
                "depth",
                "node_type",
                "display_label",
                "node_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    enriched_edges = visible_edges.copy()

    enriched_edges["source_depth"] = (
        enriched_edges["source_node_id"]
        .map(depth_by_node)
        .astype(int)
    )
    enriched_edges["target_depth"] = (
        enriched_edges["target_node_id"]
        .map(depth_by_node)
        .astype(int)
    )
    enriched_edges["relationship_label"] = (
        enriched_edges["edge_type"]
        .map(EDGE_LABELS)
        .fillna(
            enriched_edges["edge_type"].map(
                _humanize
            )
        )
    )

    category_counts = (
        enriched_nodes["decision_category"]
        .value_counts()
        .to_dict()
    )

    failed_count = int(
        category_counts.get("FAILED", 0)
    )
    deterministic_count = int(
        category_counts.get("DETERMINISTIC", 0)
    )
    pending_count = int(
        category_counts.get("PENDING", 0)
    )

    if failed_count:
        investigation_status = "NEEDS_ATTENTION"
    elif pending_count:
        investigation_status = "IN_PROGRESS"
    elif (
        deterministic_count
        and not latest_decisions
        and not latest_ai_calls
    ):
        investigation_status = (
            "DETERMINISTIC_REVIEW_COMPLETE"
        )
    else:
        investigation_status = "AI_REVIEW_COMPLETE"

    return AnalystInvestigationView(
        nodes=enriched_nodes,
        edges=enriched_edges,
        collapsed_counterparties=(
            projection.collapsed_counterparties
        ),
        investigation_status=(
            investigation_status
        ),
        max_depth=int(
            enriched_nodes["depth"].max()
        ),
        seed_count=int(
            category_counts.get("SEED", 0)
        ),
        expanded_node_count=int(
            category_counts.get(
                "CONTINUE",
                0,
            )
        ),
        stopped_node_count=int(
            category_counts.get("STOP", 0)
        ),
        deterministic_node_count=(
            deterministic_count
        ),
        pending_node_count=pending_count,
        failed_node_count=failed_count,
        collapsed_customer_count=int(
            projection.hidden_node_count
        ),
    )
