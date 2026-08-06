"""Apply persisted AI decisions and build incremental expansion work."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from network_mule_discovery.schemas import (
    SchemaValidationError,
    parse_run_date,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


DECISION_REQUIRED_COLUMNS = (
    "decision_id",
    "subject_type",
    "subject_key",
    "feature_snapshot_hash",
    "decision",
    "reason_code",
    "decision_version",
    "decided_at",
    "source",
)

VALID_SUBJECT_TYPES = frozenset({
    "COUNTERPARTY",
    "CUSTOMER",
})

VALID_COUNTERPARTY_DECISIONS = frozenset({
    "SUSPICIOUS_EXPAND",
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
})

VALID_CUSTOMER_DECISIONS = frozenset({
    "MULE_LIKE",
    "EXPOSED_VULNERABLE",
    "LOW_CONCERN",
    "INSUFFICIENT_EVIDENCE",
})

COUNTERPARTY_STATUS_BY_DECISION = {
    "SUSPICIOUS_EXPAND": (
        "COUNTERPARTY_APPROVED_SUSPICIOUS"
    ),
    "LEGITIMATE_SUPPRESS": (
        "COUNTERPARTY_SUPPRESSED_LEGITIMATE"
    ),
    "COMMON_PUBLIC_SUPPRESS": (
        "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC"
    ),
    "INSUFFICIENT_EVIDENCE_SUPPRESS": (
        "COUNTERPARTY_SUPPRESSED_INSUFFICIENT_EVIDENCE"
    ),
}

CUSTOMER_STATUS_BY_DECISION = {
    "MULE_LIKE": "CUSTOMER_APPROVED_MULE_LIKE",
    "EXPOSED_VULNERABLE": (
        "CUSTOMER_ASSESSED_EXPOSED_VULNERABLE"
    ),
    "LOW_CONCERN": "CUSTOMER_ASSESSED_LOW_CONCERN",
    "INSUFFICIENT_EVIDENCE": (
        "CUSTOMER_ASSESSED_INSUFFICIENT_EVIDENCE"
    ),
}

DETERMINISTIC_CUSTOMER_EDGE_TYPES = frozenset({
    "SAME_EMIRATES_ID",
    "BENEFICIARY_ADDED_SEED_ACCOUNT",
    "BENEFICIARY_ADDED_MULE_ACCOUNT",
})

COUNTERPARTY_EDGE_TYPES = frozenset({
    "SEED_COUNTERPARTY_EVIDENCE",
    "CUSTOMER_COUNTERPARTY_EVIDENCE",
    "SHARED_EXTERNAL_COUNTERPARTY",
})

CUSTOMER_TARGET_DISCOVERY_EDGE_TYPES = frozenset({
    "SAME_EMIRATES_ID",
    "SHARED_EXTERNAL_COUNTERPARTY",
})

CUSTOMER_SOURCE_DISCOVERY_EDGE_TYPES = frozenset({
    "BENEFICIARY_ADDED_SEED_ACCOUNT",
    "BENEFICIARY_ADDED_MULE_ACCOUNT",
})

COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT = 10
COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_METHOD = (
    "ANCHOR_RELATIONSHIPS_THEN_HIGHEST_ACTIVITY"
)


@dataclass(frozen=True)
class DecisionProjectionResult:
    """Outputs after persisted decisions are applied."""

    groups: pd.DataFrame
    nodes: pd.DataFrame
    edges: pd.DataFrame
    subject_snapshots: pd.DataFrame
    applied_decisions: pd.DataFrame
    ignored_decisions: pd.DataFrame
    expansion_queue: pd.DataFrame


def _stable_id(
    prefix: str,
    *values: object,
) -> str:
    """Create a deterministic identifier."""
    canonical_value = "|".join(
        str(value)
        for value in values
    )

    digest = hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()[:16]

    return f"{prefix}{digest}"


NUMERIC_FEATURE_COLUMNS = frozenset({
    "source_event_count",
    "candidate_event_count",
})


def _is_missing_value(
    value: object,
) -> bool:
    """Return whether one scalar feature value is missing."""
    if value is None:
        return True

    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _canonical_json_value(
    column: str,
    value: object,
) -> object:
    """
    Normalize equivalent CSV and in-memory feature values.

    Examples:
    - None, pandas NA and blank strings become null.
    - Numeric count values such as 2, 2.0 and "2" become 2.
    - Other strings are trimmed but remain strings.
    """
    if _is_missing_value(value):
        return None

    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass

    if isinstance(value, (date, pd.Timestamp)):
        return str(value)

    if isinstance(value, str):
        value = value.strip()

        if not value:
            return None

    if column in NUMERIC_FEATURE_COLUMNS:
        try:
            numeric_value = float(value)
        except (TypeError, ValueError) as exc:
            raise SchemaValidationError(
                "Feature column "
                f"{column} contains a non-numeric value: "
                f"{value!r}"
            ) from exc

        if numeric_value.is_integer():
            return int(numeric_value)

        return numeric_value

    if isinstance(value, str):
        return value

    return value


def _canonical_records(
    frame: pd.DataFrame,
    columns: list[str],
) -> list[dict[str, object]]:
    """Serialize records in a stable order."""
    records: list[dict[str, object]] = []

    for source_record in frame[columns].to_dict(
        orient="records"
    ):
        record = {
            key: _canonical_json_value(
                column=key,
                value=value,
            )
            for key, value in source_record.items()
        }

        records.append(record)

    return sorted(
        records,
        key=lambda record: json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )

def _hash_payload(
    payload: dict[str, object],
) -> tuple[str, str]:
    """Return the snapshot hash and canonical JSON payload."""
    payload_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    feature_snapshot_hash = hashlib.sha256(
        payload_json.encode("utf-8")
    ).hexdigest()

    return feature_snapshot_hash, payload_json


def _record_digest(
    records: list[dict[str, object]],
) -> str:
    """Hash a complete canonical record set without exposing it."""
    canonical_json = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


def _numeric_record_value(
    record: dict[str, object],
    field_name: str,
) -> float:
    """Return one optional numeric record value for stable ranking."""
    value = record.get(field_name)

    if value is None:
        return 0.0

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_counterparty_relationship_payload(
    *,
    relationships: pd.DataFrame,
    relationship_columns: list[str],
    subject_node_keys: set[str],
) -> tuple[
    list[dict[str, object]],
    dict[str, object] | None,
]:
    """Build bounded counterparty graph evidence with a full-set digest."""
    full_records = _canonical_records(
        frame=relationships,
        columns=relationship_columns,
    )

    edge_type_counts: dict[str, int] = {}
    connected_node_keys: set[str] = set()
    total_source_event_count = 0.0
    total_candidate_event_count = 0.0

    for record in full_records:
        edge_type = str(record.get("edge_type") or "UNKNOWN")
        edge_type_counts[edge_type] = (
            edge_type_counts.get(edge_type, 0) + 1
        )

        for field_name in (
            "source_node_key",
            "target_node_key",
        ):
            node_key = record.get(field_name)

            if (
                node_key is not None
                and str(node_key) not in subject_node_keys
            ):
                connected_node_keys.add(str(node_key))

        total_source_event_count += _numeric_record_value(
            record,
            "source_event_count",
        )
        total_candidate_event_count += _numeric_record_value(
            record,
            "candidate_event_count",
        )

    relationship_count = len(full_records)
    sample_limit = (
        COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT
    )

    if relationship_count <= sample_limit:
        # Preserve the legacy payload exactly for small counterparties.
        # Adding sampling metadata here would invalidate otherwise reusable
        # cached AI decisions solely because this guardrail was introduced.
        return full_records, None
    else:
        anchor_records = [
            record
            for record in full_records
            if record.get("edge_type")
            != "SHARED_EXTERNAL_COUNTERPARTY"
        ]
        linked_customer_records = [
            record
            for record in full_records
            if record.get("edge_type")
            == "SHARED_EXTERNAL_COUNTERPARTY"
        ]

        anchor_records = sorted(
            anchor_records,
            key=lambda record: json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

        linked_customer_records = sorted(
            linked_customer_records,
            key=lambda record: (
                -_numeric_record_value(
                    record,
                    "candidate_event_count",
                ),
                -_numeric_record_value(
                    record,
                    "source_event_count",
                ),
                json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )

        sampled_records = anchor_records[:sample_limit]
        remaining_capacity = (
            sample_limit - len(sampled_records)
        )

        if remaining_capacity > 0:
            sampled_records.extend(
                linked_customer_records[
                    :remaining_capacity
                ]
            )

        sampled_records = sorted(
            sampled_records,
            key=lambda record: json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        payload_mode = "BOUNDED_SAMPLE"
        sampling_method = (
            COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_METHOD
        )

    summary = {
        "payload_mode": payload_mode,
        "relationship_count": relationship_count,
        "sample_limit": sample_limit,
        "sampled_relationship_count": len(
            sampled_records
        ),
        "omitted_relationship_count": (
            relationship_count - len(sampled_records)
        ),
        "sampling_method": sampling_method,
        "connected_subject_count": len(
            connected_node_keys
        ),
        "edge_type_counts": dict(
            sorted(edge_type_counts.items())
        ),
        "total_source_event_count": int(
            total_source_event_count
        ),
        "total_candidate_event_count": int(
            total_candidate_event_count
        ),
        "full_relationship_digest": _record_digest(
            full_records
        ),
    }

    return sampled_records, summary


def _get_subject_relationships(
    edges: pd.DataFrame,
    subject_type: str,
    node_keys: set[str],
) -> pd.DataFrame:
    """
    Select relationships that form the subject's decision evidence.

    Counterparty decisions use every relationship touching the
    counterparty.

    Customer decisions use the relationship that caused the customer
    to enter the group. Outgoing relationships discovered only after
    approving the customer for expansion are excluded, preventing an
    unnecessary second AI call solely because expansion occurred.
    """
    if subject_type == "COUNTERPARTY":
        return edges.loc[
            edges["source_node_key"].isin(node_keys)
            | edges["target_node_key"].isin(node_keys)
        ].copy()

    target_evidence_mask = (
        edges["target_node_key"].isin(node_keys)
        & edges["edge_type"].isin(
            CUSTOMER_TARGET_DISCOVERY_EDGE_TYPES
        )
    )

    source_evidence_mask = (
        edges["source_node_key"].isin(node_keys)
        & edges["edge_type"].isin(
            CUSTOMER_SOURCE_DISCOVERY_EDGE_TYPES
        )
    )

    return edges.loc[
        target_evidence_mask
        | source_evidence_mask
    ].copy()


def _prepare_supplemental_subject_payloads(
    supplemental_subject_payloads: pd.DataFrame | None,
) -> dict[tuple[str, str], dict[str, object]]:
    """Validate optional production-derived subject evidence."""
    if supplemental_subject_payloads is None:
        return {}

    required_columns = {
        "subject_type",
        "subject_key",
        "feature_payload_json",
    }

    missing_columns = sorted(
        required_columns
        - set(supplemental_subject_payloads.columns)
    )

    if missing_columns:
        raise SchemaValidationError(
            "supplemental_subject_payloads is missing columns: "
            f"{missing_columns}"
        )

    prepared = supplemental_subject_payloads[
        [
            "subject_type",
            "subject_key",
            "feature_payload_json",
        ]
    ].copy()

    for column in (
        "subject_type",
        "subject_key",
        "feature_payload_json",
    ):
        prepared[column] = (
            prepared[column]
            .astype("string")
            .str.strip()
        )

        if (
            prepared[column].isna()
            | prepared[column].eq("")
        ).any():
            raise SchemaValidationError(
                "supplemental_subject_payloads."
                f"{column} contains null or blank values."
            )

    prepared["subject_type"] = prepared[
        "subject_type"
    ].str.upper()

    duplicate_mask = prepared.duplicated(
        subset=[
            "subject_type",
            "subject_key",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        duplicates = (
            prepared.loc[
                duplicate_mask,
                [
                    "subject_type",
                    "subject_key",
                ],
            ]
            .drop_duplicates()
            .to_dict(orient="records")
        )

        raise SchemaValidationError(
            "supplemental_subject_payloads contains duplicate "
            f"subjects: {duplicates}"
        )

    payload_map: dict[
        tuple[str, str],
        dict[str, object],
    ] = {}

    for row in prepared.itertuples(index=False):
        try:
            payload = json.loads(
                row.feature_payload_json
            )
        except json.JSONDecodeError as exc:
            raise SchemaValidationError(
                "supplemental_subject_payloads contains invalid "
                f"JSON for {row.subject_type}|{row.subject_key}."
            ) from exc

        if not isinstance(payload, dict):
            raise SchemaValidationError(
                "Supplemental subject evidence must be a JSON "
                f"object for {row.subject_type}|{row.subject_key}."
            )

        if payload.get("subject_type") != row.subject_type:
            raise SchemaValidationError(
                "Supplemental subject_type does not match its "
                f"payload for {row.subject_type}|{row.subject_key}."
            )

        if payload.get("subject_key") != row.subject_key:
            raise SchemaValidationError(
                "Supplemental subject_key does not match its "
                f"payload for {row.subject_type}|{row.subject_key}."
            )

        payload_map[(row.subject_type, row.subject_key)] = payload

    return payload_map


def build_subject_snapshots(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    supplemental_subject_payloads: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build global customer and counterparty feature snapshots.

    Graph evidence is always included. Optional neutral behavioral or
    registry evidence is combined with the graph evidence before the
    reusable feature hash is calculated.
    """
    supplemental_payload_map = (
        _prepare_supplemental_subject_payloads(
            supplemental_subject_payloads
        )
    )

    subject_records: list[dict[str, object]] = []
    observed_subjects: set[tuple[str, str]] = set()

    subject_definitions = [
        (
            "CUSTOMER",
            "entity_key",
            nodes["node_type"].eq("CUSTOMER"),
        ),
        (
            "COUNTERPARTY",
            "counterparty_key",
            nodes["node_type"].eq("COUNTERPARTY"),
        ),
    ]

    node_payload_columns = [
        "node_key",
        "node_type",
        "entity_type",
        "entity_id",
        "entity_key",
        "counterparty_key",
        "display_label",
        "node_roles",
    ]

    edge_payload_columns = [
        "source_node_key",
        "target_node_key",
        "edge_type",
        "evidence_key",
        "evidence_summary",
        "source_event_count",
        "candidate_event_count",
    ]

    for (
        subject_type,
        subject_column,
        subject_mask,
    ) in subject_definitions:
        subject_nodes = nodes.loc[
            subject_mask
            & nodes[subject_column].notna()
            & nodes[subject_column].astype(
                "string"
            ).str.strip().ne("")
        ].copy()

        subject_keys = sorted(
            subject_nodes[subject_column]
            .drop_duplicates()
            .tolist()
        )

        for subject_key in subject_keys:
            subject_identity = (
                subject_type,
                str(subject_key),
            )
            observed_subjects.add(subject_identity)

            current_nodes = subject_nodes.loc[
                subject_nodes[subject_column]
                == subject_key
            ].copy()

            node_keys = set(
                current_nodes["node_key"].tolist()
            )

            current_edges = _get_subject_relationships(
                edges=edges,
                subject_type=subject_type,
                node_keys=node_keys,
            )

            group_ids = sorted(
                current_nodes["group_id"]
                .drop_duplicates()
                .tolist()
            )

            if subject_type == "COUNTERPARTY":
                (
                    relationship_payload,
                    relationship_evidence_summary,
                ) = _build_counterparty_relationship_payload(
                    relationships=current_edges,
                    relationship_columns=(
                        edge_payload_columns
                    ),
                    subject_node_keys=node_keys,
                )
            else:
                relationship_payload = _canonical_records(
                    frame=current_edges,
                    columns=edge_payload_columns,
                )
                relationship_evidence_summary = None

            graph_payload = {
                "subject_type": subject_type,
                "subject_key": subject_key,
                "nodes": _canonical_records(
                    frame=current_nodes,
                    columns=node_payload_columns,
                ),
                "relationships": relationship_payload,
            }

            if relationship_evidence_summary is not None:
                graph_payload[
                    "relationship_evidence_summary"
                ] = relationship_evidence_summary

            supplemental_payload = (
                supplemental_payload_map.get(
                    subject_identity
                )
            )

            if supplemental_payload is None:
                payload = graph_payload
                supplemental_evidence_included = False
            else:
                payload = {
                    **graph_payload,
                    "behavioral_evidence": (
                        supplemental_payload
                    ),
                }
                supplemental_evidence_included = True

            (
                feature_snapshot_hash,
                payload_json,
            ) = _hash_payload(payload)

            subject_records.append(
                {
                    "subject_type": subject_type,
                    "subject_key": subject_key,
                    "feature_snapshot_hash": (
                        feature_snapshot_hash
                    ),
                    "group_ids": "|".join(
                        group_ids
                    ),
                    "node_count": len(
                        current_nodes
                    ),
                    "relationship_count": len(
                        current_edges
                    ),
                    "supplemental_evidence_included": (
                        supplemental_evidence_included
                    ),
                    "feature_payload_json": (
                        payload_json
                    ),
                }
            )

    unobserved_supplemental_subjects = sorted(
        set(supplemental_payload_map)
        - observed_subjects
    )

    if unobserved_supplemental_subjects:
        raise SchemaValidationError(
            "Supplemental evidence was supplied for unobserved "
            f"subjects: {unobserved_supplemental_subjects}"
        )

    return (
        pd.DataFrame(subject_records)
        .sort_values(
            by=[
                "subject_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def prepare_decisions(
    decisions: pd.DataFrame,
) -> pd.DataFrame:
    """Validate and normalize persisted AI decisions."""
    missing_columns = sorted(
        set(DECISION_REQUIRED_COLUMNS)
        - set(decisions.columns)
    )

    if missing_columns:
        raise SchemaValidationError(
            "ai_decisions is missing columns: "
            f"{missing_columns}"
        )

    prepared = decisions.copy()

    for column in DECISION_REQUIRED_COLUMNS:
        if column == "decided_at":
            continue

        prepared[column] = (
            prepared[column]
            .astype("string")
            .str.strip()
        )

    required_nonblank_columns = (
        "decision_id",
        "subject_type",
        "subject_key",
        "feature_snapshot_hash",
        "decision",
        "decision_version",
        "source",
    )

    for column in required_nonblank_columns:
        blank_mask = (
            prepared[column].isna()
            | prepared[column].eq("")
        )

        if blank_mask.any():
            raise SchemaValidationError(
                f"ai_decisions.{column} contains "
                "null or blank values."
            )

    prepared["subject_type"] = prepared[
        "subject_type"
    ].str.upper()

    prepared["decision"] = prepared[
        "decision"
    ].str.upper()

    prepared["decided_at"] = pd.to_datetime(
        prepared["decided_at"],
        errors="coerce",
    )

    if prepared["decided_at"].isna().any():
        raise SchemaValidationError(
            "ai_decisions.decided_at contains "
            "invalid timestamps."
        )

    invalid_subject_types = sorted(
        set(prepared["subject_type"])
        - VALID_SUBJECT_TYPES
    )

    if invalid_subject_types:
        raise SchemaValidationError(
            "ai_decisions contains unsupported "
            f"subject types: {invalid_subject_types}"
        )

    counterparty_decisions = set(
        prepared.loc[
            prepared["subject_type"]
            == "COUNTERPARTY",
            "decision",
        ]
    )

    invalid_counterparty_decisions = sorted(
        counterparty_decisions
        - VALID_COUNTERPARTY_DECISIONS
    )

    if invalid_counterparty_decisions:
        raise SchemaValidationError(
            "ai_decisions contains unsupported "
            "counterparty decisions: "
            f"{invalid_counterparty_decisions}"
        )

    customer_decisions = set(
        prepared.loc[
            prepared["subject_type"]
            == "CUSTOMER",
            "decision",
        ]
    )

    invalid_customer_decisions = sorted(
        customer_decisions
        - VALID_CUSTOMER_DECISIONS
    )

    if invalid_customer_decisions:
        raise SchemaValidationError(
            "ai_decisions contains unsupported "
            "customer decisions: "
            f"{invalid_customer_decisions}"
        )

    prepared = (
        prepared
        .drop_duplicates()
        .sort_values(
            by=[
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
                "decided_at",
                "decision_id",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
            keep="last",
        )
        .reset_index(drop=True)
    )

    return prepared


def _find_counterparty_key(
    source_node_key: object,
    target_node_key: object,
) -> str | None:
    """Extract a counterparty key from an edge."""
    for node_key in (
        source_node_key,
        target_node_key,
    ):
        text = str(node_key)

        if text.startswith("COUNTERPARTY|"):
            return text.split("|", 1)[1]

    return None


def apply_persisted_decisions(
    unified_result: UnifiedGroupResult,
    decisions: pd.DataFrame,
    run_date: date | str,
    supplemental_subject_payloads: pd.DataFrame | None = None,
) -> DecisionProjectionResult:
    """Apply reusable decisions and queue only unresolved work."""
    resolved_run_date = parse_run_date(run_date)

    groups = unified_result.groups.copy()
    nodes = unified_result.nodes.copy()
    edges = unified_result.edges.copy()

    subject_snapshots = build_subject_snapshots(
        nodes=nodes,
        edges=edges,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
    )

    prepared_decisions = prepare_decisions(
        decisions
    )

    reusable_decisions = (
        subject_snapshots
        .merge(
            prepared_decisions,
            how="inner",
            on=[
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
            validate="one_to_one",
        )
    )

    reusable_decision_map = {
        (
            row.subject_type,
            row.subject_key,
        ): row
        for row in reusable_decisions.itertuples(
            index=False
        )
    }

    matching_decision_ids = set(
        reusable_decisions[
            "decision_id"
        ].tolist()
    )

    observed_subject_keys = set(
        zip(
            subject_snapshots[
                "subject_type"
            ],
            subject_snapshots[
                "subject_key"
            ],
        )
    )

    ignored_decisions = prepared_decisions.loc[
        ~prepared_decisions["decision_id"].isin(
            matching_decision_ids
        )
    ].copy()

    ignored_decisions[
        "ignored_reason"
    ] = ignored_decisions.apply(
        lambda row: (
            "FEATURE_SNAPSHOT_CHANGED"
            if (
                row["subject_type"],
                row["subject_key"],
            ) in observed_subject_keys
            else "SUBJECT_NOT_OBSERVED"
        ),
        axis=1,
    )

    snapshot_hash_map = {
        (
            row.subject_type,
            row.subject_key,
        ): row.feature_snapshot_hash
        for row in subject_snapshots.itertuples(
            index=False
        )
    }

    nodes["feature_snapshot_hash"] = (
        nodes.apply(
            lambda row: snapshot_hash_map.get(
                (
                    (
                        "CUSTOMER"
                        if row["node_type"]
                        == "CUSTOMER"
                        else "COUNTERPARTY"
                    ),
                    (
                        row["entity_key"]
                        if row["node_type"]
                        == "CUSTOMER"
                        else row[
                            "counterparty_key"
                        ]
                    ),
                )
            ),
            axis=1,
        )
        .astype("string")
    )

    nodes["applied_decision_id"] = pd.Series(
        pd.NA,
        index=nodes.index,
        dtype="string",
    )

    nodes["applied_decision"] = pd.Series(
        pd.NA,
        index=nodes.index,
        dtype="string",
    )

    nodes["decision_reason_code"] = pd.Series(
        pd.NA,
        index=nodes.index,
        dtype="string",
    )

    nodes["decision_reuse_flag"] = False

    applied_decision_ids: set[str] = set()

    counterparty_decision_by_key: dict[
        str,
        object,
    ] = {}

    counterparty_nodes = nodes.loc[
        nodes["node_type"] == "COUNTERPARTY"
    ]

    for row in counterparty_nodes.itertuples(
        index=True
    ):
        decision_record = reusable_decision_map.get(
            (
                "COUNTERPARTY",
                row.counterparty_key,
            )
        )

        if decision_record is None:
            continue

        counterparty_decision_by_key[
            row.counterparty_key
        ] = decision_record

        nodes.at[
            row.Index,
            "node_status",
        ] = COUNTERPARTY_STATUS_BY_DECISION[
            decision_record.decision
        ]

        nodes.at[
            row.Index,
            "customer_discovery_allowed_flag",
        ] = (
            decision_record.decision
            == "SUSPICIOUS_EXPAND"
        )

        nodes.at[
            row.Index,
            "applied_decision_id",
        ] = decision_record.decision_id

        nodes.at[
            row.Index,
            "applied_decision",
        ] = decision_record.decision

        nodes.at[
            row.Index,
            "decision_reason_code",
        ] = decision_record.reason_code

        nodes.at[
            row.Index,
            "decision_reuse_flag",
        ] = True

        applied_decision_ids.add(
            decision_record.decision_id
        )

    edges["counterparty_key"] = edges.apply(
        lambda row: _find_counterparty_key(
            source_node_key=row[
                "source_node_key"
            ],
            target_node_key=row[
                "target_node_key"
            ],
        ),
        axis=1,
    )

    for row in edges.loc[
        edges["edge_type"].isin(
            COUNTERPARTY_EDGE_TYPES
        )
    ].itertuples(index=True):
        decision_record = (
            counterparty_decision_by_key.get(
                row.counterparty_key
            )
        )

        if decision_record is None:
            continue

        if (
            decision_record.decision
            == "SUSPICIOUS_EXPAND"
        ):
            relationship_status = (
                "COUNTERPARTY_APPROVED_SUSPICIOUS"
            )

            customer_discovery_allowed = True
        else:
            relationship_status = (
                COUNTERPARTY_STATUS_BY_DECISION[
                    decision_record.decision
                ]
            )

            customer_discovery_allowed = False

        edges.at[
            row.Index,
            "relationship_status",
        ] = relationship_status

        edges.at[
            row.Index,
            "customer_discovery_allowed_flag",
        ] = customer_discovery_allowed

        edges.at[
            row.Index,
            "recursive_expansion_allowed_flag",
        ] = False

    eligible_customer_groups: dict[
        str,
        set[str],
    ] = {}
    same_eid_customer_groups: dict[
        str,
        set[str],
    ] = {}

    customer_nodes = nodes.loc[
        nodes["node_type"] == "CUSTOMER"
    ]

    for row in customer_nodes.itertuples(
        index=True
    ):
        if (
            row.customer_assessment_status
            == "SEED_CONFIRMED"
        ):
            continue

        incident_edges = edges.loc[
            edges["group_id"].eq(row.group_id)
            & (
                edges["source_node_key"].eq(
                    row.node_key
                )
                | edges["target_node_key"].eq(
                    row.node_key
                )
            )
        ]

        deterministic_edge_allowed = (
            incident_edges[
                "customer_discovery_allowed_flag"
            ]
            .astype("string")
            .str.upper()
            .eq("TRUE")
        )
        same_eid_eligible = (
            incident_edges["edge_type"]
            .eq("SAME_EMIRATES_ID")
            & deterministic_edge_allowed
        ).any()
        deterministic_eligible = (
            incident_edges["edge_type"]
            .isin(
                DETERMINISTIC_CUSTOMER_EDGE_TYPES
            )
            & deterministic_edge_allowed
        ).any()

        approved_counterparty_eligible = (
            incident_edges["edge_type"]
            .eq("SHARED_EXTERNAL_COUNTERPARTY")
            & incident_edges[
                "relationship_status"
            ].eq(
                "COUNTERPARTY_APPROVED_SUSPICIOUS"
            )
        ).any()

        assessment_eligible = bool(
            deterministic_eligible
            or approved_counterparty_eligible
        )

        if not assessment_eligible:
            continue

        eligible_customer_groups.setdefault(
            row.entity_key,
            set(),
        ).add(row.group_id)

        if same_eid_eligible:
            same_eid_customer_groups.setdefault(
                row.entity_key,
                set(),
            ).add(row.group_id)

        nodes.at[
            row.Index,
            "node_status",
        ] = "INCLUDED_FOR_CUSTOMER_ASSESSMENT"

        nodes.at[
            row.Index,
            "customer_assessment_status",
        ] = "PENDING_CUSTOMER_AI"

        nodes.at[
            row.Index,
            "customer_discovery_allowed_flag",
        ] = True

        decision_record = reusable_decision_map.get(
            (
                "CUSTOMER",
                row.entity_key,
            )
        )

        if decision_record is None:
            continue

        nodes.at[
            row.Index,
            "node_status",
        ] = CUSTOMER_STATUS_BY_DECISION[
            decision_record.decision
        ]

        nodes.at[
            row.Index,
            "customer_assessment_status",
        ] = decision_record.decision

        nodes.at[
            row.Index,
            "expansion_source_flag",
        ] = bool(
            same_eid_eligible
            or decision_record.decision
            == "MULE_LIKE"
        )

        nodes.at[
            row.Index,
            "applied_decision_id",
        ] = decision_record.decision_id

        nodes.at[
            row.Index,
            "applied_decision",
        ] = decision_record.decision

        nodes.at[
            row.Index,
            "decision_reason_code",
        ] = decision_record.reason_code

        nodes.at[
            row.Index,
            "decision_reuse_flag",
        ] = True

        applied_decision_ids.add(
            decision_record.decision_id
        )

    queue_rows: list[dict[str, Any]] = []

    def append_queue_item(
        *,
        action_type: str,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        group_ids: list[str],
        queue_reason: str,
        priority: int,
        trigger_decision_id: str | None = None,
    ) -> None:
        queue_item_id = _stable_id(
            "Q",
            action_type,
            subject_type,
            subject_key,
            feature_snapshot_hash,
        )

        queue_rows.append(
            {
                "queue_item_id": queue_item_id,
                "run_date": resolved_run_date,
                "action_type": action_type,
                "subject_type": subject_type,
                "subject_key": subject_key,
                "feature_snapshot_hash": (
                    feature_snapshot_hash
                ),
                "group_ids": "|".join(
                    sorted(group_ids)
                ),
                "trigger_decision_id": (
                    trigger_decision_id
                ),
                "queue_reason": queue_reason,
                "priority": priority,
                "queue_status": "READY",
            }
        )

    counterparty_snapshots = (
        subject_snapshots.loc[
            subject_snapshots["subject_type"]
            == "COUNTERPARTY"
        ]
    )

    for row in counterparty_snapshots.itertuples(
        index=False
    ):
        if (
            "COUNTERPARTY",
            row.subject_key,
        ) in reusable_decision_map:
            continue

        append_queue_item(
            action_type="RUN_COUNTERPARTY_AI",
            subject_type="COUNTERPARTY",
            subject_key=row.subject_key,
            feature_snapshot_hash=(
                row.feature_snapshot_hash
            ),
            group_ids=row.group_ids.split("|"),
            queue_reason=(
                "NEW_OR_CHANGED_COUNTERPARTY_EVIDENCE"
            ),
            priority=10,
        )

    customer_snapshots = (
        subject_snapshots.loc[
            subject_snapshots["subject_type"]
            == "CUSTOMER"
        ]
    )

    customer_snapshot_map = {
        row.subject_key: row
        for row in customer_snapshots.itertuples(
            index=False
        )
    }

    for (
        customer_key,
        customer_group_ids,
    ) in sorted(
        eligible_customer_groups.items()
    ):
        snapshot = customer_snapshot_map[
            customer_key
        ]

        decision_record = reusable_decision_map.get(
            (
                "CUSTOMER",
                customer_key,
            )
        )

        if decision_record is None:
            append_queue_item(
                action_type="RUN_CUSTOMER_AI",
                subject_type="CUSTOMER",
                subject_key=customer_key,
                feature_snapshot_hash=(
                    snapshot.feature_snapshot_hash
                ),
                group_ids=sorted(
                    customer_group_ids
                ),
                queue_reason=(
                    "DETERMINISTIC_OR_APPROVED_RELATIONSHIP"
                ),
                priority=20,
            )

            continue

        if customer_key in same_eid_customer_groups:
            append_queue_item(
                action_type=(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                ),
                subject_type="CUSTOMER",
                subject_key=customer_key,
                feature_snapshot_hash=(
                    snapshot.feature_snapshot_hash
                ),
                group_ids=sorted(
                    customer_group_ids
                ),
                queue_reason=(
                    "DETERMINISTIC_SAME_EMIRATES_ID"
                ),
                priority=30,
            )
            continue

        if decision_record.decision == "MULE_LIKE":
            append_queue_item(
                action_type=(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                ),
                subject_type="CUSTOMER",
                subject_key=customer_key,
                feature_snapshot_hash=(
                    snapshot.feature_snapshot_hash
                ),
                group_ids=sorted(
                    customer_group_ids
                ),
                queue_reason=(
                    "CUSTOMER_DECISION_MULE_LIKE"
                ),
                priority=30,
                trigger_decision_id=(
                    decision_record.decision_id
                ),
            )

    expansion_queue = pd.DataFrame(queue_rows)

    if expansion_queue.empty:
        expansion_queue = pd.DataFrame(
            columns=[
                "queue_item_id",
                "run_date",
                "action_type",
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
                "group_ids",
                "trigger_decision_id",
                "queue_reason",
                "priority",
                "queue_status",
            ]
        )
    else:
        expansion_queue = (
            expansion_queue
            .drop_duplicates(
                subset=["queue_item_id"]
            )
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

    for group_index, group_row in (
        groups.iterrows()
    ):
        group_id = group_row["group_id"]

        group_nodes = nodes.loc[
            nodes["group_id"] == group_id
        ]

        customer_group_nodes = group_nodes.loc[
            group_nodes["node_type"]
            == "CUSTOMER"
        ]

        counterparty_group_nodes = (
            group_nodes.loc[
                group_nodes["node_type"]
                == "COUNTERPARTY"
            ]
        )

        group_queue_count = int(
            expansion_queue["group_ids"]
            .astype("string")
            .str.split("|")
            .map(
                lambda values: (
                    group_id in values
                    if isinstance(values, list)
                    else False
                )
            )
            .sum()
        )

        groups.at[
            group_index,
            "customer_assessment_pending_count",
        ] = int(
            customer_group_nodes[
                "customer_assessment_status"
            ]
            .eq("PENDING_CUSTOMER_AI")
            .sum()
        )

        groups.at[
            group_index,
            "counterparty_ai_pending_count",
        ] = int(
            counterparty_group_nodes[
                "node_status"
            ]
            .eq(
                "OBSERVED_PENDING_COUNTERPARTY_AI"
            )
            .sum()
        )

        groups.at[
            group_index,
            "recursive_expansion_source_count",
        ] = int(
            group_nodes[
                "expansion_source_flag"
            ].sum()
        )

        groups.at[
            group_index,
            "approved_suspicious_counterparty_count",
        ] = int(
            counterparty_group_nodes[
                "node_status"
            ]
            .eq(
                "COUNTERPARTY_APPROVED_SUSPICIOUS"
            )
            .sum()
        )

        groups.at[
            group_index,
            "suppressed_counterparty_count",
        ] = int(
            counterparty_group_nodes[
                "node_status"
            ]
            .astype("string")
            .str.startswith(
                "COUNTERPARTY_SUPPRESSED"
            )
            .sum()
        )

        groups.at[
            group_index,
            "mule_like_customer_count",
        ] = int(
            customer_group_nodes[
                "customer_assessment_status"
            ]
            .eq("MULE_LIKE")
            .sum()
        )

        groups.at[
            group_index,
            "queued_action_count",
        ] = group_queue_count

    applied_decisions = (
        prepared_decisions.loc[
            prepared_decisions[
                "decision_id"
            ].isin(applied_decision_ids)
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

    ignored_decisions = (
        ignored_decisions
        .sort_values(
            by=[
                "subject_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    nodes = (
        nodes
        .sort_values(
            by=[
                "group_id",
                "expansion_source_flag",
                "node_type",
                "node_key",
            ],
            ascending=[
                True,
                False,
                True,
                True,
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    edges = (
        edges
        .drop(
            columns=["counterparty_key"]
        )
        .sort_values(
            by=[
                "group_id",
                "edge_type",
                "source_node_key",
                "target_node_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    groups = (
        groups
        .sort_values(
            by=["group_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    return DecisionProjectionResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
        subject_snapshots=subject_snapshots,
        applied_decisions=applied_decisions,
        ignored_decisions=ignored_decisions,
        expansion_queue=expansion_queue,
    )
