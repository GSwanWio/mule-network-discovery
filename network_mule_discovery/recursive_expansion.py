"""Bounded recursive expansion using persisted decisions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from network_mule_discovery.counterparty_data_sources import (
    CounterpartyNetworkDataSource,
)
from network_mule_discovery.decision_engine import (
    DECISION_REQUIRED_COLUMNS,
    DecisionProjectionResult,
    apply_persisted_decisions,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)
from network_mule_discovery.unified_group_runner import (
    run_unified_group_projection,
)


VALID_RECURSIVE_RELATIONSHIP_TYPES = frozenset({
    "SAME_EMIRATES_ID",
    "SHARED_EXTERNAL_COUNTERPARTY",
    "BENEFICIARY_ADDED_MULE_ACCOUNT",
})

AI_ACTION_TYPES = frozenset({
    "RUN_COUNTERPARTY_AI",
    "RUN_CUSTOMER_AI",
})

DISCOVERY_ACTION_TYPE = (
    "DISCOVER_CUSTOMER_RELATIONSHIPS"
)


@dataclass(frozen=True)
class RecursiveGuardrails:
    """Limits for one recursive network run."""

    max_rounds: int = 10
    max_ai_calls: int = 25
    max_customer_expansions: int = 10
    max_total_nodes: int = 100
    max_total_edges: int = 200


@dataclass(frozen=True)
class RecursiveExpansionResult:
    """Final recursive network and execution state."""

    groups: pd.DataFrame
    nodes: pd.DataFrame
    edges: pd.DataFrame
    decision_history: pd.DataFrame
    generated_decisions: pd.DataFrame
    remaining_queue: pd.DataFrame
    round_log: pd.DataFrame
    expansion_ledger: pd.DataFrame
    termination_reason: str


class PreparedExpansionEvidenceSource:
    """Read prepared relationships for approved expansion customers."""

    REQUIRED_COLUMNS = (
        "snapshot_date",
        "source_entity_key",
        "relationship_type",
        "counterparty_key",
        "counterparty_name",
        "target_entity_type",
        "target_entity_id",
        "target_entity_key",
        "evidence_key",
        "evidence_summary",
        "source_event_count",
        "candidate_event_count",
    )

    def __init__(
        self,
        path: Path | str,
    ) -> None:
        self.path = Path(path)

        frame = pd.read_csv(
            self.path,
            dtype="string",
            keep_default_na=False,
        )

        missing_columns = sorted(
            set(self.REQUIRED_COLUMNS)
            - set(frame.columns)
        )

        if missing_columns:
            raise ValueError(
                "recursive_relationship_candidates "
                f"is missing columns: {missing_columns}"
            )

        frame["snapshot_date"] = pd.to_datetime(
            frame["snapshot_date"],
            errors="raise",
        ).dt.date

        invalid_types = sorted(
            set(frame["relationship_type"])
            - VALID_RECURSIVE_RELATIONSHIP_TYPES
        )

        if invalid_types:
            raise ValueError(
                "Unsupported recursive relationship types: "
                f"{invalid_types}"
            )

        self.frame = frame

    def get_relationships(
        self,
        run_date: date | str,
        source_entity_key: str,
    ) -> pd.DataFrame:
        """Return prepared relationships for one expansion source."""
        resolved_run_date = parse_run_date(run_date)

        return (
            self.frame.loc[
                self.frame["snapshot_date"].eq(
                    resolved_run_date
                )
                & self.frame[
                    "source_entity_key"
                ].eq(source_entity_key)
            ]
            .sort_values(
                by=[
                    "relationship_type",
                    "evidence_key",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )


class DeterministicDemoDecisionAdapter:
    """Deterministic replacement for the future live AI adapters."""

    COUNTERPARTY_DECISIONS = {
        "IBAN|AE120260000000000077701": (
            "SUSPICIOUS_EXPAND",
            "RAPID_SHARED_SENDER_PATTERN",
        ),
        "LOCAL_ACCOUNT|009991001": (
            "LEGITIMATE_SUPPRESS",
            "DOCUMENTED_LOCAL_SUPPLIER",
        ),
        "LOCAL_ACCOUNT|004440001": (
            "SUSPICIOUS_EXPAND",
            "UNEXPLAINED_SECOND_LAYER_SHARING",
        ),
        "IBAN|GB82WEST12345698765432": (
            "LEGITIMATE_SUPPRESS",
            "ESTABLISHED_COMMERCIAL_SUPPLIER",
        ),
        "SWIFT_ACCOUNT|BOFAUS3N|998877": (
            "SUSPICIOUS_EXPAND",
            "UNEXPLAINED_THIRD_LAYER_SHARING",
        ),
    }

    CUSTOMER_DECISIONS = {
        "RETAIL|R4001": (
            "MULE_LIKE",
            "DETERMINISTIC_EID_AND_FLOW_PATTERN",
        ),
        "SME|B4002": (
            "MULE_LIKE",
            "SHARED_SUSPICIOUS_COUNTERPARTY",
        ),
        "SME|B4003": (
            "LOW_CONCERN",
            "PLAUSIBLE_LINK_WITHOUT_MULE_ACTIVITY",
        ),
        "RETAIL|R4005": (
            "MULE_LIKE",
            "APPROVED_MULE_ADDED_AS_BENEFICIARY",
        ),
        "RETAIL|R5001": (
            "LOW_CONCERN",
            "EID_LINK_WITHOUT_SUPPORTING_ACTIVITY",
        ),
        "RETAIL|R5002": (
            "LOW_CONCERN",
            "COUNTERPARTY_LINK_WITHOUT_MULE_ACTIVITY",
        ),
    }

    def decide(
        self,
        *,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        run_date: date,
        round_number: int,
        sequence_number: int,
    ) -> dict[str, str]:
        """Return one deterministic structured decision."""
        if subject_type == "COUNTERPARTY":
            (
                decision,
                reason_code,
            ) = self.COUNTERPARTY_DECISIONS.get(
                subject_key,
                (
                    "LEGITIMATE_SUPPRESS",
                    "DEFAULT_DEMO_COUNTERPARTY_SUPPRESSION",
                ),
            )

        elif subject_type == "CUSTOMER":
            (
                decision,
                reason_code,
            ) = self.CUSTOMER_DECISIONS.get(
                subject_key,
                (
                    "LOW_CONCERN",
                    "DEFAULT_DEMO_CUSTOMER_LOW_CONCERN",
                ),
            )

        else:
            raise ValueError(
                f"Unsupported subject type: {subject_type}"
            )

        decision_id = _stable_id(
            "RD",
            subject_type,
            subject_key,
            feature_snapshot_hash,
            decision,
            "recursive-demo-v1",
        )

        decided_at = (
            pd.Timestamp(run_date)
            + pd.Timedelta(hours=20)
            + pd.Timedelta(minutes=round_number)
            + pd.Timedelta(seconds=sequence_number)
        )

        return {
            "decision_id": decision_id,
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "decision": decision,
            "reason_code": reason_code,
            "decision_version": "recursive-demo-v1",
            "decided_at": str(decided_at),
            "source": "DETERMINISTIC_DEMO_ADAPTER",
        }


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


def _optional_integer(value: object) -> int | None:
    """Convert a nonblank numeric value to an integer."""
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()

    if not text:
        return None

    return int(float(text))


def _combine_roles(
    existing_roles: object,
    new_role: str,
) -> str:
    """Combine pipe-delimited node roles."""
    roles = {
        role
        for role in str(existing_roles).split("|")
        if role
    }

    roles.add(new_role)

    return "|".join(sorted(roles))


def _upsert_customer_node(
    nodes: pd.DataFrame,
    *,
    run_id: str,
    run_date: date,
    group_id: str,
    entity_type: str,
    entity_id: str,
    entity_key: str,
    role: str,
    node_status: str,
    assessment_status: str,
    discovery_allowed: bool,
) -> pd.DataFrame:
    """Add or strengthen one customer node."""
    node_key = f"CUSTOMER|{entity_key}"

    mask = (
        nodes["group_id"].eq(group_id)
        & nodes["node_key"].eq(node_key)
    )

    if mask.any():
        index = nodes.index[mask][0]

        nodes.at[
            index,
            "node_roles",
        ] = _combine_roles(
            nodes.at[index, "node_roles"],
            role,
        )

        current_status = str(
            nodes.at[index, "node_status"]
        )

        status_rank = {
            "OBSERVED_PENDING_COUNTERPARTY_AI": 1,
            "INCLUDED_FOR_CUSTOMER_ASSESSMENT": 2,
            "INCLUDED_DETERMINISTIC": 3,
            "SEED_EXPANSION_SOURCE": 4,
        }

        if (
            status_rank.get(node_status, 0)
            > status_rank.get(current_status, 0)
        ):
            nodes.at[
                index,
                "node_status",
            ] = node_status

        current_assessment = str(
            nodes.at[
                index,
                "customer_assessment_status",
            ]
        )

        assessment_rank = {
            "NOT_APPLICABLE": 0,
            "BLOCKED_PENDING_COUNTERPARTY_AI": 1,
            "PENDING_CUSTOMER_AI": 2,
            "SEED_CONFIRMED": 3,
        }

        if (
            assessment_rank.get(
                assessment_status,
                0,
            )
            > assessment_rank.get(
                current_assessment,
                0,
            )
        ):
            nodes.at[
                index,
                "customer_assessment_status",
            ] = assessment_status

        nodes.at[
            index,
            "customer_discovery_allowed_flag",
        ] = bool(
            nodes.at[
                index,
                "customer_discovery_allowed_flag",
            ]
            or discovery_allowed
        )

        nodes.at[
            index,
            "last_seen_date",
        ] = run_date

        return nodes

    node = {
        "run_id": run_id,
        "run_date": run_date,
        "group_id": group_id,
        "node_id": _stable_id(
            "N",
            group_id,
            node_key,
        ),
        "node_key": node_key,
        "node_type": "CUSTOMER",
        "entity_type": entity_type,
        "entity_id": entity_id,
        "entity_key": entity_key,
        "counterparty_key": None,
        "display_label": entity_key,
        "node_roles": role,
        "node_status": node_status,
        "customer_assessment_status": (
            assessment_status
        ),
        "customer_discovery_allowed_flag": (
            discovery_allowed
        ),
        "expansion_source_flag": False,
        "first_seen_date": run_date,
        "last_seen_date": run_date,
    }

    return pd.concat(
        [
            nodes,
            pd.DataFrame([node]),
        ],
        ignore_index=True,
    )


def _upsert_counterparty_node(
    nodes: pd.DataFrame,
    *,
    run_id: str,
    run_date: date,
    group_id: str,
    counterparty_key: str,
    display_label: str,
) -> pd.DataFrame:
    """Add one pending external counterparty node."""
    node_key = f"COUNTERPARTY|{counterparty_key}"

    mask = (
        nodes["group_id"].eq(group_id)
        & nodes["node_key"].eq(node_key)
    )

    if mask.any():
        index = nodes.index[mask][0]

        nodes.at[
            index,
            "node_roles",
        ] = _combine_roles(
            nodes.at[index, "node_roles"],
            "EXTERNAL_COUNTERPARTY_CANDIDATE",
        )

        nodes.at[
            index,
            "last_seen_date",
        ] = run_date

        return nodes

    node = {
        "run_id": run_id,
        "run_date": run_date,
        "group_id": group_id,
        "node_id": _stable_id(
            "N",
            group_id,
            node_key,
        ),
        "node_key": node_key,
        "node_type": "COUNTERPARTY",
        "entity_type": None,
        "entity_id": None,
        "entity_key": None,
        "counterparty_key": counterparty_key,
        "display_label": (
            display_label
            or counterparty_key
        ),
        "node_roles": (
            "EXTERNAL_COUNTERPARTY_CANDIDATE"
        ),
        "node_status": (
            "OBSERVED_PENDING_COUNTERPARTY_AI"
        ),
        "customer_assessment_status": (
            "NOT_APPLICABLE"
        ),
        "customer_discovery_allowed_flag": False,
        "expansion_source_flag": False,
        "first_seen_date": run_date,
        "last_seen_date": run_date,
    }

    return pd.concat(
        [
            nodes,
            pd.DataFrame([node]),
        ],
        ignore_index=True,
    )


def _append_edge(
    edge_rows: list[dict[str, Any]],
    *,
    run_id: str,
    run_date: date,
    group_id: str,
    source_node_key: str,
    target_node_key: str,
    edge_type: str,
    relationship_status: str,
    discovery_allowed: bool,
    evidence_key: str,
    evidence_summary: str,
    source_event_count: int | None,
    candidate_event_count: int | None,
) -> None:
    """Append one stable evidence edge."""
    edge_rows.append(
        {
            "run_id": run_id,
            "run_date": run_date,
            "group_id": group_id,
            "edge_id": _stable_id(
                "E",
                group_id,
                edge_type,
                source_node_key,
                target_node_key,
                evidence_key,
            ),
            "source_node_key": source_node_key,
            "target_node_key": target_node_key,
            "edge_type": edge_type,
            "relationship_status": (
                relationship_status
            ),
            "customer_discovery_allowed_flag": (
                discovery_allowed
            ),
            "recursive_expansion_allowed_flag": False,
            "evidence_key": evidence_key,
            "evidence_summary": evidence_summary,
            "source_event_count": source_event_count,
            "candidate_event_count": (
                candidate_event_count
            ),
            "first_seen_date": run_date,
            "last_seen_date": run_date,
        }
    )


def _refresh_group_structure(
    graph: UnifiedGroupResult,
) -> UnifiedGroupResult:
    """Recalculate structural group counts."""
    groups = graph.groups.copy()
    nodes = graph.nodes.copy()
    edges = graph.edges.copy()

    for index, row in groups.iterrows():
        group_id = row["group_id"]

        group_nodes = nodes.loc[
            nodes["group_id"].eq(group_id)
        ]

        group_edges = edges.loc[
            edges["group_id"].eq(group_id)
        ]

        customer_nodes = group_nodes.loc[
            group_nodes["node_type"].eq("CUSTOMER")
        ]

        counterparty_nodes = group_nodes.loc[
            group_nodes["node_type"].eq(
                "COUNTERPARTY"
            )
        ]

        groups.at[
            index,
            "customer_count",
        ] = customer_nodes["entity_key"].nunique()

        groups.at[
            index,
            "counterparty_count",
        ] = counterparty_nodes[
            "counterparty_key"
        ].nunique()

        groups.at[
            index,
            "eid_link_count",
        ] = int(
            group_edges["edge_type"]
            .eq("SAME_EMIRATES_ID")
            .sum()
        )

        groups.at[
            index,
            "counterparty_candidate_count",
        ] = int(
            group_edges["edge_type"]
            .isin(
                [
                    "SEED_COUNTERPARTY_EVIDENCE",
                    "CUSTOMER_COUNTERPARTY_EVIDENCE",
                ]
            )
            .sum()
        )

        groups.at[
            index,
            "shared_counterparty_customer_count",
        ] = int(
            group_edges["edge_type"]
            .eq(
                "SHARED_EXTERNAL_COUNTERPARTY"
            )
            .sum()
        )

        groups.at[
            index,
            "beneficiary_seed_link_count",
        ] = int(
            group_edges["edge_type"]
            .isin(
                [
                    "BENEFICIARY_ADDED_SEED_ACCOUNT",
                    "BENEFICIARY_ADDED_MULE_ACCOUNT",
                ]
            )
            .sum()
        )

        groups.at[
            index,
            "customer_assessment_pending_count",
        ] = int(
            customer_nodes[
                "customer_assessment_status"
            ]
            .eq("PENDING_CUSTOMER_AI")
            .sum()
        )

        groups.at[
            index,
            "counterparty_ai_pending_count",
        ] = int(
            counterparty_nodes["node_status"]
            .eq(
                "OBSERVED_PENDING_COUNTERPARTY_AI"
            )
            .sum()
        )

        groups.at[
            index,
            "recursive_expansion_source_count",
        ] = int(
            group_nodes[
                "expansion_source_flag"
            ].sum()
        )

        groups.at[
            index,
            "total_node_count",
        ] = len(group_nodes)

        groups.at[
            index,
            "total_edge_count",
        ] = len(group_edges)

        groups.at[
            index,
            "last_seen_date",
        ] = row["run_date"]

    return UnifiedGroupResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
    )


def merge_expansion_relationships(
    graph: UnifiedGroupResult,
    relationships: pd.DataFrame,
    group_ids: list[str],
    run_date: date | str,
) -> UnifiedGroupResult:
    """Attach one expansion source's prepared relationships."""
    resolved_run_date = parse_run_date(run_date)

    groups = graph.groups.copy()
    nodes = graph.nodes.copy()
    edges = graph.edges.copy()

    new_edges: list[dict[str, Any]] = []

    for group_id in group_ids:
        group_rows = groups.loc[
            groups["group_id"].eq(group_id)
        ]

        if group_rows.empty:
            raise ValueError(
                f"Unknown group ID: {group_id}"
            )

        run_id = str(
            group_rows.iloc[0]["run_id"]
        )

        for relationship in relationships.itertuples(
            index=False
        ):
            source_node_key = (
                f"CUSTOMER|"
                f"{relationship.source_entity_key}"
            )

            source_exists = (
                nodes["group_id"].eq(group_id)
                & nodes["node_key"].eq(
                    source_node_key
                )
            ).any()

            if not source_exists:
                raise ValueError(
                    "Expansion source is not present "
                    f"in group {group_id}: "
                    f"{relationship.source_entity_key}"
                )

            target_node_key = (
                f"CUSTOMER|"
                f"{relationship.target_entity_key}"
            )

            if (
                relationship.relationship_type
                == "SAME_EMIRATES_ID"
            ):
                nodes = _upsert_customer_node(
                    nodes,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    entity_type=(
                        relationship.target_entity_type
                    ),
                    entity_id=(
                        relationship.target_entity_id
                    ),
                    entity_key=(
                        relationship.target_entity_key
                    ),
                    role="EID_LINKED_CUSTOMER",
                    node_status=(
                        "INCLUDED_DETERMINISTIC"
                    ),
                    assessment_status=(
                        "PENDING_CUSTOMER_AI"
                    ),
                    discovery_allowed=True,
                )

                _append_edge(
                    new_edges,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    source_node_key=source_node_key,
                    target_node_key=target_node_key,
                    edge_type="SAME_EMIRATES_ID",
                    relationship_status=(
                        "DETERMINISTIC_LINK"
                    ),
                    discovery_allowed=True,
                    evidence_key=(
                        relationship.evidence_key
                    ),
                    evidence_summary=(
                        relationship.evidence_summary
                    ),
                    source_event_count=None,
                    candidate_event_count=None,
                )

            elif (
                relationship.relationship_type
                == "SHARED_EXTERNAL_COUNTERPARTY"
            ):
                counterparty_node_key = (
                    "COUNTERPARTY|"
                    f"{relationship.counterparty_key}"
                )

                nodes = _upsert_counterparty_node(
                    nodes,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    counterparty_key=(
                        relationship.counterparty_key
                    ),
                    display_label=(
                        relationship.counterparty_name
                    ),
                )

                nodes = _upsert_customer_node(
                    nodes,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    entity_type=(
                        relationship.target_entity_type
                    ),
                    entity_id=(
                        relationship.target_entity_id
                    ),
                    entity_key=(
                        relationship.target_entity_key
                    ),
                    role=(
                        "COUNTERPARTY_LINKED_CUSTOMER"
                    ),
                    node_status=(
                        "OBSERVED_PENDING_COUNTERPARTY_AI"
                    ),
                    assessment_status=(
                        "BLOCKED_PENDING_COUNTERPARTY_AI"
                    ),
                    discovery_allowed=False,
                )

                _append_edge(
                    new_edges,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    source_node_key=source_node_key,
                    target_node_key=(
                        counterparty_node_key
                    ),
                    edge_type=(
                        "CUSTOMER_COUNTERPARTY_EVIDENCE"
                    ),
                    relationship_status=(
                        "COUNTERPARTY_CANDIDATE"
                    ),
                    discovery_allowed=False,
                    evidence_key=(
                        f"{relationship.evidence_key}"
                        "|SOURCE"
                    ),
                    evidence_summary=(
                        relationship.evidence_summary
                    ),
                    source_event_count=(
                        _optional_integer(
                            relationship.source_event_count
                        )
                    ),
                    candidate_event_count=(
                        _optional_integer(
                            relationship.candidate_event_count
                        )
                    ),
                )

                _append_edge(
                    new_edges,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    source_node_key=(
                        counterparty_node_key
                    ),
                    target_node_key=target_node_key,
                    edge_type=(
                        "SHARED_EXTERNAL_COUNTERPARTY"
                    ),
                    relationship_status=(
                        "COUNTERPARTY_CANDIDATE"
                    ),
                    discovery_allowed=False,
                    evidence_key=(
                        f"{relationship.evidence_key}"
                        "|TARGET"
                    ),
                    evidence_summary=(
                        relationship.evidence_summary
                    ),
                    source_event_count=(
                        _optional_integer(
                            relationship.source_event_count
                        )
                    ),
                    candidate_event_count=(
                        _optional_integer(
                            relationship.candidate_event_count
                        )
                    ),
                )

            elif (
                relationship.relationship_type
                == "BENEFICIARY_ADDED_MULE_ACCOUNT"
            ):
                nodes = _upsert_customer_node(
                    nodes,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    entity_type=(
                        relationship.target_entity_type
                    ),
                    entity_id=(
                        relationship.target_entity_id
                    ),
                    entity_key=(
                        relationship.target_entity_key
                    ),
                    role=(
                        "BENEFICIARY_TO_MULE_CUSTOMER"
                    ),
                    node_status=(
                        "INCLUDED_FOR_CUSTOMER_ASSESSMENT"
                    ),
                    assessment_status=(
                        "PENDING_CUSTOMER_AI"
                    ),
                    discovery_allowed=True,
                )

                _append_edge(
                    new_edges,
                    run_id=run_id,
                    run_date=resolved_run_date,
                    group_id=group_id,
                    source_node_key=target_node_key,
                    target_node_key=source_node_key,
                    edge_type=(
                        "BENEFICIARY_ADDED_MULE_ACCOUNT"
                    ),
                    relationship_status=(
                        "DETERMINISTIC_BENEFICIARY_TO_MULE"
                    ),
                    discovery_allowed=True,
                    evidence_key=(
                        relationship.evidence_key
                    ),
                    evidence_summary=(
                        relationship.evidence_summary
                    ),
                    source_event_count=1,
                    candidate_event_count=1,
                )

    if new_edges:
        edges = pd.concat(
            [
                edges,
                pd.DataFrame(new_edges),
            ],
            ignore_index=True,
        )

    nodes = (
        nodes
        .drop_duplicates(
            subset=[
                "group_id",
                "node_key",
            ],
            keep="first",
        )
        .reset_index(drop=True)
    )

    node_id_map = {
        (
            row.group_id,
            row.node_key,
        ): row.node_id
        for row in nodes.itertuples(index=False)
    }

    edges = (
        edges
        .drop_duplicates(
            subset=["edge_id"],
            keep="first",
        )
        .copy()
    )

    edges["source_node_id"] = edges.apply(
        lambda row: node_id_map[
            (
                row["group_id"],
                row["source_node_key"],
            )
        ],
        axis=1,
    )

    edges["target_node_id"] = edges.apply(
        lambda row: node_id_map[
            (
                row["group_id"],
                row["target_node_key"],
            )
        ],
        axis=1,
    )

    nodes = (
        nodes
        .sort_values(
            by=[
                "group_id",
                "node_type",
                "node_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    edges = (
        edges
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

    return _refresh_group_structure(
        UnifiedGroupResult(
            groups=groups,
            nodes=nodes,
            edges=edges,
        )
    )


def _filter_completed_discovery_actions(
    queue: pd.DataFrame,
    processed_subjects: set[str],
) -> pd.DataFrame:
    """Remove discovery work already completed this run."""
    if queue.empty:
        return queue.copy()

    completed_mask = (
        queue["action_type"].eq(
            DISCOVERY_ACTION_TYPE
        )
        & queue["subject_key"].isin(
            processed_subjects
        )
    )

    return (
        queue.loc[~completed_mask]
        .copy()
        .reset_index(drop=True)
    )


def _update_group_queue_counts(
    groups: pd.DataFrame,
    queue: pd.DataFrame,
) -> pd.DataFrame:
    """Update each group's remaining queue count."""
    updated = groups.copy()

    for index, row in updated.iterrows():
        group_id = row["group_id"]

        if queue.empty:
            count = 0
        else:
            count = int(
                queue["group_ids"]
                .map(
                    lambda value: (
                        group_id
                        in str(value).split("|")
                    )
                )
                .sum()
            )

        updated.at[
            index,
            "queued_action_count",
        ] = count

    return updated


def run_recursive_expansion(
    *,
    data_source: CounterpartyNetworkDataSource,
    initial_decisions: pd.DataFrame,
    evidence_source: PreparedExpansionEvidenceSource,
    decision_adapter: DeterministicDemoDecisionAdapter,
    run_date: date | str,
    output_directory: Path | str,
    guardrails: RecursiveGuardrails = (
        RecursiveGuardrails()
    ),
    persist_outputs: bool = True,
) -> RecursiveExpansionResult:
    """Run bounded discovery and decision rounds."""
    resolved_run_date = parse_run_date(run_date)

    evidence_graph = run_unified_group_projection(
        data_source=data_source,
        run_date=resolved_run_date,
        output_directory=output_directory,
        persist_outputs=False,
    )

    decision_history = initial_decisions.copy()

    processed_expansion_subjects: set[str] = set()
    generated_decision_rows: list[dict[str, str]] = []
    expansion_ledger_rows: list[dict[str, Any]] = []
    round_rows: list[dict[str, Any]] = []

    ai_call_count = 0
    expansion_count = 0
    termination_reason = "MAX_ROUNDS_REACHED"

    for round_number in range(
        1,
        guardrails.max_rounds + 1,
    ):
        projection = apply_persisted_decisions(
            unified_result=evidence_graph,
            decisions=decision_history,
            run_date=resolved_run_date,
        )

        queue = _filter_completed_discovery_actions(
            queue=projection.expansion_queue,
            processed_subjects=(
                processed_expansion_subjects
            ),
        )

        ai_queue = queue.loc[
            queue["action_type"].isin(
                AI_ACTION_TYPES
            )
        ].copy()

        discovery_queue = queue.loc[
            queue["action_type"].eq(
                DISCOVERY_ACTION_TYPE
            )
        ].copy()

        if ai_queue.empty and discovery_queue.empty:
            termination_reason = "FRONTIER_EMPTY"

            round_rows.append(
                {
                    "round_number": round_number,
                    "queued_ai_actions": 0,
                    "queued_discovery_actions": 0,
                    "executed_ai_actions": 0,
                    "executed_discovery_actions": 0,
                    "new_nodes": 0,
                    "new_edges": 0,
                    "total_ai_calls": ai_call_count,
                    "total_customer_expansions": (
                        expansion_count
                    ),
                    "termination_reason": (
                        termination_reason
                    ),
                }
            )

            break

        available_ai_calls = (
            guardrails.max_ai_calls
            - ai_call_count
        )

        selected_ai_queue = ai_queue.head(
            max(available_ai_calls, 0)
        )

        executed_ai_actions = 0

        for sequence_number, row in enumerate(
            selected_ai_queue.itertuples(
                index=False
            ),
            start=1,
        ):
            decision = decision_adapter.decide(
                subject_type=row.subject_type,
                subject_key=row.subject_key,
                feature_snapshot_hash=(
                    row.feature_snapshot_hash
                ),
                run_date=resolved_run_date,
                round_number=round_number,
                sequence_number=sequence_number,
            )

            generated_decision_rows.append(
                decision
            )

            decision_history = pd.concat(
                [
                    decision_history,
                    pd.DataFrame([decision]),
                ],
                ignore_index=True,
            )

            ai_call_count += 1
            executed_ai_actions += 1

        available_expansions = (
            guardrails.max_customer_expansions
            - expansion_count
        )

        selected_discovery_queue = (
            discovery_queue.head(
                max(available_expansions, 0)
            )
        )

        before_node_count = len(
            evidence_graph.nodes
        )

        before_edge_count = len(
            evidence_graph.edges
        )

        executed_discovery_actions = 0
        guardrail_termination: str | None = None

        for row in selected_discovery_queue.itertuples(
            index=False
        ):
            relationships = (
                evidence_source.get_relationships(
                    run_date=resolved_run_date,
                    source_entity_key=(
                        row.subject_key
                    ),
                )
            )

            group_ids = [
                group_id
                for group_id in str(
                    row.group_ids
                ).split("|")
                if group_id
            ]

            candidate_graph = (
                merge_expansion_relationships(
                    graph=evidence_graph,
                    relationships=relationships,
                    group_ids=group_ids,
                    run_date=resolved_run_date,
                )
            )

            if (
                len(candidate_graph.nodes)
                > guardrails.max_total_nodes
            ):
                guardrail_termination = (
                    "NODE_LIMIT_REACHED"
                )

                break

            if (
                len(candidate_graph.edges)
                > guardrails.max_total_edges
            ):
                guardrail_termination = (
                    "EDGE_LIMIT_REACHED"
                )

                break

            evidence_graph = candidate_graph

            processed_expansion_subjects.add(
                row.subject_key
            )

            expansion_count += 1
            executed_discovery_actions += 1

            expansion_ledger_rows.append(
                {
                    "run_date": resolved_run_date,
                    "round_number": round_number,
                    "queue_item_id": row.queue_item_id,
                    "source_entity_key": (
                        row.subject_key
                    ),
                    "group_ids": row.group_ids,
                    "relationship_rows_found": len(
                        relationships
                    ),
                    "expansion_status": "COMPLETED",
                }
            )

        new_node_count = (
            len(evidence_graph.nodes)
            - before_node_count
        )

        new_edge_count = (
            len(evidence_graph.edges)
            - before_edge_count
        )

        if guardrail_termination is not None:
            termination_reason = (
                guardrail_termination
            )

        elif (
            len(ai_queue)
            > executed_ai_actions
        ):
            termination_reason = (
                "AI_CALL_BUDGET_REACHED"
            )

        elif (
            len(discovery_queue)
            > executed_discovery_actions
        ):
            termination_reason = (
                "CUSTOMER_EXPANSION_LIMIT_REACHED"
            )

        else:
            termination_reason = ""

        round_rows.append(
            {
                "round_number": round_number,
                "queued_ai_actions": len(
                    ai_queue
                ),
                "queued_discovery_actions": len(
                    discovery_queue
                ),
                "executed_ai_actions": (
                    executed_ai_actions
                ),
                "executed_discovery_actions": (
                    executed_discovery_actions
                ),
                "new_nodes": new_node_count,
                "new_edges": new_edge_count,
                "total_ai_calls": ai_call_count,
                "total_customer_expansions": (
                    expansion_count
                ),
                "termination_reason": (
                    termination_reason
                ),
            }
        )

        if termination_reason:
            break

    final_projection: DecisionProjectionResult = (
        apply_persisted_decisions(
            unified_result=evidence_graph,
            decisions=decision_history,
            run_date=resolved_run_date,
        )
    )

    remaining_queue = (
        _filter_completed_discovery_actions(
            queue=final_projection.expansion_queue,
            processed_subjects=(
                processed_expansion_subjects
            ),
        )
    )

    final_groups = _update_group_queue_counts(
        groups=final_projection.groups,
        queue=remaining_queue,
    )

    generated_decisions = pd.DataFrame(
        generated_decision_rows,
        columns=DECISION_REQUIRED_COLUMNS,
    )

    expansion_ledger = pd.DataFrame(
        expansion_ledger_rows,
        columns=[
            "run_date",
            "round_number",
            "queue_item_id",
            "source_entity_key",
            "group_ids",
            "relationship_rows_found",
            "expansion_status",
        ],
    )

    round_log = pd.DataFrame(
        round_rows
    )

    result = RecursiveExpansionResult(
        groups=final_groups,
        nodes=final_projection.nodes,
        edges=final_projection.edges,
        decision_history=decision_history,
        generated_decisions=generated_decisions,
        remaining_queue=remaining_queue,
        round_log=round_log,
        expansion_ledger=expansion_ledger,
        termination_reason=termination_reason,
    )

    if persist_outputs:
        resolved_output_directory = Path(
            output_directory
        )

        resolved_output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_frames = {
            "recursive_groups.csv": (
                result.groups
            ),
            "recursive_group_nodes.csv": (
                result.nodes
            ),
            "recursive_group_edges.csv": (
                result.edges
            ),
            "recursive_decision_history.csv": (
                result.decision_history
            ),
            "recursive_generated_decisions.csv": (
                result.generated_decisions
            ),
            "recursive_remaining_queue.csv": (
                result.remaining_queue
            ),
            "recursive_round_log.csv": (
                result.round_log
            ),
            "recursive_expansion_ledger.csv": (
                result.expansion_ledger
            ),
        }

        for filename, frame in output_frames.items():
            frame.to_csv(
                resolved_output_directory
                / filename,
                index=False,
                lineterminator="\n",
            )

    return result
