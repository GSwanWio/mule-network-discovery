"""Build unified seed-led groups from EID and counterparty evidence."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from network_mule_discovery.counterparty_discovery import (
    CounterpartyDiscoveryResult,
)
from network_mule_discovery.eid_discovery import (
    EidDiscoveryResult,
)
from network_mule_discovery.schemas import parse_run_date


GROUP_STATUS_ACTIVE = "ACTIVE"

NODE_STATUS_SEED = "SEED_EXPANSION_SOURCE"
NODE_STATUS_EID = "INCLUDED_DETERMINISTIC"
NODE_STATUS_ASSESSMENT = "INCLUDED_FOR_CUSTOMER_ASSESSMENT"
NODE_STATUS_COUNTERPARTY_PENDING = "OBSERVED_PENDING_COUNTERPARTY_AI"

ASSESSMENT_STATUS_SEED = "SEED_CONFIRMED"
ASSESSMENT_STATUS_PENDING = "PENDING_CUSTOMER_AI"
ASSESSMENT_STATUS_BLOCKED = "BLOCKED_PENDING_COUNTERPARTY_AI"
ASSESSMENT_STATUS_NOT_APPLICABLE = "NOT_APPLICABLE"

RELATIONSHIP_STATUS_EID = "DETERMINISTIC_LINK"
RELATIONSHIP_STATUS_BENEFICIARY = (
    "DETERMINISTIC_BENEFICIARY_TO_SEED"
)
RELATIONSHIP_STATUS_COUNTERPARTY = (
    "COUNTERPARTY_CANDIDATE"
)


@dataclass(frozen=True)
class UnifiedGroupResult:
    """Persistable unified group projection."""

    groups: pd.DataFrame
    nodes: pd.DataFrame
    edges: pd.DataFrame


_NODE_STATUS_RANK = {
    NODE_STATUS_COUNTERPARTY_PENDING: 1,
    NODE_STATUS_ASSESSMENT: 2,
    NODE_STATUS_EID: 3,
    NODE_STATUS_SEED: 4,
}

_ASSESSMENT_STATUS_RANK = {
    ASSESSMENT_STATUS_NOT_APPLICABLE: 1,
    ASSESSMENT_STATUS_BLOCKED: 2,
    ASSESSMENT_STATUS_PENDING: 3,
    ASSESSMENT_STATUS_SEED: 4,
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


def _clean_text(value: object) -> str | None:
    """Return a nonblank string or None."""
    if value is None or pd.isna(value):
        return None

    cleaned = str(value).strip()

    return cleaned or None


def _max_status(
    current_status: str,
    new_status: str,
    ranks: dict[str, int],
) -> str:
    """Return the higher-priority status."""
    if ranks[new_status] > ranks[current_status]:
        return new_status

    return current_status


def build_unified_seed_groups(
    eid_discovery: EidDiscoveryResult,
    counterparty_discovery: CounterpartyDiscoveryResult,
    run_date: date | str,
    *,
    assess_eid_linked_customers: bool = True,
) -> UnifiedGroupResult:
    """
    Combine deterministic and candidate evidence into seed-led groups.

    Section 2 does not recursively expand through any customer or
    counterparty. Only seed entities are expansion sources.
    """
    resolved_run_date = parse_run_date(run_date)
    run_id = f"unified_{resolved_run_date:%Y%m%d}"

    seed_entities = (
        eid_discovery.seed_resolution.seed_entities
        .sort_values(
            by=[
                "entity_key",
                "seed_customer_id",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=["entity_key"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    group_id_by_seed: dict[str, str] = {}

    for row in seed_entities.itertuples(index=False):
        group_id_by_seed[row.entity_key] = _stable_id(
            "G",
            "SEED_GROUP",
            row.entity_key,
        )

    node_registry: dict[
        tuple[str, str],
        dict[str, Any],
    ] = {}

    edge_rows: list[dict[str, Any]] = []

    def upsert_node(
        *,
        group_id: str,
        node_key: str,
        node_type: str,
        node_role: str,
        node_status: str,
        customer_assessment_status: str,
        customer_discovery_allowed_flag: bool,
        expansion_source_flag: bool,
        entity_type: object = None,
        entity_id: object = None,
        entity_key: object = None,
        counterparty_key: object = None,
        display_label: object = None,
    ) -> None:
        registry_key = (
            group_id,
            node_key,
        )

        if registry_key not in node_registry:
            node_registry[registry_key] = {
                "run_id": run_id,
                "run_date": resolved_run_date,
                "group_id": group_id,
                "node_id": _stable_id(
                    "N",
                    group_id,
                    node_key,
                ),
                "node_key": node_key,
                "node_type": node_type,
                "entity_type": _clean_text(entity_type),
                "entity_id": _clean_text(entity_id),
                "entity_key": _clean_text(entity_key),
                "counterparty_key": _clean_text(
                    counterparty_key
                ),
                "display_label": (
                    _clean_text(display_label)
                    or node_key
                ),
                "node_roles": {node_role},
                "node_status": node_status,
                "customer_assessment_status": (
                    customer_assessment_status
                ),
                "customer_discovery_allowed_flag": (
                    customer_discovery_allowed_flag
                ),
                "expansion_source_flag": (
                    expansion_source_flag
                ),
                "first_seen_date": resolved_run_date,
                "last_seen_date": resolved_run_date,
            }

            return

        existing = node_registry[registry_key]

        existing["node_roles"].add(node_role)

        existing["node_status"] = _max_status(
            current_status=existing["node_status"],
            new_status=node_status,
            ranks=_NODE_STATUS_RANK,
        )

        existing["customer_assessment_status"] = (
            _max_status(
                current_status=existing[
                    "customer_assessment_status"
                ],
                new_status=customer_assessment_status,
                ranks=_ASSESSMENT_STATUS_RANK,
            )
        )

        existing[
            "customer_discovery_allowed_flag"
        ] = bool(
            existing[
                "customer_discovery_allowed_flag"
            ]
            or customer_discovery_allowed_flag
        )

        existing["expansion_source_flag"] = bool(
            existing["expansion_source_flag"]
            or expansion_source_flag
        )

        for field_name, field_value in (
            ("entity_type", entity_type),
            ("entity_id", entity_id),
            ("entity_key", entity_key),
            ("counterparty_key", counterparty_key),
            ("display_label", display_label),
        ):
            if existing[field_name] is None:
                existing[field_name] = _clean_text(
                    field_value
                )

    def append_edge(
        *,
        group_id: str,
        source_node_key: str,
        target_node_key: str,
        edge_type: str,
        relationship_status: str,
        customer_discovery_allowed_flag: bool,
        recursive_expansion_allowed_flag: bool,
        evidence_key: object,
        evidence_summary: object,
        source_event_count: object = None,
        candidate_event_count: object = None,
    ) -> None:
        edge_id = _stable_id(
            "E",
            group_id,
            edge_type,
            source_node_key,
            target_node_key,
            evidence_key,
        )

        edge_rows.append(
            {
                "run_id": run_id,
                "run_date": resolved_run_date,
                "group_id": group_id,
                "edge_id": edge_id,
                "source_node_key": source_node_key,
                "target_node_key": target_node_key,
                "edge_type": edge_type,
                "relationship_status": (
                    relationship_status
                ),
                "customer_discovery_allowed_flag": (
                    customer_discovery_allowed_flag
                ),
                "recursive_expansion_allowed_flag": (
                    recursive_expansion_allowed_flag
                ),
                "evidence_key": _clean_text(
                    evidence_key
                ),
                "evidence_summary": _clean_text(
                    evidence_summary
                ),
                "source_event_count": (
                    source_event_count
                ),
                "candidate_event_count": (
                    candidate_event_count
                ),
                "first_seen_date": resolved_run_date,
                "last_seen_date": resolved_run_date,
            }
        )

    for row in seed_entities.itertuples(index=False):
        group_id = group_id_by_seed[row.entity_key]

        upsert_node(
            group_id=group_id,
            node_key=f"CUSTOMER|{row.entity_key}",
            node_type="CUSTOMER",
            node_role="SEED_MULE",
            node_status=NODE_STATUS_SEED,
            customer_assessment_status=(
                ASSESSMENT_STATUS_SEED
            ),
            customer_discovery_allowed_flag=True,
            expansion_source_flag=True,
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            entity_key=row.entity_key,
            display_label=row.entity_key,
        )

    for row in eid_discovery.eid_links.itertuples(
        index=False
    ):
        group_id = group_id_by_seed.get(
            row.seed_entity_key
        )

        if group_id is None:
            raise ValueError(
                "Missing group for EID seed entity: "
                f"{row.seed_entity_key}"
            )

        source_node_key = (
            f"CUSTOMER|{row.seed_entity_key}"
        )

        target_node_key = (
            f"CUSTOMER|{row.candidate_entity_key}"
        )

        upsert_node(
            group_id=group_id,
            node_key=target_node_key,
            node_type="CUSTOMER",
            node_role="EID_LINKED_CUSTOMER",
            node_status=NODE_STATUS_EID,
            customer_assessment_status=(
                ASSESSMENT_STATUS_PENDING
                if assess_eid_linked_customers
                else ASSESSMENT_STATUS_NOT_APPLICABLE
            ),
            customer_discovery_allowed_flag=(
                assess_eid_linked_customers
            ),
            expansion_source_flag=False,
            entity_type=row.candidate_entity_type,
            entity_id=row.candidate_entity_id,
            entity_key=row.candidate_entity_key,
            display_label=row.candidate_entity_key,
        )

        append_edge(
            group_id=group_id,
            source_node_key=source_node_key,
            target_node_key=target_node_key,
            edge_type="SAME_EMIRATES_ID",
            relationship_status=(
                RELATIONSHIP_STATUS_EID
            ),
            customer_discovery_allowed_flag=(
                assess_eid_linked_customers
            ),
            recursive_expansion_allowed_flag=False,
            evidence_key=row.emirates_id_number,
            evidence_summary=row.reason_code,
        )

    for row in (
        counterparty_discovery
        .candidate_counterparties
        .itertuples(index=False)
    ):
        group_id = group_id_by_seed.get(
            row.seed_entity_key
        )

        if group_id is None:
            raise ValueError(
                "Missing group for counterparty seed: "
                f"{row.seed_entity_key}"
            )

        seed_node_key = (
            f"CUSTOMER|{row.seed_entity_key}"
        )

        counterparty_node_key = (
            f"COUNTERPARTY|{row.counterparty_key}"
        )

        upsert_node(
            group_id=group_id,
            node_key=counterparty_node_key,
            node_type="COUNTERPARTY",
            node_role="EXTERNAL_COUNTERPARTY_CANDIDATE",
            node_status=(
                NODE_STATUS_COUNTERPARTY_PENDING
            ),
            customer_assessment_status=(
                ASSESSMENT_STATUS_NOT_APPLICABLE
            ),
            customer_discovery_allowed_flag=False,
            expansion_source_flag=False,
            counterparty_key=row.counterparty_key,
            display_label=(
                row.counterparty_names
                or row.counterparty_key
            ),
        )

        append_edge(
            group_id=group_id,
            source_node_key=seed_node_key,
            target_node_key=counterparty_node_key,
            edge_type="SEED_COUNTERPARTY_EVIDENCE",
            relationship_status=(
                RELATIONSHIP_STATUS_COUNTERPARTY
            ),
            customer_discovery_allowed_flag=False,
            recursive_expansion_allowed_flag=False,
            evidence_key=row.counterparty_key,
            evidence_summary=(
                "Seed transfer evidence before "
                "the FRC cutoff"
            ),
            source_event_count=row.seed_event_count,
            candidate_event_count=(
                row.candidate_event_count
            ),
        )

    for row in (
        counterparty_discovery
        .candidate_customer_links
        .itertuples(index=False)
    ):
        group_id = group_id_by_seed.get(
            row.seed_entity_key
        )

        if group_id is None:
            raise ValueError(
                "Missing group for candidate link seed: "
                f"{row.seed_entity_key}"
            )

        counterparty_node_key = (
            f"COUNTERPARTY|{row.counterparty_key}"
        )

        candidate_node_key = (
            f"CUSTOMER|{row.candidate_entity_key}"
        )

        upsert_node(
            group_id=group_id,
            node_key=candidate_node_key,
            node_type="CUSTOMER",
            node_role=(
                "COUNTERPARTY_LINKED_CUSTOMER"
            ),
            node_status=(
                NODE_STATUS_COUNTERPARTY_PENDING
            ),
            customer_assessment_status=(
                ASSESSMENT_STATUS_BLOCKED
            ),
            customer_discovery_allowed_flag=False,
            expansion_source_flag=False,
            entity_type=row.candidate_entity_type,
            entity_id=row.candidate_entity_id,
            entity_key=row.candidate_entity_key,
            display_label=row.candidate_entity_key,
        )

        append_edge(
            group_id=group_id,
            source_node_key=counterparty_node_key,
            target_node_key=candidate_node_key,
            edge_type="SHARED_EXTERNAL_COUNTERPARTY",
            relationship_status=(
                RELATIONSHIP_STATUS_COUNTERPARTY
            ),
            customer_discovery_allowed_flag=False,
            recursive_expansion_allowed_flag=False,
            evidence_key=row.relationship_id,
            evidence_summary=(
                row.candidate_event_types
            ),
            source_event_count=row.seed_event_count,
            candidate_event_count=(
                row.candidate_event_count
            ),
        )

    for row in (
        counterparty_discovery
        .beneficiary_seed_links
        .itertuples(index=False)
    ):
        group_id = group_id_by_seed.get(
            row.seed_entity_key
        )

        if group_id is None:
            raise ValueError(
                "Missing group for beneficiary seed: "
                f"{row.seed_entity_key}"
            )

        candidate_node_key = (
            f"CUSTOMER|{row.candidate_entity_key}"
        )

        seed_node_key = (
            f"CUSTOMER|{row.seed_entity_key}"
        )

        upsert_node(
            group_id=group_id,
            node_key=candidate_node_key,
            node_type="CUSTOMER",
            node_role=(
                "BENEFICIARY_TO_SEED_CUSTOMER"
            ),
            node_status=NODE_STATUS_ASSESSMENT,
            customer_assessment_status=(
                ASSESSMENT_STATUS_PENDING
            ),
            customer_discovery_allowed_flag=True,
            expansion_source_flag=False,
            entity_type=row.candidate_entity_type,
            entity_id=row.candidate_entity_id,
            entity_key=row.candidate_entity_key,
            display_label=row.candidate_entity_key,
        )

        append_edge(
            group_id=group_id,
            source_node_key=candidate_node_key,
            target_node_key=seed_node_key,
            edge_type=(
                "BENEFICIARY_ADDED_SEED_ACCOUNT"
            ),
            relationship_status=(
                RELATIONSHIP_STATUS_BENEFICIARY
            ),
            customer_discovery_allowed_flag=True,
            recursive_expansion_allowed_flag=False,
            evidence_key=row.relationship_id,
            evidence_summary=(
                "Customer added a known seed mule "
                "account as beneficiary"
            ),
            source_event_count=1,
            candidate_event_count=1,
        )

    node_rows: list[dict[str, Any]] = []

    for node in node_registry.values():
        serialized_node = node.copy()

        serialized_node["node_roles"] = "|".join(
            sorted(serialized_node["node_roles"])
        )

        node_rows.append(serialized_node)

    nodes = pd.DataFrame(node_rows)

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

    node_id_by_group_and_key = {
        (
            row.group_id,
            row.node_key,
        ): row.node_id
        for row in nodes.itertuples(index=False)
    }

    edges = pd.DataFrame(edge_rows)

    edges = (
        edges
        .drop_duplicates(
            subset=["edge_id"],
            keep="first",
        )
        .copy()
    )

    edges["source_node_id"] = edges.apply(
        lambda row: node_id_by_group_and_key[
            (
                row["group_id"],
                row["source_node_key"],
            )
        ],
        axis=1,
    )

    edges["target_node_id"] = edges.apply(
        lambda row: node_id_by_group_and_key[
            (
                row["group_id"],
                row["target_node_key"],
            )
        ],
        axis=1,
    )

    edge_columns = [
        "run_id",
        "run_date",
        "group_id",
        "edge_id",
        "source_node_id",
        "target_node_id",
        "source_node_key",
        "target_node_key",
        "edge_type",
        "relationship_status",
        "customer_discovery_allowed_flag",
        "recursive_expansion_allowed_flag",
        "evidence_key",
        "evidence_summary",
        "source_event_count",
        "candidate_event_count",
        "first_seen_date",
        "last_seen_date",
    ]

    edges = (
        edges[edge_columns]
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

    group_rows: list[dict[str, Any]] = []

    for seed_row in seed_entities.itertuples(
        index=False
    ):
        group_id = group_id_by_seed[
            seed_row.entity_key
        ]

        group_nodes = nodes.loc[
            nodes["group_id"] == group_id
        ]

        group_edges = edges.loc[
            edges["group_id"] == group_id
        ]

        customer_nodes = group_nodes.loc[
            group_nodes["node_type"] == "CUSTOMER"
        ]

        counterparty_nodes = group_nodes.loc[
            group_nodes["node_type"]
            == "COUNTERPARTY"
        ]

        group_rows.append(
            {
                "run_id": run_id,
                "run_date": resolved_run_date,
                "group_id": group_id,
                "group_anchor_seed_entity_key": (
                    seed_row.entity_key
                ),
                "group_status": GROUP_STATUS_ACTIVE,
                "seed_entity_count": 1,
                "customer_count": (
                    customer_nodes[
                        "entity_key"
                    ].nunique()
                ),
                "counterparty_count": (
                    counterparty_nodes[
                        "counterparty_key"
                    ].nunique()
                ),
                "eid_link_count": int(
                    group_edges["edge_type"]
                    .eq("SAME_EMIRATES_ID")
                    .sum()
                ),
                "counterparty_candidate_count": int(
                    group_edges["edge_type"]
                    .eq(
                        "SEED_COUNTERPARTY_EVIDENCE"
                    )
                    .sum()
                ),
                "shared_counterparty_customer_count": int(
                    group_edges["edge_type"]
                    .eq(
                        "SHARED_EXTERNAL_COUNTERPARTY"
                    )
                    .sum()
                ),
                "beneficiary_seed_link_count": int(
                    group_edges["edge_type"]
                    .eq(
                        "BENEFICIARY_ADDED_SEED_ACCOUNT"
                    )
                    .sum()
                ),
                "customer_assessment_pending_count": int(
                    customer_nodes[
                        "customer_assessment_status"
                    ]
                    .eq(ASSESSMENT_STATUS_PENDING)
                    .sum()
                ),
                "counterparty_ai_pending_count": int(
                    counterparty_nodes[
                        "node_status"
                    ]
                    .eq(
                        NODE_STATUS_COUNTERPARTY_PENDING
                    )
                    .sum()
                ),
                "recursive_expansion_source_count": int(
                    group_nodes[
                        "expansion_source_flag"
                    ].sum()
                ),
                "total_node_count": len(group_nodes),
                "total_edge_count": len(group_edges),
                "first_seen_date": resolved_run_date,
                "last_seen_date": resolved_run_date,
            }
        )

    groups = (
        pd.DataFrame(group_rows)
        .sort_values(
            by=["group_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    return UnifiedGroupResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
    )
