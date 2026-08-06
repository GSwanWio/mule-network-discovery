"""Discover and assess one recursive counterparty frontier."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from network_mule_discovery.behavioral_features import (
    BehavioralFeatureResult,
    build_behavioral_features,
)
from network_mule_discovery.counterparty_schemas import (
    build_counterparty_identity,
)
from network_mule_discovery.daily_ai_runner import (
    ControlledDailyAiRunResult,
    CsvAiCallLedger,
    DailyAiSettings,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    DailyIncrementalPlan,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_ai import (
    CounterpartyFrontierRunResult,
    run_counterparty_ai_frontier,
)
from network_mule_discovery.raw_source_adapter import (
    RawDiscoverySources,
    load_raw_discovery_sources,
)
from network_mule_discovery.recursive_expansion import (
    merge_expansion_relationships,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


DISCOVERY_ACTION_TYPE = "DISCOVER_CUSTOMER_RELATIONSHIPS"
COUNTERPARTY_ACTION_TYPE = "RUN_COUNTERPARTY_AI"


class RecursiveCounterpartyFrontierError(RuntimeError):
    """The recursive counterparty frontier cannot proceed safely."""


@dataclass(frozen=True)
class RecursiveCounterpartyDiscoveryResult:
    """Raw-source relationships found for one approved customer."""

    source_entity_key: str
    source_customer_id: str
    source_group_ids: tuple[str, ...]
    relationships: pd.DataFrame
    counterparty_summary: pd.DataFrame
    new_counterparty_keys: tuple[str, ...]
    skipped_existing_counterparty_keys: tuple[str, ...]
    unshared_counterparty_keys: tuple[str, ...]


@dataclass(frozen=True)
class RecursiveCounterpartyFrontierResult:
    """Persisted state after discovery and counterparty AI."""

    pre_discovery_plan: DailyIncrementalPlan
    discovery: RecursiveCounterpartyDiscoveryResult
    expanded_network: UnifiedGroupResult
    new_features: BehavioralFeatureResult
    controlled_run: ControlledDailyAiRunResult
    decision_store: pd.DataFrame
    ai_call_ledger: pd.DataFrame
    guardrail_telemetry: pd.DataFrame



def _resolve_raw_sources(
    *,
    source_directory: Path | str | None,
    raw_sources: RawDiscoverySources | None,
) -> RawDiscoverySources:
    """Resolve exactly one recursive source interface."""
    if source_directory is None and raw_sources is None:
        raise RecursiveCounterpartyFrontierError(
            "Either source_directory or raw_sources must be provided."
        )

    if source_directory is not None and raw_sources is not None:
        raise RecursiveCounterpartyFrontierError(
            "Provide source_directory or raw_sources, not both."
        )

    if raw_sources is not None:
        if not isinstance(raw_sources, RawDiscoverySources):
            raise RecursiveCounterpartyFrontierError(
                "raw_sources must be RawDiscoverySources."
            )

        return raw_sources

    return load_raw_discovery_sources(source_directory)

def _stable_id(prefix: str, *values: object) -> str:
    canonical = "|".join(str(value) for value in values)
    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()[:16]
    return f"{prefix}{digest}"


def _inclusive_cutoff(run_date: date) -> pd.Timestamp:
    return pd.Timestamp(run_date) + pd.Timedelta(days=1)


def _prepare_identity_lookup(
    identity: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "customer_id",
        "entity_type",
        "entity_id",
    }
    missing = sorted(required - set(identity.columns))

    if missing:
        raise RecursiveCounterpartyFrontierError(
            "customer_identity.csv is missing columns: "
            f"{missing}"
        )

    lookup = identity[
        [
            "customer_id",
            "entity_type",
            "entity_id",
        ]
    ].copy()
    lookup["entity_key"] = (
        lookup["entity_type"].astype("string")
        + "|"
        + lookup["entity_id"].astype("string")
    )

    duplicates = lookup.loc[
        lookup["customer_id"].duplicated(keep=False)
    ]

    if not duplicates.empty:
        raise RecursiveCounterpartyFrontierError(
            "Customer identity resolution is not one-to-one for: "
            f"{sorted(duplicates['customer_id'].unique())}"
        )

    return lookup


def _prepare_outward_events(
    outward: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    required = {
        "transfer_id",
        "status",
        "customer_id",
        "transaction_timestamp",
        "beneficiary_account_number",
        "target_amount",
    }
    missing = sorted(required - set(outward.columns))

    if missing:
        raise RecursiveCounterpartyFrontierError(
            "local_outward_payments.csv is missing columns: "
            f"{missing}"
        )

    prepared = outward.copy()
    prepared["transaction_at"] = pd.to_datetime(
        prepared["transaction_timestamp"],
        errors="coerce",
    )

    if prepared["transaction_at"].isna().any():
        raise RecursiveCounterpartyFrontierError(
            "local_outward_payments.csv contains invalid timestamps."
        )

    prepared = prepared.loc[
        prepared["status"]
        .astype("string")
        .str.upper()
        .eq("COMPLETED")
        & prepared["transaction_at"].lt(
            _inclusive_cutoff(run_date)
        )
    ].copy()

    prepared["amount_value"] = pd.to_numeric(
        prepared["target_amount"],
        errors="coerce",
    )

    if prepared["amount_value"].isna().any():
        raise RecursiveCounterpartyFrontierError(
            "local_outward_payments.target_amount contains "
            "invalid values."
        )

    prepared["counterparty_key"] = prepared[
        "beneficiary_account_number"
    ].map(
        lambda value: build_counterparty_identity(
            rail="LOCAL",
            counterparty_iban="",
            counterparty_swift_bic="",
            counterparty_account_number=value,
        ).counterparty_key
    )

    return prepared


def _counterparty_name_lookup(
    retail_beneficiaries: pd.DataFrame,
    sme_beneficiaries: pd.DataFrame,
) -> dict[str, str]:
    sme = sme_beneficiaries.rename(
        columns={"business_id": "customer_id"}
    )
    beneficiaries = pd.concat(
        [retail_beneficiaries, sme],
        ignore_index=True,
        sort=False,
    )

    if beneficiaries.empty:
        return {}

    beneficiaries["counterparty_key"] = beneficiaries[
        "beneficiary_account_number"
    ].map(
        lambda value: build_counterparty_identity(
            rail="LOCAL",
            counterparty_iban="",
            counterparty_swift_bic="",
            counterparty_account_number=value,
        ).counterparty_key
    )

    name_column = "beneficiary_account_holder_name"

    if name_column not in beneficiaries.columns:
        return {}

    prepared = beneficiaries.loc[
        beneficiaries[name_column]
        .astype("string")
        .str.strip()
        .ne("")
    ].copy()

    return (
        prepared
        .sort_values(
            by=["counterparty_key", name_column],
            kind="stable",
        )
        .drop_duplicates(
            subset=["counterparty_key"],
            keep="first",
        )
        .set_index("counterparty_key")[name_column]
        .astype("string")
        .to_dict()
    )


def discover_recursive_counterparties(
    *,
    observed_network: UnifiedGroupResult,
    source_entity_key: str,
    group_ids: list[str] | tuple[str, ...],
    run_date: date | str,
    source_directory: Path | str | None = None,
    raw_sources: RawDiscoverySources | None = None,
) -> RecursiveCounterpartyDiscoveryResult:
    """Find unseen counterparties shared by one approved source."""
    resolved_run_date = parse_run_date(run_date)
    resolved_group_ids = tuple(sorted(set(group_ids)))

    if not resolved_group_ids:
        raise RecursiveCounterpartyFrontierError(
            "At least one source group ID is required."
        )

    source_node_key = f"CUSTOMER|{source_entity_key}"
    source_groups = set(
        observed_network.nodes.loc[
            observed_network.nodes["node_key"].eq(
                source_node_key
            ),
            "group_id",
        ].astype("string")
    )

    if not set(resolved_group_ids).issubset(source_groups):
        raise RecursiveCounterpartyFrontierError(
            "The recursive source is not present in every requested "
            f"group: {source_entity_key}"
        )

    sources = _resolve_raw_sources(
        source_directory=source_directory,
        raw_sources=raw_sources,
    )
    identity_lookup = _prepare_identity_lookup(
        sources.customer_identity
    )

    source_resolution = identity_lookup.loc[
        identity_lookup["entity_key"].eq(
            source_entity_key
        )
    ]

    if len(source_resolution) != 1:
        raise RecursiveCounterpartyFrontierError(
            "Expected exactly one customer resolution for recursive "
            f"source {source_entity_key}; found {len(source_resolution)}."
        )

    source_customer_id = str(
        source_resolution.iloc[0]["customer_id"]
    )
    outward = _prepare_outward_events(
        sources.local_outward_payments,
        resolved_run_date,
    )
    names = _counterparty_name_lookup(
        sources.retail_beneficiaries,
        sources.sme_beneficiaries,
    )

    source_events = outward.loc[
        outward["customer_id"].eq(source_customer_id)
    ].copy()

    if source_events.empty:
        raise RecursiveCounterpartyFrontierError(
            "The approved recursive source has no completed outward "
            f"events on or before {resolved_run_date}: "
            f"{source_entity_key}"
        )

    existing_counterparty_keys = set(
        observed_network.nodes.loc[
            observed_network.nodes["node_type"].eq(
                "COUNTERPARTY"
            ),
            "counterparty_key",
        ]
        .astype("string")
        .str.strip()
    )

    relationship_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    new_keys: list[str] = []
    skipped_existing: list[str] = []
    unshared: list[str] = []

    for counterparty_key in sorted(
        source_events["counterparty_key"].unique()
    ):
        current_source_events = source_events.loc[
            source_events["counterparty_key"].eq(
                counterparty_key
            )
        ]

        if counterparty_key in existing_counterparty_keys:
            skipped_existing.append(counterparty_key)
            continue

        candidate_events = outward.loc[
            outward["counterparty_key"].eq(
                counterparty_key
            )
            & ~outward["customer_id"].eq(
                source_customer_id
            )
        ].copy()

        if candidate_events.empty:
            unshared.append(counterparty_key)
            continue

        candidate_summary = (
            candidate_events
            .groupby("customer_id", as_index=False)
            .agg(
                candidate_event_count=(
                    "transfer_id",
                    "nunique",
                ),
                candidate_total_amount=(
                    "amount_value",
                    "sum",
                ),
                candidate_first_event_timestamp=(
                    "transaction_at",
                    "min",
                ),
                candidate_last_event_timestamp=(
                    "transaction_at",
                    "max",
                ),
            )
            .merge(
                identity_lookup,
                how="left",
                on="customer_id",
                validate="one_to_one",
            )
        )

        if candidate_summary["entity_key"].isna().any():
            unresolved = sorted(
                candidate_summary.loc[
                    candidate_summary["entity_key"].isna(),
                    "customer_id",
                ].unique()
            )
            raise RecursiveCounterpartyFrontierError(
                "Recursive candidates could not be resolved: "
                f"{unresolved}"
            )

        total_candidate_events = int(
            candidate_summary["candidate_event_count"].sum()
        )
        total_candidate_amount = float(
            candidate_summary["candidate_total_amount"].sum()
        )
        source_event_count = int(
            current_source_events["transfer_id"].nunique()
        )
        source_total_amount = float(
            current_source_events["amount_value"].sum()
        )
        counterparty_name = names.get(
            counterparty_key,
            counterparty_key,
        )
        evidence_key = (
            f"RECURSIVE_SHARED_COUNTERPARTY|{counterparty_key}"
        )
        evidence_summary = (
            f"Approved expansion source {source_entity_key} used "
            f"{counterparty_key} in {source_event_count} completed "
            f"outward events; {len(candidate_summary)} other Wio "
            f"customers used the same counterparty in "
            f"{total_candidate_events} events."
        )

        new_keys.append(counterparty_key)
        summary_rows.append(
            {
                "run_date": str(resolved_run_date),
                "source_entity_key": source_entity_key,
                "source_customer_id": source_customer_id,
                "group_ids": "|".join(resolved_group_ids),
                "counterparty_key": counterparty_key,
                "counterparty_name": counterparty_name,
                "source_event_count": source_event_count,
                "source_total_amount": round(
                    source_total_amount,
                    2,
                ),
                "candidate_customer_count": len(
                    candidate_summary
                ),
                "candidate_event_count": (
                    total_candidate_events
                ),
                "candidate_total_amount": round(
                    total_candidate_amount,
                    2,
                ),
                "source_first_event_timestamp": str(
                    current_source_events[
                        "transaction_at"
                    ].min()
                ),
                "source_last_event_timestamp": str(
                    current_source_events[
                        "transaction_at"
                    ].max()
                ),
                "candidate_first_event_timestamp": str(
                    candidate_events[
                        "transaction_at"
                    ].min()
                ),
                "candidate_last_event_timestamp": str(
                    candidate_events[
                        "transaction_at"
                    ].max()
                ),
            }
        )

        for candidate in candidate_summary.itertuples(
            index=False
        ):
            relationship_rows.append(
                {
                    "snapshot_date": str(
                        resolved_run_date
                    ),
                    "source_entity_key": (
                        source_entity_key
                    ),
                    "relationship_type": (
                        "SHARED_EXTERNAL_COUNTERPARTY"
                    ),
                    "counterparty_key": (
                        counterparty_key
                    ),
                    "counterparty_name": (
                        counterparty_name
                    ),
                    "target_entity_type": (
                        candidate.entity_type
                    ),
                    "target_entity_id": (
                        candidate.entity_id
                    ),
                    "target_entity_key": (
                        candidate.entity_key
                    ),
                    "evidence_key": evidence_key,
                    "evidence_summary": (
                        evidence_summary
                    ),
                    "source_event_count": (
                        source_event_count
                    ),
                    "candidate_event_count": int(
                        candidate.candidate_event_count
                    ),
                    "total_candidate_event_count": (
                        total_candidate_events
                    ),
                    "source_total_amount": round(
                        source_total_amount,
                        2,
                    ),
                    "candidate_total_amount": round(
                        float(
                            candidate.candidate_total_amount
                        ),
                        2,
                    ),
                }
            )

    relationships = pd.DataFrame(relationship_rows)
    counterparty_summary = pd.DataFrame(summary_rows)

    if relationships.empty:
        relationships = pd.DataFrame(
            columns=[
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
                "total_candidate_event_count",
                "source_total_amount",
                "candidate_total_amount",
            ]
        )
    else:
        relationships = relationships.sort_values(
            by=[
                "counterparty_key",
                "target_entity_key",
            ],
            kind="stable",
        ).reset_index(drop=True)

    if counterparty_summary.empty:
        counterparty_summary = pd.DataFrame(
            columns=[
                "run_date",
                "source_entity_key",
                "source_customer_id",
                "group_ids",
                "counterparty_key",
                "counterparty_name",
                "source_event_count",
                "source_total_amount",
                "candidate_customer_count",
                "candidate_event_count",
                "candidate_total_amount",
                "source_first_event_timestamp",
                "source_last_event_timestamp",
                "candidate_first_event_timestamp",
                "candidate_last_event_timestamp",
            ]
        )
    else:
        counterparty_summary = (
            counterparty_summary
            .sort_values(
                by=["counterparty_key"],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    return RecursiveCounterpartyDiscoveryResult(
        source_entity_key=source_entity_key,
        source_customer_id=source_customer_id,
        source_group_ids=resolved_group_ids,
        relationships=relationships,
        counterparty_summary=counterparty_summary,
        new_counterparty_keys=tuple(sorted(new_keys)),
        skipped_existing_counterparty_keys=tuple(
            sorted(skipped_existing)
        ),
        unshared_counterparty_keys=tuple(
            sorted(unshared)
        ),
    )


def _group_depth_metrics(
    network: UnifiedGroupResult,
    group_id: str,
    anchor_entity_key: str,
) -> tuple[int, int]:
    group_nodes = network.nodes.loc[
        network.nodes["group_id"].eq(group_id)
    ]
    group_edges = network.edges.loc[
        network.edges["group_id"].eq(group_id)
    ]
    anchor_node_key = f"CUSTOMER|{anchor_entity_key}"

    adjacency: dict[str, set[str]] = {
        str(value): set()
        for value in group_nodes["node_key"]
    }

    for edge in group_edges.itertuples(index=False):
        source = str(edge.source_node_key)
        target = str(edge.target_node_key)
        adjacency.setdefault(source, set()).add(target)
        adjacency.setdefault(target, set()).add(source)

    if anchor_node_key not in adjacency:
        return 0, len(group_nodes)

    distances = {anchor_node_key: 0}
    frontier = [anchor_node_key]

    while frontier:
        current = frontier.pop(0)

        for neighbor in sorted(adjacency.get(current, set())):
            if neighbor in distances:
                continue

            distances[neighbor] = distances[current] + 1
            frontier.append(neighbor)

    unreachable_count = max(
        len(group_nodes) - len(distances),
        0,
    )

    return max(distances.values(), default=0), unreachable_count


def build_guardrail_telemetry(
    *,
    network: UnifiedGroupResult,
    frontier_queue: pd.DataFrame,
    run_date: date | str,
    stage: str,
    new_node_count: int,
    new_edge_count: int,
) -> pd.DataFrame:
    """Measure breadth/depth without enforcing synthetic-data caps."""
    resolved_run_date = parse_run_date(run_date)
    records: list[dict[str, Any]] = []

    for group in network.groups.itertuples(index=False):
        group_id = str(group.group_id)
        anchor = str(group.group_anchor_seed_entity_key)
        max_depth, unreachable_count = _group_depth_metrics(
            network,
            group_id,
            anchor,
        )
        group_nodes = network.nodes.loc[
            network.nodes["group_id"].eq(group_id)
        ]
        group_edges = network.edges.loc[
            network.edges["group_id"].eq(group_id)
        ]

        ready_items = frontier_queue.loc[
            frontier_queue["queue_status"]
            .astype("string")
            .str.upper()
            .eq("READY")
        ]

        group_subject_keys: set[str] = set()

        for key_column in (
            "entity_key",
            "counterparty_key",
        ):
            if key_column not in group_nodes.columns:
                continue

            group_subject_keys.update(
                value
                for value in group_nodes[key_column]
                .fillna("")
                .astype("string")
                .tolist()
                if value
            )

        frontier_width = sum(
            1
            for item in ready_items.itertuples(index=False)
            if (
                group_id
                in {
                    value
                    for value in str(
                        getattr(item, "group_ids", "")
                    ).split("|")
                    if value
                }
                or (
                    not str(
                        getattr(item, "group_ids", "")
                    ).strip()
                    and str(
                        getattr(item, "subject_key", "")
                    ) in group_subject_keys
                )
            )
        )

        records.append(
            {
                "run_date": str(resolved_run_date),
                "stage": stage,
                "group_id": group_id,
                "group_anchor_seed_entity_key": anchor,
                "max_observed_depth": max_depth,
                "unreachable_node_count": unreachable_count,
                "total_node_count": len(group_nodes),
                "total_edge_count": len(group_edges),
                "current_frontier_width": frontier_width,
                "expansion_source_count": int(
                    group_nodes["expansion_source_flag"]
                    .astype(bool)
                    .sum()
                ),
                "new_node_count": new_node_count,
                "new_edge_count": new_edge_count,
                "breadth_cap_enforced_flag": False,
                "depth_cap_enforced_flag": False,
                "guardrail_status": "TELEMETRY_ONLY",
            }
        )

    return pd.DataFrame(records)



def _resume_discovery_from_state(
    *,
    snapshot,
    supplemental_subject_payloads: pd.DataFrame,
) -> RecursiveCounterpartyDiscoveryResult:
    """Reconstruct a completed recursive discovery from state."""
    completed = snapshot.expansion_ledger.loc[
        snapshot.expansion_ledger["expansion_status"]
        .astype("string")
        .str.upper()
        .eq("COMPLETED")
    ].copy()

    if completed.empty:
        raise RecursiveCounterpartyFrontierError(
            "No ready discovery action or completed recursive "
            "expansion was found."
        )

    latest = completed.iloc[-1]
    source_entity_key = str(latest["source_entity_key"])
    source_group_ids = tuple(
        value
        for value in str(latest["group_ids"]).split("|")
        if value
    )
    supplied_counterparties = set(
        supplemental_subject_payloads.loc[
            supplemental_subject_payloads["subject_type"]
            .astype("string")
            .str.upper()
            .eq("COUNTERPARTY"),
            "subject_key",
        ].astype("string")
    )
    observed_counterparties = set(
        snapshot.network.nodes.loc[
            snapshot.network.nodes["node_type"].eq("COUNTERPARTY"),
            "counterparty_key",
        ].astype("string")
    )
    new_keys = tuple(sorted(
        observed_counterparties - supplied_counterparties
    ))

    if not new_keys:
        raise RecursiveCounterpartyFrontierError(
            "Completed recursive state contains no counterparty "
            "outside the supplied first-layer payloads."
        )

    identity_nodes = snapshot.network.nodes.loc[
        snapshot.network.nodes["node_key"].eq(
            f"CUSTOMER|{source_entity_key}"
        )
    ]
    source_customer_id = (
        str(identity_nodes.iloc[0]["entity_id"])
        if not identity_nodes.empty
        else source_entity_key.split("|", 1)[-1]
    )
    relationship_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    for counterparty_key in new_keys:
        counterparty_node_key = f"COUNTERPARTY|{counterparty_key}"
        target_edges = snapshot.network.edges.loc[
            snapshot.network.edges["source_node_key"].eq(
                counterparty_node_key
            )
            & snapshot.network.edges["edge_type"].eq(
                "SHARED_EXTERNAL_COUNTERPARTY"
            )
        ]
        source_edges = snapshot.network.edges.loc[
            snapshot.network.edges["target_node_key"].eq(
                counterparty_node_key
            )
            & snapshot.network.edges["edge_type"].eq(
                "CUSTOMER_COUNTERPARTY_EVIDENCE"
            )
        ]
        counterparty_node = snapshot.network.nodes.loc[
            snapshot.network.nodes["node_key"].eq(
                counterparty_node_key
            )
        ].iloc[0]

        for edge in target_edges.itertuples(index=False):
            target = snapshot.network.nodes.loc[
                snapshot.network.nodes["node_key"].eq(
                    edge.target_node_key
                )
            ].iloc[0]
            relationship_rows.append({
                "snapshot_date": str(edge.last_seen_date),
                "source_entity_key": source_entity_key,
                "relationship_type": "SHARED_EXTERNAL_COUNTERPARTY",
                "counterparty_key": counterparty_key,
                "counterparty_name": str(counterparty_node.display_label),
                "target_entity_type": str(target.entity_type),
                "target_entity_id": str(target.entity_id),
                "target_entity_key": str(target.entity_key),
                "evidence_key": str(edge.evidence_key),
                "evidence_summary": str(edge.evidence_summary),
                "source_event_count": (
                    source_edges.iloc[0]["source_event_count"]
                    if not source_edges.empty else ""
                ),
                "candidate_event_count": edge.candidate_event_count,
                "total_candidate_event_count": (
                    source_edges.iloc[0]["candidate_event_count"]
                    if not source_edges.empty else ""
                ),
                "source_total_amount": "",
                "candidate_total_amount": "",
            })

        summary_rows.append({
            "run_date": str(latest["run_date"]),
            "source_entity_key": source_entity_key,
            "source_customer_id": source_customer_id,
            "group_ids": "|".join(source_group_ids),
            "counterparty_key": counterparty_key,
            "counterparty_name": str(counterparty_node.display_label),
            "source_event_count": (
                source_edges.iloc[0]["source_event_count"]
                if not source_edges.empty else ""
            ),
            "candidate_customer_count": len(target_edges),
            "candidate_event_count": (
                source_edges.iloc[0]["candidate_event_count"]
                if not source_edges.empty else ""
            ),
        })

    return RecursiveCounterpartyDiscoveryResult(
        source_entity_key=source_entity_key,
        source_customer_id=source_customer_id,
        source_group_ids=source_group_ids,
        relationships=pd.DataFrame(relationship_rows),
        counterparty_summary=pd.DataFrame(summary_rows),
        new_counterparty_keys=new_keys,
        skipped_existing_counterparty_keys=tuple(),
        unshared_counterparty_keys=tuple(),
    )


def _next_expansion_round_number(
    expansion_ledger: pd.DataFrame,
) -> int:
    """Return the next monotonic expansion round."""
    if expansion_ledger.empty:
        return 1

    values = pd.to_numeric(
        expansion_ledger["round_number"],
        errors="coerce",
    ).dropna()

    return (
        int(values.max()) + 1
        if not values.empty
        else 1
    )


def _empty_behavioral_feature_result(
) -> BehavioralFeatureResult:
    """Return schema-safe empty recursive feature output."""
    return BehavioralFeatureResult(
        counterparty_profiles=pd.DataFrame(),
        counterparty_customer_profiles=pd.DataFrame(),
        counterparty_payloads=pd.DataFrame(
            columns=[
                "subject_type",
                "subject_key",
                "feature_payload_json",
            ]
        ),
    )


def run_recursive_counterparty_frontier(
    *,
    state_directory: Path | str,
    run_date: date | str,
    supplemental_subject_payloads: pd.DataFrame,
    settings: DailyAiSettings,
    source_directory: Path | str | None = None,
    raw_sources: RawDiscoverySources | None = None,
    international_counterparty_currency_activity: (
        pd.DataFrame | None
    ) = None,
    selected_source_entity_key: str | None = None,
    adapter_factory=None,
) -> RecursiveCounterpartyFrontierResult:
    """Consume approved discovery work and run new counterparty AI."""
    resolved_run_date = parse_run_date(run_date)
    resolved_sources = _resolve_raw_sources(
        source_directory=source_directory,
        raw_sources=raw_sources,
    )
    state_store = CsvDailyStateStore(state_directory)

    try:
        snapshot = state_store.load_snapshot()
    except FileNotFoundError as exc:
        raise RecursiveCounterpartyFrontierError(
            "Persisted Scenario 1 live state is unavailable. Rebuild "
            "the prior counterparty and customer frontiers before "
            "starting recursion."
        ) from exc

    pre_discovery_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
    )
    discovery_queue = (
        pre_discovery_plan.actionable_queue.loc[
            pre_discovery_plan.actionable_queue[
                "action_type"
            ].eq(DISCOVERY_ACTION_TYPE)
        ]
        .copy()
    )

    if not discovery_queue.empty:
        discovery_queue["subject_key"] = (
            discovery_queue["subject_key"]
            .astype("string")
            .str.strip()
        )
        discovery_queue = (
            discovery_queue
            .sort_values(
                by=[
                    "subject_key",
                    "queue_item_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        if selected_source_entity_key is not None:
            normalized_source_key = str(
                selected_source_entity_key
            ).strip()

            if not normalized_source_key:
                raise RecursiveCounterpartyFrontierError(
                    "selected_source_entity_key must be nonblank."
                )

            discovery_queue = (
                discovery_queue.loc[
                    discovery_queue["subject_key"].eq(
                        normalized_source_key
                    )
                ]
                .copy()
                .reset_index(drop=True)
            )

            if len(discovery_queue) != 1:
                raise RecursiveCounterpartyFrontierError(
                    "The selected recursive discovery source "
                    "does not resolve to exactly one ready queue "
                    f"item: {normalized_source_key}."
                )

        discovery_item = discovery_queue.iloc[0]
        source_entity_key = str(discovery_item["subject_key"])
        source_group_ids = tuple(
            sorted(
                value
                for value in str(
                    discovery_item["group_ids"]
                ).split("|")
                if value
            )
        )
        discovery = discover_recursive_counterparties(
            raw_sources=resolved_sources,
            observed_network=snapshot.network,
            source_entity_key=source_entity_key,
            group_ids=source_group_ids,
            run_date=resolved_run_date,
        )

        before_node_count = len(snapshot.network.nodes)
        before_edge_count = len(snapshot.network.edges)

        if discovery.relationships.empty:
            expanded_network = snapshot.network
        else:
            expanded_network = merge_expansion_relationships(
                graph=snapshot.network,
                relationships=discovery.relationships,
                group_ids=list(source_group_ids),
                run_date=resolved_run_date,
            )
            state_store.save_network_state(
                expanded_network,
                resolved_run_date,
            )

        new_node_count = (
            len(expanded_network.nodes) - before_node_count
        )
        new_edge_count = (
            len(expanded_network.edges) - before_edge_count
        )

        state_store.append_expansion_ledger(
            pd.DataFrame(
                [
                    {
                        "run_date": str(resolved_run_date),
                        "round_number": (
                            _next_expansion_round_number(
                                snapshot.expansion_ledger
                            )
                        ),
                        "queue_item_id": str(
                            discovery_item["queue_item_id"]
                        ),
                        "source_entity_key": (
                            source_entity_key
                        ),
                        "group_ids": "|".join(
                            source_group_ids
                        ),
                        "relationship_rows_found": str(
                            len(discovery.relationships)
                        ),
                        "expansion_status": "COMPLETED",
                    }
                ]
            )
        )
    else:
        discovery = _resume_discovery_from_state(
            snapshot=snapshot,
            supplemental_subject_payloads=(
                supplemental_subject_payloads
            ),
        )
        expanded_network = snapshot.network
        new_node_count = 0
        new_edge_count = 0

    if discovery.new_counterparty_keys:
        new_features = build_behavioral_features(
            raw_sources=resolved_sources,
            international_counterparty_currency_activity=(
                international_counterparty_currency_activity
            ),
            counterparty_keys=(
                discovery.new_counterparty_keys
            ),
            run_date=resolved_run_date,
        )
    else:
        new_features = (
            _empty_behavioral_feature_result()
        )
    combined_payloads = pd.concat(
        [
            supplemental_subject_payloads,
            new_features.counterparty_payloads,
        ],
        ignore_index=True,
    )
    combined_payloads = (
        combined_payloads
        .drop_duplicates(
            subset=["subject_type", "subject_key"],
            keep="last",
        )
        .sort_values(
            by=["subject_type", "subject_key"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    frontier_kwargs: dict[str, Any] = {
        "unified_result": expanded_network,
        "supplemental_subject_payloads": (
            combined_payloads
        ),
        "state_directory": state_directory,
        "run_date": resolved_run_date,
        "settings": settings,
    }

    if adapter_factory is not None:
        frontier_kwargs["adapter_factory"] = (
            adapter_factory
        )

    counterparty_result: CounterpartyFrontierRunResult = (
        run_counterparty_ai_frontier(
            **frontier_kwargs
        )
    )
    final_queue = (
        counterparty_result.controlled_run
        .final_plan.frontier_queue
    )
    telemetry_network = UnifiedGroupResult(
        groups=(
            counterparty_result.controlled_run
            .final_plan.projection.groups
        ),
        nodes=(
            counterparty_result.controlled_run
            .final_plan.projection.nodes
        ),
        edges=(
            counterparty_result.controlled_run
            .final_plan.projection.edges
        ),
    )
    telemetry = build_guardrail_telemetry(
        network=telemetry_network,
        frontier_queue=final_queue,
        run_date=resolved_run_date,
        stage="RECURSIVE_COUNTERPARTY_FRONTIER",
        new_node_count=new_node_count,
        new_edge_count=new_edge_count,
    )

    return RecursiveCounterpartyFrontierResult(
        pre_discovery_plan=pre_discovery_plan,
        discovery=discovery,
        expanded_network=expanded_network,
        new_features=new_features,
        controlled_run=(
            counterparty_result.controlled_run
        ),
        decision_store=(
            counterparty_result.decision_store
        ),
        ai_call_ledger=CsvAiCallLedger(
            state_directory
        ).load(),
        guardrail_telemetry=telemetry,
    )
