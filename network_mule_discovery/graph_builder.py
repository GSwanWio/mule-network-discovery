"""Build deterministic graph outputs from EID discovery results."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date

import pandas as pd

from network_mule_discovery.eid_discovery import (
    EID_REASON_CODE,
    EidDiscoveryResult,
)
from network_mule_discovery.schemas import parse_run_date


GROUP_TYPE_EID_ONLY = "EID_ONLY"


@dataclass(frozen=True)
class GraphBuildResult:
    """Persistable graph tables."""

    groups: pd.DataFrame
    nodes: pd.DataFrame
    edges: pd.DataFrame


def _stable_id(prefix: str, *values: str) -> str:
    """Create a deterministic identifier from canonical string values."""
    canonical_value = "|".join(str(value) for value in values)
    digest = hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()[:16]

    return f"{prefix}{digest}"


def _combine_text_values(values: pd.Series) -> str | pd.NA:
    """Combine sorted, unique, nonblank text values."""
    normalized_values = sorted(
        {
            str(value).strip()
            for value in values.dropna()
            if str(value).strip()
        }
    )

    if not normalized_values:
        return pd.NA

    return "|".join(normalized_values)


def _build_components(
    links: pd.DataFrame,
) -> list[list[str]]:
    """Build undirected connected components without external graph libraries."""
    adjacency: dict[str, set[str]] = {}

    for row in links.itertuples(index=False):
        source = row.seed_entity_key
        target = row.candidate_entity_key

        adjacency.setdefault(source, set()).add(target)
        adjacency.setdefault(target, set()).add(source)

    components: list[list[str]] = []
    visited: set[str] = set()

    for starting_entity in sorted(adjacency):
        if starting_entity in visited:
            continue

        stack = [starting_entity]
        component: list[str] = []

        while stack:
            entity_key = stack.pop()

            if entity_key in visited:
                continue

            visited.add(entity_key)
            component.append(entity_key)

            neighbours = sorted(
                adjacency.get(entity_key, set()),
                reverse=True,
            )

            stack.extend(
                neighbour
                for neighbour in neighbours
                if neighbour not in visited
            )

        components.append(sorted(component))

    return components


def build_eid_graph(
    discovery_result: EidDiscoveryResult,
    run_date: date | str,
) -> GraphBuildResult:
    """Convert deterministic EID links into groups, nodes, and edges."""
    resolved_run_date = parse_run_date(run_date)
    run_id = f"eid_{resolved_run_date:%Y%m%d}"

    links = discovery_result.eid_links.copy()

    group_columns = [
        "run_id",
        "run_date",
        "group_id",
        "group_type",
        "seed_entity_count",
        "discovered_entity_count",
        "total_entity_count",
        "eid_count",
        "edge_count",
    ]

    node_columns = [
        "run_id",
        "run_date",
        "group_id",
        "node_id",
        "entity_type",
        "entity_id",
        "entity_key",
        "lookup_customer_id",
        "seed_customer_ids",
        "seed_sources",
        "seed_flag",
        "discovered_flag",
        "discovery_reason_code",
        "entity_created_at",
    ]

    edge_columns = [
        "run_id",
        "run_date",
        "group_id",
        "edge_id",
        "source_node_id",
        "target_node_id",
        "source_entity_key",
        "target_entity_key",
        "edge_type",
        "reason_code",
        "emirates_id_number",
        "seed_individual_ids",
        "candidate_individual_ids",
        "deterministic_flag",
    ]

    if links.empty:
        return GraphBuildResult(
            groups=pd.DataFrame(columns=group_columns),
            nodes=pd.DataFrame(columns=node_columns),
            edges=pd.DataFrame(columns=edge_columns),
        )

    components = _build_components(links)

    entity_to_group: dict[str, str] = {}

    for component in components:
        group_id = _stable_id(
            "G",
            GROUP_TYPE_EID_ONLY,
            *component,
        )

        for entity_key in component:
            entity_to_group[entity_key] = group_id

    seed_entities = (
        discovery_result.seed_resolution.seed_entities
        .groupby(
            [
                "entity_type",
                "entity_id",
                "entity_key",
                "lookup_customer_id",
                "entity_created_at",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            seed_customer_ids=(
                "seed_customer_id",
                _combine_text_values,
            ),
            seed_sources=(
                "seed_source",
                _combine_text_values,
            ),
        )
    )

    seed_entities = seed_entities.loc[
        seed_entities["entity_key"].isin(entity_to_group)
    ].copy()

    seed_entities["seed_flag"] = True
    seed_entities["discovered_flag"] = False
    seed_entities["discovery_reason_code"] = pd.NA

    discovered_entities = (
        discovery_result.discovered_entities
        .rename(
            columns={
                "candidate_entity_type": "entity_type",
                "candidate_entity_id": "entity_id",
                "candidate_entity_key": "entity_key",
                "candidate_lookup_customer_id": "lookup_customer_id",
                "candidate_entity_created_at": "entity_created_at",
            }
        )
        .copy()
    )

    discovered_entities["seed_customer_ids"] = pd.NA
    discovered_entities["seed_sources"] = pd.NA
    discovered_entities["seed_flag"] = False
    discovered_entities["discovered_flag"] = True
    discovered_entities["discovery_reason_code"] = EID_REASON_CODE

    node_base = pd.concat(
        [
            seed_entities[
                [
                    "entity_type",
                    "entity_id",
                    "entity_key",
                    "lookup_customer_id",
                    "seed_customer_ids",
                    "seed_sources",
                    "seed_flag",
                    "discovered_flag",
                    "discovery_reason_code",
                    "entity_created_at",
                ]
            ],
            discovered_entities[
                [
                    "entity_type",
                    "entity_id",
                    "entity_key",
                    "lookup_customer_id",
                    "seed_customer_ids",
                    "seed_sources",
                    "seed_flag",
                    "discovered_flag",
                    "discovery_reason_code",
                    "entity_created_at",
                ]
            ],
        ],
        ignore_index=True,
    )

    node_base = (
        node_base
        .drop_duplicates(subset=["entity_key"])
        .sort_values(
            by=["entity_key"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    node_base["run_id"] = run_id
    node_base["run_date"] = resolved_run_date
    node_base["group_id"] = node_base["entity_key"].map(
        entity_to_group
    )
    node_base["node_id"] = node_base["entity_key"].map(
        lambda entity_key: _stable_id(
            "N",
            entity_key,
        )
    )

    nodes = (
        node_base[node_columns]
        .sort_values(
            by=[
                "group_id",
                "seed_flag",
                "entity_key",
            ],
            ascending=[True, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    node_id_by_entity = dict(
        zip(
            nodes["entity_key"],
            nodes["node_id"],
            strict=True,
        )
    )

    edges = links.rename(
        columns={
            "seed_entity_key": "source_entity_key",
            "candidate_entity_key": "target_entity_key",
        }
    ).copy()

    edges["run_id"] = run_id
    edges["run_date"] = resolved_run_date
    edges["group_id"] = edges["source_entity_key"].map(
        entity_to_group
    )
    edges["source_node_id"] = edges["source_entity_key"].map(
        node_id_by_entity
    )
    edges["target_node_id"] = edges["target_entity_key"].map(
        node_id_by_entity
    )

    edges["edge_id"] = edges.apply(
        lambda row: _stable_id(
            "E",
            row["source_entity_key"],
            row["target_entity_key"],
            row["emirates_id_number"],
            row["edge_type"],
        ),
        axis=1,
    )

    edges = (
        edges[edge_columns]
        .sort_values(
            by=[
                "group_id",
                "source_entity_key",
                "target_entity_key",
                "emirates_id_number",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    node_summary = (
        nodes
        .groupby(
            "group_id",
            as_index=False,
        )
        .agg(
            seed_entity_count=(
                "seed_flag",
                "sum",
            ),
            discovered_entity_count=(
                "discovered_flag",
                "sum",
            ),
            total_entity_count=(
                "entity_key",
                "nunique",
            ),
        )
    )

    edge_summary = (
        edges
        .groupby(
            "group_id",
            as_index=False,
        )
        .agg(
            eid_count=(
                "emirates_id_number",
                "nunique",
            ),
            edge_count=(
                "edge_id",
                "nunique",
            ),
        )
    )

    groups = node_summary.merge(
        edge_summary,
        how="inner",
        on="group_id",
        validate="one_to_one",
    )

    groups["run_id"] = run_id
    groups["run_date"] = resolved_run_date
    groups["group_type"] = GROUP_TYPE_EID_ONLY

    groups = (
        groups[group_columns]
        .sort_values(
            by=["group_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    return GraphBuildResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
    )
