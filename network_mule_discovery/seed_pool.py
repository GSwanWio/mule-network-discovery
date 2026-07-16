"""Known-mule seed loading and entity resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from network_mule_discovery.data_sources import NetworkDataSource


@dataclass(frozen=True)
class SeedResolutionResult:
    """Resolved and unresolved seed-pool records."""

    seeds: pd.DataFrame
    seed_entities: pd.DataFrame
    seed_identity_rows: pd.DataFrame
    unresolved_seeds: pd.DataFrame
    ambiguous_seed_ids: pd.DataFrame


def resolve_seed_pool(
    data_source: NetworkDataSource,
    run_date: date | str,
) -> SeedResolutionResult:
    """
    Resolve FRC seed customer IDs to SME and retail graph entities.

    A seed customer ID may resolve through:
    - SME business_id
    - retail customer_id
    """
    seeds = data_source.get_seed_mules(run_date)

    seed_identity_rows = (
        data_source.get_entities_by_lookup_customer_ids(
            lookup_customer_ids=seeds["seed_customer_id"].tolist(),
            run_date=run_date,
        )
    )

    seed_identity_rows = (
        seed_identity_rows
        .merge(
            seeds[
                [
                    "seed_customer_id",
                    "seed_source",
                ]
            ],
            how="inner",
            left_on="lookup_customer_id",
            right_on="seed_customer_id",
            validate="many_to_one",
        )
        .sort_values(
            by=[
                "seed_customer_id",
                "entity_key",
                "individual_id",
                "emirates_id_number",
            ],
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )

    seed_entities = (
        seed_identity_rows[
            [
                "snapshot_date",
                "seed_customer_id",
                "seed_source",
                "entity_type",
                "entity_id",
                "entity_key",
                "lookup_customer_id",
                "entity_created_at",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            by=[
                "seed_customer_id",
                "entity_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    resolved_seed_ids = set(
        seed_entities["seed_customer_id"].dropna()
    )

    unresolved_seeds = (
        seeds.loc[
            ~seeds["seed_customer_id"].isin(resolved_seed_ids)
        ]
        .copy()
        .reset_index(drop=True)
    )

    resolution_counts = (
        seed_entities
        .groupby(
            "seed_customer_id",
            as_index=False,
        )
        .agg(
            resolved_entity_count=(
                "entity_key",
                "nunique",
            )
        )
    )

    ambiguous_seed_ids = (
        resolution_counts.loc[
            resolution_counts["resolved_entity_count"] > 1
        ]
        .copy()
        .reset_index(drop=True)
    )

    return SeedResolutionResult(
        seeds=seeds,
        seed_entities=seed_entities,
        seed_identity_rows=seed_identity_rows,
        unresolved_seeds=unresolved_seeds,
        ambiguous_seed_ids=ambiguous_seed_ids,
    )
