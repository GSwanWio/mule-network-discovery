"""Deterministic, seed-led Emirates ID discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from network_mule_discovery.data_sources import NetworkDataSource
from network_mule_discovery.seed_pool import (
    SeedResolutionResult,
    resolve_seed_pool,
)


EID_EDGE_TYPE = "SAME_EMIRATES_ID"
EID_REASON_CODE = "same_eid_as_seed_mule"


@dataclass(frozen=True)
class EidDiscoveryResult:
    """Outputs from direct seed-led EID discovery."""

    seed_resolution: SeedResolutionResult
    seed_eids: pd.DataFrame
    matching_identity_rows: pd.DataFrame
    discovered_entities: pd.DataFrame
    eid_links: pd.DataFrame


def _combine_individual_ids(values: pd.Series) -> str | pd.NA:
    """Return sorted, unique supporting individual IDs."""
    individual_ids = sorted(
        {
            str(value).strip()
            for value in values.dropna()
            if str(value).strip()
        }
    )

    if not individual_ids:
        return pd.NA

    return "|".join(individual_ids)


def discover_entities_by_seed_eids(
    data_source: NetworkDataSource,
    run_date: date | str,
) -> EidDiscoveryResult:
    """
    Discover non-seed entities sharing valid EIDs with resolved seed entities.

    Discovery is direct from seed EIDs only. EIDs belonging only to newly
    discovered entities are not used for further expansion.
    """
    seed_resolution = resolve_seed_pool(
        data_source=data_source,
        run_date=run_date,
    )

    seed_identity_rows = seed_resolution.seed_identity_rows

    seed_eids = (
        seed_identity_rows.loc[
            seed_identity_rows["eid_linkable_flag"],
            [
                "snapshot_date",
                "seed_customer_id",
                "seed_source",
                "entity_type",
                "entity_id",
                "entity_key",
                "lookup_customer_id",
                "individual_id",
                "emirates_id_number",
                "entity_created_at",
            ],
        ]
        .drop_duplicates()
        .sort_values(
            by=[
                "entity_key",
                "emirates_id_number",
                "individual_id",
            ],
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )

    distinct_seed_eids = (
        seed_eids["emirates_id_number"]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    matching_identity_rows = (
        data_source.search_entities_by_eids(
            emirates_ids=distinct_seed_eids,
            run_date=run_date,
        )
    )

    seed_side = seed_eids.rename(
        columns={
            "entity_type": "seed_entity_type",
            "entity_id": "seed_entity_id",
            "entity_key": "seed_entity_key",
            "lookup_customer_id": "seed_lookup_customer_id",
            "individual_id": "seed_individual_id",
            "entity_created_at": "seed_entity_created_at",
        }
    )

    candidate_side = matching_identity_rows.rename(
        columns={
            "entity_type": "candidate_entity_type",
            "entity_id": "candidate_entity_id",
            "entity_key": "candidate_entity_key",
            "lookup_customer_id": "candidate_lookup_customer_id",
            "individual_id": "candidate_individual_id",
            "entity_created_at": "candidate_entity_created_at",
        }
    )

    candidate_columns = [
        "emirates_id_number",
        "candidate_entity_type",
        "candidate_entity_id",
        "candidate_entity_key",
        "candidate_lookup_customer_id",
        "candidate_individual_id",
        "candidate_entity_created_at",
    ]

    raw_links = seed_side.merge(
        candidate_side[candidate_columns],
        how="inner",
        on="emirates_id_number",
        validate="many_to_many",
    )

    active_seed_entity_keys = set(
        seed_resolution.seed_entities["entity_key"]
    )

    raw_links = raw_links.loc[
        ~raw_links["candidate_entity_key"].isin(
            active_seed_entity_keys
        )
    ].copy()

    raw_links = raw_links.loc[
        raw_links["seed_entity_key"]
        != raw_links["candidate_entity_key"]
    ].copy()

    logical_link_columns = [
        "snapshot_date",
        "seed_customer_id",
        "seed_source",
        "seed_entity_type",
        "seed_entity_id",
        "seed_entity_key",
        "seed_lookup_customer_id",
        "seed_entity_created_at",
        "candidate_entity_type",
        "candidate_entity_id",
        "candidate_entity_key",
        "candidate_lookup_customer_id",
        "candidate_entity_created_at",
        "emirates_id_number",
    ]

    if raw_links.empty:
        eid_links = pd.DataFrame(
            columns=[
                *logical_link_columns,
                "seed_individual_ids",
                "candidate_individual_ids",
                "edge_type",
                "reason_code",
                "deterministic_flag",
            ]
        )
    else:
        eid_links = (
            raw_links
            .groupby(
                logical_link_columns,
                as_index=False,
                dropna=False,
            )
            .agg(
                seed_individual_ids=(
                    "seed_individual_id",
                    _combine_individual_ids,
                ),
                candidate_individual_ids=(
                    "candidate_individual_id",
                    _combine_individual_ids,
                ),
            )
        )

        eid_links["edge_type"] = EID_EDGE_TYPE
        eid_links["reason_code"] = EID_REASON_CODE
        eid_links["deterministic_flag"] = True

        eid_links = (
            eid_links
            .sort_values(
                by=[
                    "seed_entity_key",
                    "candidate_entity_key",
                    "emirates_id_number",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    discovered_entities = (
        eid_links[
            [
                "candidate_entity_type",
                "candidate_entity_id",
                "candidate_entity_key",
                "candidate_lookup_customer_id",
                "candidate_entity_created_at",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            by="candidate_entity_key",
            kind="stable",
        )
        .reset_index(drop=True)
    )

    return EidDiscoveryResult(
        seed_resolution=seed_resolution,
        seed_eids=seed_eids,
        matching_identity_rows=matching_identity_rows,
        discovered_entities=discovered_entities,
        eid_links=eid_links,
    )
