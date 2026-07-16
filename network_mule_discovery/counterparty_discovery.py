"""Section 2 counterparty candidate discovery."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date

import pandas as pd

from network_mule_discovery.counterparty_data_sources import (
    CounterpartyNetworkDataSource,
)


CANDIDATE_STATUS = "CANDIDATE_NOT_EXPANDED"

TRANSFER_EVENT_TYPES = frozenset({
    "TRANSFER_RECEIVED",
    "TRANSFER_SENT",
})

BENEFICIARY_RELATIONSHIP_TYPE = (
    "BENEFICIARY_ADDED_SEED_ACCOUNT"
)

SHARED_COUNTERPARTY_RELATIONSHIP_TYPE = (
    "SHARED_EXTERNAL_COUNTERPARTY"
)


@dataclass(frozen=True)
class CounterpartyDiscoveryResult:
    """Outputs from Section 2 candidate discovery."""

    seed_cutoffs: pd.DataFrame
    seed_transfer_events: pd.DataFrame
    seed_counterparties: pd.DataFrame
    candidate_customer_links: pd.DataFrame
    candidate_counterparties: pd.DataFrame
    beneficiary_seed_links: pd.DataFrame


def _stable_id(
    prefix: str,
    *values: object,
) -> str:
    """Build a deterministic identifier."""
    canonical_value = "|".join(
        str(value)
        for value in values
    )

    digest = hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()[:16]

    return f"{prefix}{digest}"


def _combine_text_values(
    values: pd.Series,
) -> str | pd.NA:
    """Combine sorted unique nonblank text values."""
    normalized_values = sorted({
        str(value).strip()
        for value in values.dropna()
        if str(value).strip()
    })

    if not normalized_values:
        return pd.NA

    return "|".join(normalized_values)


def _sum_amounts(
    values: pd.Series,
) -> float | pd.NA:
    """Sum amounts while retaining missing-only groups as null."""
    numeric_values = pd.to_numeric(
        values,
        errors="coerce",
    )

    if numeric_values.notna().sum() == 0:
        return pd.NA

    return float(numeric_values.sum())


def _get_entity_resolution(
    data_source: CounterpartyNetworkDataSource,
    customer_ids: list[str],
    run_date: date | str,
) -> pd.DataFrame:
    """Resolve source customer IDs to unique graph entities."""
    identity_rows = (
        data_source.get_entities_by_lookup_customer_ids(
            lookup_customer_ids=customer_ids,
            run_date=run_date,
        )
    )

    return (
        identity_rows[
            [
                "entity_type",
                "entity_id",
                "entity_key",
                "lookup_customer_id",
                "entity_created_at",
            ]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "lookup_customer_id": "customer_id",
            }
        )
        .sort_values(
            by=[
                "customer_id",
                "entity_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def discover_counterparty_candidates(
    data_source: CounterpartyNetworkDataSource,
    run_date: date | str,
) -> CounterpartyDiscoveryResult:
    """
    Discover shared external counterparties and beneficiary-to-seed links.

    No counterparty is approved for graph expansion in Section 2.
    """
    seed_events = data_source.get_seed_mule_events(
        run_date
    )

    counterparty_events = (
        data_source.get_counterparty_events(
            run_date
        )
    )

    seed_customer_ids = sorted(
        seed_events["seed_customer_id"]
        .dropna()
        .unique()
        .tolist()
    )

    seed_entity_resolution = _get_entity_resolution(
        data_source=data_source,
        customer_ids=seed_customer_ids,
        run_date=run_date,
    ).rename(
        columns={
            "customer_id": "seed_customer_id",
            "entity_type": "seed_entity_type",
            "entity_id": "seed_entity_id",
            "entity_key": "seed_entity_key",
            "entity_created_at": (
                "seed_entity_created_at"
            ),
        }
    )

    seed_cutoffs = (
        seed_events
        .groupby(
            "seed_customer_id",
            as_index=False,
        )
        .agg(
            first_reported_date=(
                "date_reported",
                "min",
            ),
            latest_reported_date=(
                "date_reported",
                "max",
            ),
            seed_event_count=(
                "seed_event_id",
                "nunique",
            ),
            seed_event_ids=(
                "seed_event_id",
                _combine_text_values,
            ),
        )
        .merge(
            seed_entity_resolution,
            how="left",
            on="seed_customer_id",
            validate="one_to_many",
        )
        .sort_values(
            by=[
                "seed_customer_id",
                "seed_entity_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    transfer_events = counterparty_events.loc[
        counterparty_events["event_type"].isin(
            TRANSFER_EVENT_TYPES
        )
    ].copy()

    seed_transfer_events = (
        transfer_events
        .merge(
            seed_cutoffs[
                [
                    "seed_customer_id",
                    "seed_entity_type",
                    "seed_entity_id",
                    "seed_entity_key",
                    "first_reported_date",
                ]
            ],
            how="inner",
            left_on="customer_id",
            right_on="seed_customer_id",
            validate="many_to_many",
        )
    )

    seed_transfer_events = seed_transfer_events.loc[
        seed_transfer_events[
            "counterparty_key_usable_flag"
        ]
        & (
            seed_transfer_events[
                "event_timestamp"
            ].dt.date
            <= seed_transfer_events[
                "first_reported_date"
            ]
        )
    ].copy()

    seed_transfer_events = (
        seed_transfer_events
        .sort_values(
            by=[
                "seed_entity_key",
                "event_timestamp",
                "event_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    seed_counterparties = (
        seed_transfer_events
        .groupby(
            [
                "seed_customer_id",
                "seed_entity_type",
                "seed_entity_id",
                "seed_entity_key",
                "counterparty_key",
                "counterparty_key_type",
                "counterparty_key_quality",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            seed_event_count=(
                "event_id",
                "nunique",
            ),
            seed_event_ids=(
                "event_id",
                _combine_text_values,
            ),
            seed_event_types=(
                "event_type",
                _combine_text_values,
            ),
            rails=(
                "rail",
                _combine_text_values,
            ),
            seed_first_event_timestamp=(
                "event_timestamp",
                "min",
            ),
            seed_last_event_timestamp=(
                "event_timestamp",
                "max",
            ),
            seed_total_amount=(
                "amount",
                _sum_amounts,
            ),
            counterparty_names=(
                "counterparty_name",
                _combine_text_values,
            ),
            counterparty_countries=(
                "counterparty_country",
                _combine_text_values,
            ),
        )
    )

    candidate_transfer_events = transfer_events.loc[
        ~transfer_events["customer_id"].isin(
            seed_customer_ids
        )
        & transfer_events[
            "counterparty_key_usable_flag"
        ]
    ].copy()

    candidate_event_summary = (
        candidate_transfer_events
        .groupby(
            [
                "counterparty_key",
                "customer_id",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            candidate_event_count=(
                "event_id",
                "nunique",
            ),
            candidate_event_ids=(
                "event_id",
                _combine_text_values,
            ),
            candidate_event_types=(
                "event_type",
                _combine_text_values,
            ),
            candidate_rails=(
                "rail",
                _combine_text_values,
            ),
            candidate_first_event_timestamp=(
                "event_timestamp",
                "min",
            ),
            candidate_last_event_timestamp=(
                "event_timestamp",
                "max",
            ),
            candidate_total_amount=(
                "amount",
                _sum_amounts,
            ),
        )
        .rename(
            columns={
                "customer_id": (
                    "candidate_customer_id"
                ),
            }
        )
    )

    candidate_customer_ids = sorted(
        candidate_event_summary[
            "candidate_customer_id"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    candidate_entity_resolution = (
        _get_entity_resolution(
            data_source=data_source,
            customer_ids=candidate_customer_ids,
            run_date=run_date,
        )
        .rename(
            columns={
                "customer_id": (
                    "candidate_customer_id"
                ),
                "entity_type": (
                    "candidate_entity_type"
                ),
                "entity_id": (
                    "candidate_entity_id"
                ),
                "entity_key": (
                    "candidate_entity_key"
                ),
                "entity_created_at": (
                    "candidate_entity_created_at"
                ),
            }
        )
    )

    candidate_customer_links = (
        seed_counterparties
        .merge(
            candidate_event_summary,
            how="inner",
            on="counterparty_key",
            validate="many_to_many",
        )
        .merge(
            candidate_entity_resolution,
            how="left",
            on="candidate_customer_id",
            validate="many_to_many",
        )
    )

    candidate_customer_links = (
        candidate_customer_links.loc[
            candidate_customer_links[
                "seed_entity_key"
            ]
            != candidate_customer_links[
                "candidate_entity_key"
            ]
        ]
        .copy()
    )

    candidate_customer_links[
        "relationship_type"
    ] = SHARED_COUNTERPARTY_RELATIONSHIP_TYPE

    candidate_customer_links[
        "candidate_status"
    ] = CANDIDATE_STATUS

    candidate_customer_links[
        "expansion_allowed_flag"
    ] = False

    candidate_customer_links[
        "relationship_id"
    ] = candidate_customer_links.apply(
        lambda row: _stable_id(
            "CPR",
            row["seed_entity_key"],
            row["counterparty_key"],
            row["candidate_entity_key"],
            SHARED_COUNTERPARTY_RELATIONSHIP_TYPE,
        ),
        axis=1,
    )

    candidate_customer_links = (
        candidate_customer_links
        .sort_values(
            by=[
                "seed_entity_key",
                "counterparty_key",
                "candidate_entity_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    candidate_counterparties = (
        candidate_customer_links
        .groupby(
            [
                "seed_customer_id",
                "seed_entity_type",
                "seed_entity_id",
                "seed_entity_key",
                "counterparty_key",
                "counterparty_key_type",
                "counterparty_key_quality",
                "candidate_status",
                "expansion_allowed_flag",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            candidate_customer_count=(
                "candidate_entity_key",
                "nunique",
            ),
            candidate_customer_ids=(
                "candidate_customer_id",
                _combine_text_values,
            ),
            candidate_entity_keys=(
                "candidate_entity_key",
                _combine_text_values,
            ),
            seed_event_count=(
                "seed_event_count",
                "max",
            ),
            candidate_event_count=(
                "candidate_event_count",
                "sum",
            ),
            seed_first_event_timestamp=(
                "seed_first_event_timestamp",
                "min",
            ),
            seed_last_event_timestamp=(
                "seed_last_event_timestamp",
                "max",
            ),
            candidate_first_event_timestamp=(
                "candidate_first_event_timestamp",
                "min",
            ),
            candidate_last_event_timestamp=(
                "candidate_last_event_timestamp",
                "max",
            ),
            counterparty_names=(
                "counterparty_names",
                _combine_text_values,
            ),
            counterparty_countries=(
                "counterparty_countries",
                _combine_text_values,
            ),
        )
    )

    candidate_counterparties[
        "counterparty_candidate_id"
    ] = candidate_counterparties.apply(
        lambda row: _stable_id(
            "CPC",
            row["seed_entity_key"],
            row["counterparty_key"],
        ),
        axis=1,
    )

    beneficiary_events = (
        counterparty_events.loc[
            (
                counterparty_events["event_type"]
                == "BENEFICIARY_ADDED"
            )
            & counterparty_events[
                "counterparty_account_match_key"
            ].notna()
        ]
        .copy()
    )

    seed_account_records = (
        seed_events.loc[
            seed_events[
                "seed_account_match_key"
            ].notna(),
            [
                "seed_event_id",
                "seed_customer_id",
                "date_reported",
                "frc_rail",
                "seed_account_match_key",
                "seed_account_number_normalized",
                "seed_iban_normalized",
            ],
        ]
        .merge(
            seed_entity_resolution,
            how="left",
            on="seed_customer_id",
            validate="many_to_many",
        )
    )

    beneficiary_seed_links = (
        beneficiary_events
        .merge(
            seed_account_records,
            how="inner",
            left_on=(
                "counterparty_account_match_key"
            ),
            right_on="seed_account_match_key",
            validate="many_to_many",
        )
        .rename(
            columns={
                "customer_id": (
                    "candidate_customer_id"
                ),
                "event_id": (
                    "beneficiary_event_id"
                ),
                "event_timestamp": (
                    "beneficiary_added_timestamp"
                ),
                "status": (
                    "beneficiary_status"
                ),
            }
        )
    )

    beneficiary_candidate_ids = sorted(
        beneficiary_seed_links[
            "candidate_customer_id"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    beneficiary_entity_resolution = (
        _get_entity_resolution(
            data_source=data_source,
            customer_ids=beneficiary_candidate_ids,
            run_date=run_date,
        )
        .rename(
            columns={
                "customer_id": (
                    "candidate_customer_id"
                ),
                "entity_type": (
                    "candidate_entity_type"
                ),
                "entity_id": (
                    "candidate_entity_id"
                ),
                "entity_key": (
                    "candidate_entity_key"
                ),
                "entity_created_at": (
                    "candidate_entity_created_at"
                ),
            }
        )
    )

    beneficiary_seed_links = (
        beneficiary_seed_links
        .merge(
            beneficiary_entity_resolution,
            how="left",
            on="candidate_customer_id",
            validate="many_to_many",
        )
    )

    beneficiary_seed_links = (
        beneficiary_seed_links.loc[
            beneficiary_seed_links[
                "candidate_entity_key"
            ]
            != beneficiary_seed_links[
                "seed_entity_key"
            ]
        ]
        .copy()
    )

    beneficiary_seed_links[
        "relationship_type"
    ] = BENEFICIARY_RELATIONSHIP_TYPE

    beneficiary_seed_links[
        "candidate_status"
    ] = CANDIDATE_STATUS

    beneficiary_seed_links[
        "expansion_allowed_flag"
    ] = False

    beneficiary_seed_links[
        "relationship_id"
    ] = beneficiary_seed_links.apply(
        lambda row: _stable_id(
            "BSR",
            row["seed_entity_key"],
            row["candidate_entity_key"],
            row["beneficiary_event_id"],
            row["seed_account_match_key"],
        ),
        axis=1,
    )

    beneficiary_seed_links = (
        beneficiary_seed_links
        .drop_duplicates(
            subset=["relationship_id"]
        )
        .sort_values(
            by=[
                "seed_entity_key",
                "candidate_entity_key",
                "beneficiary_event_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    return CounterpartyDiscoveryResult(
        seed_cutoffs=seed_cutoffs,
        seed_transfer_events=seed_transfer_events,
        seed_counterparties=seed_counterparties,
        candidate_customer_links=(
            candidate_customer_links
        ),
        candidate_counterparties=(
            candidate_counterparties
        ),
        beneficiary_seed_links=(
            beneficiary_seed_links
        ),
    )
