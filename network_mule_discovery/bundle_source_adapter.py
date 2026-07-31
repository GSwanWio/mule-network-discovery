"""Convert a validated source bundle into canonical discovery inputs."""

from __future__ import annotations

import pandas as pd

from network_mule_discovery.counterparty_schemas import (
    COUNTERPARTY_EVENT_REQUIRED_COLUMNS,
    prepare_counterparty_events,
)
from network_mule_discovery.raw_source_adapter import (
    CanonicalDiscoveryInputs,
    RawSourceAdapterError,
    _beneficiary_union,
    _build_beneficiary_events,
    _build_inward_events,
    _build_outward_events,
    _completed_mask,
    _customer_account_lookup,
    _on_or_before_run_date_mask,
    _prepare_customer_identity_raw,
    _prepare_seed_mule_events_raw,
    _prepare_seed_mules_raw,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.source_contracts import (
    DiscoverySourceBundle,
)


def _normalized_identifier(
    values: pd.Series,
) -> pd.Series:
    return (
        values.astype("string")
        .fillna("")
        .str.upper()
        .str.replace(
            r"[^0-9A-Z]",
            "",
            regex=True,
        )
    )


def _international_beneficiary_lookup(
    beneficiaries: pd.DataFrame,
) -> pd.DataFrame:
    lookup = beneficiaries.copy()

    lookup["beneficiary_join_key"] = (
        _normalized_identifier(
            lookup["swift_code"]
        )
        + "|"
        + _normalized_identifier(
            lookup["beneficiary_account_number"]
        )
    )

    usable = (
        _normalized_identifier(
            lookup["swift_code"]
        ).ne("")
        & _normalized_identifier(
            lookup["beneficiary_account_number"]
        ).ne("")
    )

    lookup = lookup.loc[usable].copy()

    return (
        lookup.sort_values(
            by=[
                "customer_id",
                "beneficiary_join_key",
                "beneficiary_updated_date",
                "beneficiary_created_date",
                "beneficiary_id",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "customer_id",
                "beneficiary_join_key",
            ],
            keep="last",
        )
        [
            [
                "customer_id",
                "beneficiary_join_key",
                "beneficiary_account_holder_name",
                "country_of_beneficiary",
            ]
        ]
        .rename(
            columns={
                "beneficiary_account_holder_name": (
                    "resolved_counterparty_name"
                ),
                "country_of_beneficiary": (
                    "resolved_counterparty_country"
                ),
            }
        )
        .reset_index(drop=True)
    )


def _build_international_outward_events(
    *,
    outward: pd.DataFrame,
    accounts: pd.DataFrame,
    beneficiaries: pd.DataFrame,
    run_date,
) -> pd.DataFrame:
    completed = outward.loc[
        _completed_mask(outward)
        & _on_or_before_run_date_mask(
            frame=outward,
            timestamp_column="transaction_timestamp",
            run_date=run_date,
            dataset_name=(
                "international_outward_payments"
            ),
        )
    ].copy()

    account_enrichment = accounts.rename(
        columns={
            "account_number": (
                "resolved_wio_account_number"
            ),
            "iban": "resolved_wio_iban",
        }
    )

    completed = completed.merge(
        account_enrichment,
        how="left",
        left_on="source_account_id",
        right_on="account_id",
        validate="many_to_one",
        suffixes=("", "_account"),
    )

    missing_account = (
        completed["resolved_wio_account_number"].isna()
        | completed["resolved_wio_iban"].isna()
    )

    if missing_account.any():
        missing_ids = (
            completed.loc[
                missing_account,
                "source_account_id",
            ]
            .drop_duplicates()
            .tolist()
        )
        raise RawSourceAdapterError(
            "International outward payments reference "
            f"unknown Wio account IDs: {missing_ids}"
        )

    completed["beneficiary_join_key"] = (
        _normalized_identifier(
            completed["beneficiary_swift_code"]
        )
        + "|"
        + _normalized_identifier(
            completed[
                "beneficiary_account_number"
            ]
        )
    )

    completed = completed.merge(
        _international_beneficiary_lookup(
            beneficiaries
        ),
        how="left",
        on=[
            "customer_id",
            "beneficiary_join_key",
        ],
        validate="many_to_one",
    )

    target_country = (
        completed["target_country_code"]
        .astype("string")
        .fillna("")
        .str.strip()
    )

    return pd.DataFrame(
        {
            "snapshot_date": str(run_date),
            "event_id": (
                "INTERNATIONAL_OUTWARD|"
                + completed[
                    "transfer_id"
                ].astype("string")
            ),
            "event_type": "TRANSFER_SENT",
            "rail": "INTERNATIONAL",
            "customer_id": completed["customer_id"],
            "event_timestamp": completed[
                "transaction_timestamp"
            ],
            "transfer_id": completed["transfer_id"],
            "reference_number": completed[
                "reference_number"
            ],
            "beneficiary_id": completed[
                "beneficiary_id"
            ],
            "status": completed["status"],
            "wio_account_number": completed[
                "resolved_wio_account_number"
            ],
            "wio_iban": completed[
                "resolved_wio_iban"
            ],
            "counterparty_account_number": completed[
                "beneficiary_account_number"
            ],
            "counterparty_iban": "",
            "counterparty_swift_bic": completed[
                "beneficiary_swift_code"
            ],
            "counterparty_name": completed[
                "resolved_counterparty_name"
            ].fillna(""),
            "counterparty_country": (
                target_country.where(
                    target_country.ne(""),
                    completed[
                        "resolved_counterparty_country"
                    ].fillna(""),
                )
            ),
            "amount": completed["target_amount"],
            "currency": completed[
                "target_currency"
            ],
            "source_table": (
                "international_outward_payments"
            ),
        }
    )


def _build_international_inward_events(
    *,
    inward: pd.DataFrame,
    accounts: pd.DataFrame,
    run_date,
) -> pd.DataFrame:
    completed = inward.loc[
        _completed_mask(inward)
        & _on_or_before_run_date_mask(
            frame=inward,
            timestamp_column="transaction_timestamp",
            run_date=run_date,
            dataset_name=(
                "international_inward_payments"
            ),
        )
    ].copy()

    account_enrichment = accounts.rename(
        columns={
            "account_number": (
                "resolved_wio_account_number"
            ),
            "iban": "resolved_wio_iban",
        }
    )

    completed = completed.merge(
        account_enrichment,
        how="left",
        on="customer_id",
        validate="many_to_one",
    )

    missing_account = (
        completed["resolved_wio_account_number"].isna()
        | completed["resolved_wio_iban"].isna()
    )

    if missing_account.any():
        missing_customers = (
            completed.loc[
                missing_account,
                "customer_id",
            ]
            .drop_duplicates()
            .tolist()
        )
        raise RawSourceAdapterError(
            "International inward payments reference "
            "customers without an active Wio account: "
            f"{missing_customers}"
        )

    return pd.DataFrame(
        {
            "snapshot_date": str(run_date),
            "event_id": (
                "INTERNATIONAL_INWARD|"
                + completed[
                    "transfer_id"
                ].astype("string")
            ),
            "event_type": "TRANSFER_RECEIVED",
            "rail": "INTERNATIONAL",
            "customer_id": completed["customer_id"],
            "event_timestamp": completed[
                "transaction_timestamp"
            ],
            "transfer_id": completed["transfer_id"],
            "reference_number": completed[
                "reference_number"
            ],
            "beneficiary_id": "",
            "status": completed["status"],
            "wio_account_number": completed[
                "resolved_wio_account_number"
            ],
            "wio_iban": completed[
                "resolved_wio_iban"
            ],
            "counterparty_account_number": "",
            "counterparty_iban": completed[
                "source_customer_iban"
            ],
            "counterparty_swift_bic": "",
            "counterparty_name": "",
            "counterparty_country": completed[
                "source_country_code"
            ],
            "amount": completed["source_amount"],
            "currency": completed[
                "source_currency"
            ],
            "source_table": (
                "international_inward_payments"
            ),
        }
    )


def _build_bundle_beneficiary_events(
    *,
    beneficiaries: pd.DataFrame,
    accounts: pd.DataFrame,
    run_date,
) -> pd.DataFrame:
    events = _build_beneficiary_events(
        beneficiaries=beneficiaries,
        accounts=accounts,
        run_date=run_date,
    )

    rail_lookup = beneficiaries[
        [
            "customer_id",
            "beneficiary_id",
            "beneficiary_type",
        ]
    ].copy()

    rail_lookup["rail"] = "LOCAL"

    international = (
        rail_lookup["beneficiary_type"]
        .astype("string")
        .str.strip()
        .str.upper()
        .eq("INTERNATIONAL")
    )

    rail_lookup.loc[
        international,
        "rail",
    ] = "INTERNATIONAL"

    events = (
        events.drop(columns=["rail"])
        .merge(
            rail_lookup,
            how="left",
            on=[
                "customer_id",
                "beneficiary_id",
            ],
            validate="many_to_one",
        )
    )

    events["event_id"] = (
        "BENEFICIARY|"
        + events["customer_id"].astype("string")
        + "|"
        + events["beneficiary_id"].astype("string")
    )

    return events


def _prepare_bundle_counterparty_events(
    *,
    source_bundle: DiscoverySourceBundle,
    beneficiaries: pd.DataFrame,
    run_date,
) -> pd.DataFrame:
    account_lookup = _customer_account_lookup(
        source_bundle.customer_account_master
    )

    events = pd.concat(
        [
            _build_outward_events(
                outward=(
                    source_bundle.local_outward_payments
                ),
                accounts=account_lookup,
                beneficiaries=beneficiaries,
                run_date=run_date,
            ),
            _build_inward_events(
                inward=(
                    source_bundle.local_inward_payments
                ),
                accounts=account_lookup,
                run_date=run_date,
            ),
            _build_international_outward_events(
                outward=(
                    source_bundle
                    .international_outward_payments
                ),
                accounts=account_lookup,
                beneficiaries=beneficiaries,
                run_date=run_date,
            ),
            _build_international_inward_events(
                inward=(
                    source_bundle
                    .international_inward_payments
                ),
                accounts=account_lookup,
                run_date=run_date,
            ),
            _build_bundle_beneficiary_events(
                beneficiaries=beneficiaries,
                accounts=account_lookup,
                run_date=run_date,
            ),
        ],
        ignore_index=True,
    )

    events = events[
        list(COUNTERPARTY_EVENT_REQUIRED_COLUMNS)
    ].copy()

    prepare_counterparty_events(events)

    return (
        events.sort_values(
            by=[
                "event_timestamp",
                "event_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def build_canonical_discovery_inputs_from_bundle(
    source_bundle: DiscoverySourceBundle,
) -> CanonicalDiscoveryInputs:
    """Build canonical frames from one validated source bundle."""
    if not isinstance(
        source_bundle,
        DiscoverySourceBundle,
    ):
        raise RawSourceAdapterError(
            "source_bundle must be a "
            "DiscoverySourceBundle."
        )

    run_date = parse_run_date(
        source_bundle.metadata.run_date
    )

    seed_pool = source_bundle.seed_mule_pool

    snapshot_dates = sorted(
        seed_pool["snapshot_date"]
        .astype("string")
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    if snapshot_dates != [str(run_date)]:
        raise RawSourceAdapterError(
            "seed_mule_pool snapshot_date must match "
            f"the source run date. Found {snapshot_dates}; "
            f"expected {run_date}."
        )

    beneficiaries = _beneficiary_union(
        source_bundle.retail_beneficiary_master,
        source_bundle.sme_beneficiary_master,
    )

    return CanonicalDiscoveryInputs(
        seed_mules=_prepare_seed_mules_raw(
            seed_pool
        ),
        customer_identity=(
            _prepare_customer_identity_raw(
                source_bundle.customer_identity,
                run_date,
            )
        ),
        seed_mule_events=(
            _prepare_seed_mule_events_raw(
                seed_pool
            )
        ),
        counterparty_events=(
            _prepare_bundle_counterparty_events(
                source_bundle=source_bundle,
                beneficiaries=beneficiaries,
                run_date=run_date,
            )
        ),
    )
