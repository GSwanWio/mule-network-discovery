"""Provider-neutral and currency-safe behavioral evidence sources."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from network_mule_discovery.behavioral_features import (
    BehavioralFeatureError,
    build_behavioral_features,
)
from network_mule_discovery.decision_policy import (
    COUNTERPARTY_ASSESSMENT_POLICY_VERSION,
)
from network_mule_discovery.counterparty_schemas import (
    build_counterparty_identity,
)
from network_mule_discovery.raw_source_adapter import (
    RawDiscoverySources,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.source_contracts import (
    DiscoverySourceBundle,
)


CUSTOMER_ACTIVITY_COLUMNS = (
    "run_date",
    "customer_id",
    "direction",
    "currency",
    "event_count",
    "total_amount",
    "distinct_counterparty_count",
)

COUNTERPARTY_ACTIVITY_COLUMNS = (
    "run_date",
    "counterparty_key",
    "currency",
    "event_count",
    "total_amount",
    "distinct_customer_count",
)


@dataclass(frozen=True)
class BundleBehavioralSources:
    """Local feature sources plus currency-safe international evidence."""

    raw_sources: RawDiscoverySources
    international_customer_currency_activity: pd.DataFrame
    international_counterparty_currency_activity: pd.DataFrame


def _completed_before_run_date(
    *,
    frame: pd.DataFrame,
    run_date,
    dataset_name: str,
) -> pd.DataFrame:
    result = frame.copy()

    result["transaction_at"] = pd.to_datetime(
        result["transaction_timestamp"],
        errors="coerce",
    )

    if result["transaction_at"].isna().any():
        raise BehavioralFeatureError(
            f"{dataset_name}.transaction_timestamp "
            "contains invalid values."
        )

    cutoff = (
        pd.Timestamp(run_date)
        + pd.Timedelta(days=1)
    )

    return result.loc[
        result["status"]
        .astype("string")
        .str.strip()
        .str.upper()
        .eq("COMPLETED")
        & result["transaction_at"].lt(cutoff)
    ].copy()


def _prepare_amount_and_currency(
    *,
    frame: pd.DataFrame,
    amount_column: str,
    currency_column: str,
    dataset_name: str,
) -> pd.DataFrame:
    result = frame.copy()

    result["amount_value"] = pd.to_numeric(
        result[amount_column],
        errors="coerce",
    )

    if result["amount_value"].isna().any():
        raise BehavioralFeatureError(
            f"{dataset_name}.{amount_column} "
            "contains invalid values."
        )

    result["amount_currency"] = (
        result[currency_column]
        .astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
    )

    if result["amount_currency"].eq("").any():
        raise BehavioralFeatureError(
            f"{dataset_name}.{currency_column} "
            "contains blank values."
        )

    return result


def _customer_currency_activity(
    *,
    inward: pd.DataFrame,
    outward: pd.DataFrame,
    run_date,
) -> pd.DataFrame:
    inward_activity = inward[
        [
            "customer_id",
            "source_customer_iban",
            "amount_currency",
            "amount_value",
        ]
    ].copy()

    inward_activity["direction"] = "INWARD"
    inward_activity["counterparty_reference"] = (
        inward_activity["source_customer_iban"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )

    outward_activity = outward[
        [
            "customer_id",
            "counterparty_key",
            "amount_currency",
            "amount_value",
        ]
    ].copy()

    outward_activity["direction"] = "OUTWARD"
    outward_activity["counterparty_reference"] = (
        outward_activity["counterparty_key"]
    )

    activity = pd.concat(
        [
            inward_activity[
                [
                    "customer_id",
                    "direction",
                    "amount_currency",
                    "amount_value",
                    "counterparty_reference",
                ]
            ],
            outward_activity[
                [
                    "customer_id",
                    "direction",
                    "amount_currency",
                    "amount_value",
                    "counterparty_reference",
                ]
            ],
        ],
        ignore_index=True,
    )

    if activity.empty:
        return pd.DataFrame(
            columns=CUSTOMER_ACTIVITY_COLUMNS
        )

    result = (
        activity.groupby(
            [
                "customer_id",
                "direction",
                "amount_currency",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            event_count=(
                "amount_value",
                "size",
            ),
            total_amount=(
                "amount_value",
                "sum",
            ),
            distinct_counterparty_count=(
                "counterparty_reference",
                "nunique",
            ),
        )
        .rename(
            columns={
                "amount_currency": "currency",
            }
        )
    )

    result.insert(
        0,
        "run_date",
        str(run_date),
    )

    result["total_amount"] = (
        result["total_amount"]
        .astype(float)
        .round(2)
    )

    return result[
        list(CUSTOMER_ACTIVITY_COLUMNS)
    ].sort_values(
        by=[
            "customer_id",
            "direction",
            "currency",
        ],
        kind="stable",
    ).reset_index(drop=True)


def _counterparty_currency_activity(
    *,
    outward: pd.DataFrame,
    run_date,
) -> pd.DataFrame:
    if outward.empty:
        return pd.DataFrame(
            columns=COUNTERPARTY_ACTIVITY_COLUMNS
        )

    result = (
        outward.groupby(
            [
                "counterparty_key",
                "amount_currency",
            ],
            as_index=False,
        )
        .agg(
            event_count=(
                "amount_value",
                "size",
            ),
            total_amount=(
                "amount_value",
                "sum",
            ),
            distinct_customer_count=(
                "customer_id",
                "nunique",
            ),
        )
        .rename(
            columns={
                "amount_currency": "currency",
            }
        )
    )

    result.insert(
        0,
        "run_date",
        str(run_date),
    )

    result["total_amount"] = (
        result["total_amount"]
        .astype(float)
        .round(2)
    )

    return result[
        list(COUNTERPARTY_ACTIVITY_COLUMNS)
    ].sort_values(
        by=[
            "counterparty_key",
            "currency",
        ],
        kind="stable",
    ).reset_index(drop=True)


def build_bundle_behavioral_sources(
    source_bundle: DiscoverySourceBundle,
) -> BundleBehavioralSources:
    """Prepare local sources and separated international evidence."""
    if not isinstance(
        source_bundle,
        DiscoverySourceBundle,
    ):
        raise BehavioralFeatureError(
            "source_bundle must be a "
            "DiscoverySourceBundle."
        )

    run_date = parse_run_date(
        source_bundle.metadata.run_date
    )

    raw_sources = RawDiscoverySources(
        seed_mule_pool=(
            source_bundle.seed_mule_pool.copy()
        ),
        customer_identity=(
            source_bundle.customer_identity.copy()
        ),
        customer_accounts=(
            source_bundle.customer_account_master.copy()
        ),
        local_inward_payments=(
            source_bundle.local_inward_payments.copy()
        ),
        local_outward_payments=(
            source_bundle.local_outward_payments.copy()
        ),
        retail_beneficiaries=(
            source_bundle.retail_beneficiary_master.copy()
        ),
        sme_beneficiaries=(
            source_bundle.sme_beneficiary_master.copy()
        ),
    )

    inward = _completed_before_run_date(
        frame=(
            source_bundle
            .international_inward_payments
        ),
        run_date=run_date,
        dataset_name=(
            "international_inward_payments"
        ),
    )

    inward = _prepare_amount_and_currency(
        frame=inward,
        amount_column="target_amount",
        currency_column="target_currency",
        dataset_name=(
            "international_inward_payments"
        ),
    )

    outward = _completed_before_run_date(
        frame=(
            source_bundle
            .international_outward_payments
        ),
        run_date=run_date,
        dataset_name=(
            "international_outward_payments"
        ),
    )

    outward = _prepare_amount_and_currency(
        frame=outward,
        amount_column="source_amount",
        currency_column="source_currency",
        dataset_name=(
            "international_outward_payments"
        ),
    )

    outward["counterparty_key"] = [
        build_counterparty_identity(
            rail="INTERNATIONAL",
            counterparty_iban="",
            counterparty_swift_bic=(
                row.beneficiary_swift_code
            ),
            counterparty_account_number=(
                row.beneficiary_account_number
            ),
        ).counterparty_key
        for row in outward.itertuples(index=False)
    ]

    return BundleBehavioralSources(
        raw_sources=raw_sources,
        international_customer_currency_activity=(
            _customer_currency_activity(
                inward=inward,
                outward=outward,
                run_date=run_date,
            )
        ),
        international_counterparty_currency_activity=(
            _counterparty_currency_activity(
                outward=outward,
                run_date=run_date,
            )
        ),
    )

def build_bundle_counterparty_payloads(
    *,
    source_bundle: DiscoverySourceBundle,
    counterparty_keys: Iterable[str],
) -> pd.DataFrame:
    """Build local and international counterparty AI payloads."""
    requested_keys = sorted({
        str(value).strip()
        for value in counterparty_keys
        if value is not None
        and str(value).strip()
    })

    if not requested_keys:
        raise BehavioralFeatureError(
            "At least one counterparty key is required."
        )

    sources = build_bundle_behavioral_sources(
        source_bundle
    )
    run_date = parse_run_date(
        source_bundle.metadata.run_date
    )

    local_outward = _completed_before_run_date(
        frame=(
            sources.raw_sources
            .local_outward_payments
        ),
        run_date=run_date,
        dataset_name="local_outward_payments",
    )

    local_available_keys = {
        build_counterparty_identity(
            rail="LOCAL",
            counterparty_iban="",
            counterparty_swift_bic="",
            counterparty_account_number=value,
        ).counterparty_key
        for value in local_outward[
            "beneficiary_account_number"
        ]
    }

    local_keys = sorted(
        set(requested_keys)
        & local_available_keys
    )

    payload_frames: list[pd.DataFrame] = []

    if local_keys:
        local_result = build_behavioral_features(
            counterparty_keys=local_keys,
            run_date=run_date,
            raw_sources=sources.raw_sources,
            international_counterparty_currency_activity=(
                sources
                .international_counterparty_currency_activity
            ),
        )

        payload_frames.append(
            local_result.counterparty_payloads
        )

    remaining_keys = sorted(
        set(requested_keys) - set(local_keys)
    )

    international_activity = (
        sources
        .international_counterparty_currency_activity
    )

    international_records = []

    for counterparty_key in remaining_keys:
        current = international_activity.loc[
            international_activity[
                "counterparty_key"
            ]
            .astype("string")
            .str.strip()
            .eq(counterparty_key)
        ].sort_values(
            by=["currency"],
            kind="stable",
        )

        if current.empty:
            raise BehavioralFeatureError(
                "No completed local or international "
                "transfer evidence exists for "
                f"{counterparty_key}."
            )

        currency_activity = [
            {
                "currency": str(row.currency),
                "event_count": int(
                    row.event_count
                ),
                "total_amount": float(
                    row.total_amount
                ),
                "distinct_customer_count": int(
                    row.distinct_customer_count
                ),
            }
            for row in current.itertuples(
                index=False
            )
        ]

        payload = {
            "subject_type": "COUNTERPARTY",
            "subject_key": counterparty_key,
            "run_date": str(run_date),
            "counterparty_assessment_policy_version": (
                COUNTERPARTY_ASSESSMENT_POLICY_VERSION
            ),
            "evidence_scope": (
                "INTERNATIONAL_CURRENCY_SUMMARY"
            ),
            "international_currency_activity": (
                currency_activity
            ),
            "aggregate_behavior": {
                "currency_group_count": len(
                    currency_activity
                ),
                "transfer_event_count": sum(
                    record["event_count"]
                    for record in currency_activity
                ),
                "amounts_kept_separate_by_currency": (
                    True
                ),
            },
        }

        international_records.append({
            "subject_type": "COUNTERPARTY",
            "subject_key": counterparty_key,
            "feature_payload_json": json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        })

    if international_records:
        payload_frames.append(
            pd.DataFrame(
                international_records
            )
        )

    result = pd.concat(
        payload_frames,
        ignore_index=True,
    )

    duplicate_mask = result.duplicated(
        subset=[
            "subject_type",
            "subject_key",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise BehavioralFeatureError(
            "Counterparty payload generation "
            "produced duplicate subjects."
        )

    return (
        result.sort_values(
            by=[
                "subject_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )
