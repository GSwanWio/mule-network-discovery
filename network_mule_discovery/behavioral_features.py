"""Build neutral behavioral evidence from production-shaped sources."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from network_mule_discovery.counterparty_schemas import (
    build_counterparty_identity,
)
from network_mule_discovery.decision_policy import (
    COUNTERPARTY_ASSESSMENT_POLICY_VERSION,
)
from network_mule_discovery.raw_source_adapter import (
    RawDiscoverySources,
    load_raw_discovery_sources,
)
from network_mule_discovery.schemas import parse_run_date


COUNTERPARTY_PROFILE_FILENAME = "counterparty_behavior_profiles.csv"
COUNTERPARTY_CUSTOMER_PROFILE_FILENAME = (
    "counterparty_customer_behavior_profiles.csv"
)
COUNTERPARTY_PAYLOAD_FILENAME = "counterparty_feature_payloads.csv"
COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT = 10
COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_METHOD = (
    "HIGHEST_COUNTERPARTY_TRANSFER_AMOUNT_THEN_CUSTOMER_ID"
)


class BehavioralFeatureError(RuntimeError):
    """Behavioral evidence cannot be built safely."""


@dataclass(frozen=True)
class BehavioralFeatureResult:
    """Neutral counterparty and linked-customer evidence."""

    counterparty_profiles: pd.DataFrame
    counterparty_customer_profiles: pd.DataFrame
    counterparty_payloads: pd.DataFrame


def _safe_ratio(
    numerator: float,
    denominator: float,
) -> float:
    if denominator <= 0:
        return 0.0

    return float(numerator / denominator)


def _round(value: object, digits: int = 6) -> float:
    if pd.isna(value):
        return 0.0

    return round(float(value), digits)


def _inclusive_cutoff(run_date: date) -> pd.Timestamp:
    return pd.Timestamp(run_date) + pd.Timedelta(days=1)


def _records_digest(
    records: list[dict[str, object]],
) -> str:
    """Hash a complete evidence population without exposing all rows."""
    canonical_json = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


def _prepare_sources(
    sources: RawDiscoverySources,
    run_date: date,
) -> dict[str, pd.DataFrame]:
    cutoff = _inclusive_cutoff(run_date)

    identity = sources.customer_identity.copy()
    accounts = sources.customer_accounts.copy()
    inward = sources.local_inward_payments.copy()
    outward = sources.local_outward_payments.copy()
    retail_beneficiaries = sources.retail_beneficiaries.copy()
    sme_beneficiaries = sources.sme_beneficiaries.rename(
        columns={"business_id": "customer_id"}
    ).copy()
    seed_pool = sources.seed_mule_pool.copy()

    identity["customer_created_at"] = pd.to_datetime(
        identity["customer_created_date"],
        errors="coerce",
    )
    accounts["account_opened_at"] = pd.to_datetime(
        accounts["account_opened_date"],
        errors="coerce",
    )
    inward["transaction_at"] = pd.to_datetime(
        inward["transaction_timestamp"],
        errors="coerce",
    )
    outward["transaction_at"] = pd.to_datetime(
        outward["transaction_timestamp"],
        errors="coerce",
    )

    beneficiaries = pd.concat(
        [retail_beneficiaries, sme_beneficiaries],
        ignore_index=True,
        sort=False,
    )
    beneficiaries["beneficiary_created_at"] = pd.to_datetime(
        beneficiaries["beneficiary_created_date"],
        errors="coerce",
    )

    validation_columns = {
        "customer_identity.customer_created_date": identity[
            "customer_created_at"
        ],
        "customer_account_master.account_opened_date": accounts[
            "account_opened_at"
        ],
        "local_inward_payments.transaction_timestamp": inward[
            "transaction_at"
        ],
        "local_outward_payments.transaction_timestamp": outward[
            "transaction_at"
        ],
        "beneficiary_master.beneficiary_created_date": beneficiaries[
            "beneficiary_created_at"
        ],
    }

    for label, values in validation_columns.items():
        if values.isna().any():
            raise BehavioralFeatureError(
                f"{label} contains invalid timestamps."
            )

    inward = inward.loc[
        inward["status"].astype("string").str.upper().eq("COMPLETED")
        & inward["transaction_at"].lt(cutoff)
    ].copy()
    outward = outward.loc[
        outward["status"].astype("string").str.upper().eq("COMPLETED")
        & outward["transaction_at"].lt(cutoff)
    ].copy()
    beneficiaries = beneficiaries.loc[
        beneficiaries["beneficiary_created_at"].lt(cutoff)
    ].copy()

    inward["amount_value"] = pd.to_numeric(
        inward["source_amount"],
        errors="coerce",
    )
    outward["amount_value"] = pd.to_numeric(
        outward["target_amount"],
        errors="coerce",
    )

    if inward["amount_value"].isna().any():
        raise BehavioralFeatureError(
            "local_inward_payments.source_amount contains invalid values."
        )

    if outward["amount_value"].isna().any():
        raise BehavioralFeatureError(
            "local_outward_payments.target_amount contains invalid values."
        )

    outward["counterparty_key"] = outward[
        "beneficiary_account_number"
    ].map(
        lambda value: build_counterparty_identity(
            rail="LOCAL",
            counterparty_iban="",
            counterparty_swift_bic="",
            counterparty_account_number=value,
        ).counterparty_key
    )

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

    identity_lookup = identity[
        [
            "customer_id",
            "entity_type",
            "entity_id",
            "customer_segment",
            "customer_created_at",
        ]
    ].drop_duplicates(subset=["customer_id"])

    account_lookup = (
        accounts.loc[
            accounts["account_status"]
            .astype("string")
            .str.upper()
            .eq("ACTIVE")
        ]
        .sort_values(
            by=["customer_id", "account_opened_at", "account_id"],
            kind="stable",
        )
        .drop_duplicates(subset=["customer_id"], keep="first")
        [
            [
                "customer_id",
                "account_id",
                "account_opened_at",
                "account_currency",
            ]
        ]
    )

    customer_lookup = identity_lookup.merge(
        account_lookup,
        how="left",
        on="customer_id",
        validate="one_to_one",
    )

    if customer_lookup["account_id"].isna().any():
        raise BehavioralFeatureError(
            "One or more customers have no active account."
        )

    seed_customer_ids = set(
        seed_pool["seed_customer_id"]
        .astype("string")
        .str.strip()
        .tolist()
    )

    return {
        "identity": identity,
        "accounts": accounts,
        "inward": inward,
        "outward": outward,
        "beneficiaries": beneficiaries,
        "customer_lookup": customer_lookup,
        "seed_customer_ids": seed_customer_ids,
    }


def _customer_window_metrics(
    customer_id: str,
    inward: pd.DataFrame,
    outward: pd.DataFrame,
    beneficiaries: pd.DataFrame,
    run_date: date,
) -> dict[str, object]:
    cutoff = _inclusive_cutoff(run_date)
    current_inward = inward.loc[inward["customer_id"].eq(customer_id)]
    current_outward = outward.loc[outward["customer_id"].eq(customer_id)]
    current_beneficiaries = beneficiaries.loc[
        beneficiaries["customer_id"].eq(customer_id)
    ]

    metrics: dict[str, object] = {}

    for days in (7, 30, 90, 365):
        start = cutoff - pd.Timedelta(days=days)
        inward_window = current_inward.loc[
            current_inward["transaction_at"].ge(start)
        ]
        outward_window = current_outward.loc[
            current_outward["transaction_at"].ge(start)
        ]

        inward_amount = float(inward_window["amount_value"].sum())
        outward_amount = float(outward_window["amount_value"].sum())

        metrics[f"inward_event_count_{days}d"] = len(inward_window)
        metrics[f"inward_amount_{days}d"] = _round(inward_amount, 2)
        metrics[f"outward_event_count_{days}d"] = len(outward_window)
        metrics[f"outward_amount_{days}d"] = _round(outward_amount, 2)
        metrics[f"flow_through_ratio_{days}d"] = _round(
            _safe_ratio(outward_amount, inward_amount)
        )

    recent_start = cutoff - pd.Timedelta(days=30)
    recent_inward = current_inward.loc[
        current_inward["transaction_at"].ge(recent_start)
    ]
    recent_outward = current_outward.loc[
        current_outward["transaction_at"].ge(recent_start)
    ]

    rapid_outward = []

    for row in recent_outward.itertuples(index=False):
        matching_inward = recent_inward.loc[
            recent_inward["transaction_at"].le(row.transaction_at)
            & recent_inward["transaction_at"].ge(
                row.transaction_at - pd.Timedelta(hours=2)
            )
        ]

        if not matching_inward.empty:
            rapid_outward.append(float(row.amount_value))

    rapid_amount = sum(rapid_outward)
    recent_outward_amount = float(recent_outward["amount_value"].sum())

    metrics["rapid_outward_event_count_2h_30d"] = len(rapid_outward)
    metrics["rapid_outward_amount_2h_30d"] = _round(rapid_amount, 2)
    metrics["rapid_outward_share_2h_30d"] = _round(
        _safe_ratio(rapid_amount, recent_outward_amount)
    )
    metrics["distinct_inward_source_count_30d"] = int(
        recent_inward["source_iban"].nunique()
    )
    metrics["distinct_outward_counterparty_count_30d"] = int(
        recent_outward["beneficiary_account_number"].nunique()
    )
    metrics["beneficiary_count"] = len(current_beneficiaries)

    beneficiary_age_days = (
        pd.Timestamp(run_date)
        - current_beneficiaries["beneficiary_created_at"]
    ).dt.total_seconds() / 86400

    metrics["beneficiary_created_last_7d_count"] = int(
        beneficiary_age_days.le(7).sum()
    )
    metrics["beneficiary_created_last_30d_count"] = int(
        beneficiary_age_days.le(30).sum()
    )

    return metrics



def _international_counterparty_activity_records(
    *,
    activity: pd.DataFrame | None,
    counterparty_key: str,
) -> list[dict[str, object]]:
    """Return currency-separated international evidence."""
    if activity is None:
        return []

    required_columns = {
        "counterparty_key",
        "currency",
        "event_count",
        "total_amount",
        "distinct_customer_count",
    }

    missing_columns = sorted(
        required_columns - set(activity.columns)
    )

    if missing_columns:
        raise BehavioralFeatureError(
            "International counterparty activity "
            f"is missing columns: {missing_columns}"
        )

    current = activity.loc[
        activity["counterparty_key"]
        .astype("string")
        .str.strip()
        .eq(counterparty_key)
    ].copy()

    current = current.sort_values(
        by=["currency"],
        kind="stable",
    )

    return [
        {
            "currency": str(row.currency),
            "event_count": int(row.event_count),
            "total_amount": float(row.total_amount),
            "distinct_customer_count": int(
                row.distinct_customer_count
            ),
        }
        for row in current.itertuples(index=False)
    ]

def build_behavioral_features(
    *,
    counterparty_keys: Iterable[str],
    run_date: date | str,
    source_directory: Path | str | None = None,
    raw_sources: RawDiscoverySources | None = None,
    international_counterparty_currency_activity: (
        pd.DataFrame | None
    ) = None,
) -> BehavioralFeatureResult:
    """Build neutral evidence for an observed counterparty frontier."""
    resolved_run_date = parse_run_date(run_date)
    requested_keys = sorted(
        {
            str(value).strip()
            for value in counterparty_keys
            if str(value).strip()
        }
    )

    if not requested_keys:
        raise BehavioralFeatureError(
            "At least one counterparty key is required."
        )

    if (
        source_directory is None
        and raw_sources is None
    ):
        raise BehavioralFeatureError(
            "Either source_directory or raw_sources "
            "must be provided."
        )

    if (
        source_directory is not None
        and raw_sources is not None
    ):
        raise BehavioralFeatureError(
            "Provide source_directory or raw_sources, "
            "not both."
        )

    if raw_sources is not None:
        if not isinstance(
            raw_sources,
            RawDiscoverySources,
        ):
            raise BehavioralFeatureError(
                "raw_sources must be RawDiscoverySources."
            )

        resolved_sources = raw_sources
    else:
        resolved_sources = load_raw_discovery_sources(
            source_directory
        )

    prepared = _prepare_sources(
        resolved_sources,
        resolved_run_date,
    )

    outward = prepared["outward"]
    inward = prepared["inward"]
    beneficiaries = prepared["beneficiaries"]
    customer_lookup = prepared["customer_lookup"]
    seed_customer_ids = prepared["seed_customer_ids"]

    observed = outward.loc[
        outward["counterparty_key"].isin(requested_keys)
    ].copy()

    missing_keys = sorted(
        set(requested_keys)
        - set(observed["counterparty_key"].dropna())
    )

    if missing_keys:
        raise BehavioralFeatureError(
            "No completed transfer evidence exists on or before the run "
            f"date for counterparties: {missing_keys}"
        )

    observed = observed.merge(
        customer_lookup,
        how="left",
        on="customer_id",
        validate="many_to_one",
    )

    if observed["entity_type"].isna().any():
        raise BehavioralFeatureError(
            "Counterparty transfers contain unresolved customers."
        )

    relationship_records: list[dict[str, object]] = []
    profile_records: list[dict[str, object]] = []
    payload_records: list[dict[str, object]] = []

    for counterparty_key in requested_keys:
        transfers = observed.loc[
            observed["counterparty_key"].eq(counterparty_key)
        ].copy()
        current_beneficiaries = beneficiaries.loc[
            beneficiaries["counterparty_key"].eq(counterparty_key)
        ].copy()

        customer_amounts = transfers.groupby("customer_id")[
            "amount_value"
        ].sum().sort_values(ascending=False)
        customer_event_counts = transfers.groupby("customer_id")[
            "transfer_id"
        ].nunique()
        customer_active_months = transfers.assign(
            active_month=transfers["transaction_at"].dt.to_period("M")
        ).groupby("customer_id")["active_month"].nunique()

        total_amount = float(transfers["amount_value"].sum())
        distinct_customer_count = int(transfers["customer_id"].nunique())
        beneficiary_age_days = (
            pd.Timestamp(resolved_run_date)
            - current_beneficiaries["beneficiary_created_at"]
        ).dt.total_seconds() / 86400

        top_purpose_counts = transfers["payment_purpose_name"].value_counts()
        top_purpose = (
            str(top_purpose_counts.index[0])
            if not top_purpose_counts.empty
            else ""
        )
        top_purpose_share = (
            _safe_ratio(
                int(top_purpose_counts.iloc[0]),
                len(transfers),
            )
            if not top_purpose_counts.empty
            else 0.0
        )

        customer_rows: list[dict[str, object]] = []

        for customer_id in sorted(transfers["customer_id"].unique()):
            customer_transfers = transfers.loc[
                transfers["customer_id"].eq(customer_id)
            ]
            lookup_row = customer_lookup.loc[
                customer_lookup["customer_id"].eq(customer_id)
            ].iloc[0]

            customer_outward_30d = outward.loc[
                outward["customer_id"].eq(customer_id)
                & outward["transaction_at"].ge(
                    _inclusive_cutoff(resolved_run_date)
                    - pd.Timedelta(days=30)
                )
            ]
            customer_outward_30d_amount = float(
                customer_outward_30d["amount_value"].sum()
            )
            current_counterparty_30d_amount = float(
                customer_transfers.loc[
                    customer_transfers["transaction_at"].ge(
                        _inclusive_cutoff(resolved_run_date)
                        - pd.Timedelta(days=30)
                    ),
                    "amount_value",
                ].sum()
            )

            record = {
                "run_date": str(resolved_run_date),
                "counterparty_key": counterparty_key,
                "customer_id": customer_id,
                "entity_type": str(lookup_row["entity_type"]),
                "entity_id": str(lookup_row["entity_id"]),
                "entity_key": (
                    f"{lookup_row['entity_type']}|{lookup_row['entity_id']}"
                ),
                "is_seed_customer": customer_id in seed_customer_ids,
                "customer_tenure_days": int(
                    (
                        pd.Timestamp(resolved_run_date)
                        - lookup_row["customer_created_at"]
                    ).days
                ),
                "account_tenure_days": int(
                    (
                        pd.Timestamp(resolved_run_date)
                        - lookup_row["account_opened_at"]
                    ).days
                ),
                "counterparty_transfer_count": len(customer_transfers),
                "counterparty_transfer_amount": _round(
                    customer_transfers["amount_value"].sum(), 2
                ),
                "counterparty_first_transfer_timestamp": str(
                    customer_transfers["transaction_at"].min()
                ),
                "counterparty_last_transfer_timestamp": str(
                    customer_transfers["transaction_at"].max()
                ),
                "counterparty_share_of_customer_outward_30d": _round(
                    _safe_ratio(
                        current_counterparty_30d_amount,
                        customer_outward_30d_amount,
                    )
                ),
            }
            record.update(
                _customer_window_metrics(
                    customer_id=customer_id,
                    inward=inward,
                    outward=outward,
                    beneficiaries=beneficiaries,
                    run_date=resolved_run_date,
                )
            )
            relationship_records.append(record)
            customer_rows.append(record)

        first_transfer = transfers["transaction_at"].min()
        last_transfer = transfers["transaction_at"].max()
        recent_account_count = sum(
            int(row["account_tenure_days"] <= 30)
            for row in customer_rows
        )

        profile = {
            "run_date": str(resolved_run_date),
            "counterparty_key": counterparty_key,
            "counterparty_key_type": counterparty_key.split("|", 1)[0],
            "transfer_event_count": len(transfers),
            "distinct_customer_count": distinct_customer_count,
            "seed_customer_count": int(
                transfers.loc[
                    transfers["customer_id"].isin(seed_customer_ids),
                    "customer_id",
                ].nunique()
            ),
            "retail_customer_count": int(
                transfers.loc[
                    transfers["entity_type"].eq("RETAIL"),
                    "customer_id",
                ].nunique()
            ),
            "sme_customer_count": int(
                transfers.loc[
                    transfers["entity_type"].eq("SME"),
                    "customer_id",
                ].nunique()
            ),
            "total_transfer_amount": _round(total_amount, 2),
            "mean_transfer_amount": _round(transfers["amount_value"].mean(), 2),
            "median_transfer_amount": _round(
                transfers["amount_value"].median(), 2
            ),
            "max_transfer_amount": _round(transfers["amount_value"].max(), 2),
            "first_transfer_timestamp": str(first_transfer),
            "last_transfer_timestamp": str(last_transfer),
            "relationship_tenure_days": int(
                (last_transfer.normalize() - first_transfer.normalize()).days
            ),
            "active_day_count": int(
                transfers["transaction_at"].dt.date.nunique()
            ),
            "active_month_count": int(
                transfers["transaction_at"].dt.to_period("M").nunique()
            ),
            "single_event_customer_count": int(customer_event_counts.eq(1).sum()),
            "single_event_customer_share": _round(
                _safe_ratio(
                    int(customer_event_counts.eq(1).sum()),
                    distinct_customer_count,
                )
            ),
            "repeat_customer_count": int(customer_event_counts.gt(1).sum()),
            "repeat_customer_share": _round(
                _safe_ratio(
                    int(customer_event_counts.gt(1).sum()),
                    distinct_customer_count,
                )
            ),
            "recurring_3_month_customer_count": int(
                customer_active_months.ge(3).sum()
            ),
            "recurring_3_month_customer_share": _round(
                _safe_ratio(
                    int(customer_active_months.ge(3).sum()),
                    distinct_customer_count,
                )
            ),
            "mean_events_per_customer": _round(
                customer_event_counts.mean()
            ),
            "median_events_per_customer": _round(
                customer_event_counts.median()
            ),
            "top_customer_amount_share": _round(
                _safe_ratio(
                    float(customer_amounts.iloc[0]),
                    total_amount,
                )
            ),
            "top_3_customer_amount_share": _round(
                _safe_ratio(
                    float(customer_amounts.head(3).sum()),
                    total_amount,
                )
            ),
            "beneficiary_relationship_count": len(current_beneficiaries),
            "beneficiary_customer_count": int(
                current_beneficiaries["customer_id"].nunique()
            ),
            "beneficiary_age_min_days": _round(
                beneficiary_age_days.min(), 2
            ),
            "beneficiary_age_median_days": _round(
                beneficiary_age_days.median(), 2
            ),
            "beneficiary_age_max_days": _round(
                beneficiary_age_days.max(), 2
            ),
            "beneficiary_created_last_7d_count": int(
                beneficiary_age_days.le(7).sum()
            ),
            "beneficiary_created_last_7d_share": _round(
                _safe_ratio(
                    int(beneficiary_age_days.le(7).sum()),
                    len(current_beneficiaries),
                )
            ),
            "beneficiary_created_last_30d_count": int(
                beneficiary_age_days.le(30).sum()
            ),
            "beneficiary_created_last_30d_share": _round(
                _safe_ratio(
                    int(beneficiary_age_days.le(30).sum()),
                    len(current_beneficiaries),
                )
            ),
            "recent_account_customer_count_30d": recent_account_count,
            "recent_account_customer_share_30d": _round(
                _safe_ratio(recent_account_count, distinct_customer_count)
            ),
            "distinct_payment_purpose_count": int(
                transfers["payment_purpose_name"].nunique()
            ),
            "top_payment_purpose": top_purpose,
            "top_payment_purpose_share": _round(top_purpose_share),
        }
        profile_records.append(profile)

        sorted_customer_rows = sorted(
            customer_rows,
            key=lambda row: (
                -float(row["counterparty_transfer_amount"]),
                str(row["customer_id"]),
            ),
        )

        sampled_customer_rows = sorted_customer_rows[
            :COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT
        ]

        payload = {
            "subject_type": "COUNTERPARTY",
            "subject_key": counterparty_key,
            "run_date": str(resolved_run_date),
            "counterparty_assessment_policy_version": (
                COUNTERPARTY_ASSESSMENT_POLICY_VERSION
            ),
            "international_currency_activity": (
                _international_counterparty_activity_records(
                    activity=(
                        international_counterparty_currency_activity
                    ),
                    counterparty_key=counterparty_key,
                )
            ),
            "aggregate_behavior": profile,
            "linked_customer_distribution": {
                "customer_count": distinct_customer_count,
                "flow_through_ratio_30d_median": _round(
                    pd.Series(
                        [
                            row["flow_through_ratio_30d"]
                            for row in customer_rows
                        ]
                    ).median()
                ),
                "rapid_outward_share_2h_30d_median": _round(
                    pd.Series(
                        [
                            row["rapid_outward_share_2h_30d"]
                            for row in customer_rows
                        ]
                    ).median()
                ),
                "recent_account_customer_share_30d": profile[
                    "recent_account_customer_share_30d"
                ],
            },
            "linked_customer_sampling": {
                "population_customer_count": (
                    distinct_customer_count
                ),
                "sample_limit": (
                    COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT
                ),
                "sampled_customer_count": len(
                    sampled_customer_rows
                ),
                "omitted_customer_count": (
                    distinct_customer_count
                    - len(sampled_customer_rows)
                ),
                "sampling_method": (
                    COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_METHOD
                ),
                "full_population_behavior_digest": (
                    _records_digest(
                        sorted(
                            customer_rows,
                            key=lambda row: str(
                                row["customer_id"]
                            ),
                        )
                    )
                ),
            },
            "highest_value_linked_customers": (
                sampled_customer_rows
            ),
        }

        payload_records.append(
            {
                "subject_type": "COUNTERPARTY",
                "subject_key": counterparty_key,
                "feature_payload_json": json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
        )

    counterparty_profiles = (
        pd.DataFrame(profile_records)
        .sort_values(by=["counterparty_key"], kind="stable")
        .reset_index(drop=True)
    )
    counterparty_customer_profiles = (
        pd.DataFrame(relationship_records)
        .sort_values(
            by=["counterparty_key", "customer_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    counterparty_payloads = (
        pd.DataFrame(payload_records)
        .sort_values(by=["subject_key"], kind="stable")
        .reset_index(drop=True)
    )

    return BehavioralFeatureResult(
        counterparty_profiles=counterparty_profiles,
        counterparty_customer_profiles=counterparty_customer_profiles,
        counterparty_payloads=counterparty_payloads,
    )


def write_behavioral_features(
    *,
    source_directory: Path | str,
    counterparty_keys: Iterable[str],
    output_directory: Path | str,
    run_date: date | str,
) -> BehavioralFeatureResult:
    """Build and write neutral feature tables for one frontier."""
    result = build_behavioral_features(
        source_directory=source_directory,
        counterparty_keys=counterparty_keys,
        run_date=run_date,
    )

    resolved_output_directory = Path(output_directory)
    resolved_output_directory.mkdir(parents=True, exist_ok=True)

    result.counterparty_profiles.to_csv(
        resolved_output_directory / COUNTERPARTY_PROFILE_FILENAME,
        index=False,
    )
    result.counterparty_customer_profiles.to_csv(
        resolved_output_directory / COUNTERPARTY_CUSTOMER_PROFILE_FILENAME,
        index=False,
    )
    result.counterparty_payloads.to_csv(
        resolved_output_directory / COUNTERPARTY_PAYLOAD_FILENAME,
        index=False,
    )

    return result
