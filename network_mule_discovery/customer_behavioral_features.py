"""Build neutral customer evidence for breadth-first AI assessment."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from network_mule_discovery.behavioral_features import (
    BehavioralFeatureError,
    _customer_window_metrics,
    _inclusive_cutoff,
    _prepare_sources,
    _round,
    _safe_ratio,
)
from network_mule_discovery.counterparty_schemas import (
    build_counterparty_identity,
)
from network_mule_discovery.decision_engine import (
    DecisionProjectionResult,
)
from network_mule_discovery.raw_source_adapter import (
    RawDiscoverySources,
    load_raw_discovery_sources,
)
from network_mule_discovery.schemas import parse_run_date


CUSTOMER_PROFILE_FILENAME = "customer_behavior_profiles.csv"
CUSTOMER_COUNTERPARTY_PROFILE_FILENAME = (
    "customer_counterparty_behavior_profiles.csv"
)
CUSTOMER_PAYLOAD_FILENAME = "customer_feature_payloads.csv"
CUSTOMER_ASSESSMENT_POLICY_VERSION = (
    "customer-assessment-policy-v2"
)


@dataclass(frozen=True)
class CustomerBehavioralFeatureResult:
    """Neutral behavioral and relationship evidence for customers."""

    customer_profiles: pd.DataFrame
    customer_counterparty_profiles: pd.DataFrame
    customer_payloads: pd.DataFrame


def _resolve_customer_lookup(
    *,
    customer_keys: Iterable[str],
    customer_lookup: pd.DataFrame,
) -> pd.DataFrame:
    requested_keys = sorted({
        str(value).strip()
        for value in customer_keys
        if str(value).strip()
    })

    if not requested_keys:
        raise BehavioralFeatureError(
            "At least one customer key is required."
        )

    lookup = customer_lookup.copy()
    lookup["entity_key"] = (
        lookup["entity_type"].astype("string")
        + "|"
        + lookup["entity_id"].astype("string")
    )

    duplicate_mask = lookup.duplicated(
        subset=["entity_key"],
        keep=False,
    )

    if duplicate_mask.any():
        duplicates = sorted(
            lookup.loc[
                duplicate_mask,
                "entity_key",
            ].unique()
        )
        raise BehavioralFeatureError(
            "Customer identity lookup contains duplicate entity keys: "
            f"{duplicates}"
        )

    resolved = lookup.loc[
        lookup["entity_key"].isin(requested_keys)
    ].copy()

    missing_keys = sorted(
        set(requested_keys)
        - set(resolved["entity_key"])
    )

    if missing_keys:
        raise BehavioralFeatureError(
            "Customer keys could not be resolved from source identity: "
            f"{missing_keys}"
        )

    return (
        resolved
        .sort_values(by=["entity_key"], kind="stable")
        .reset_index(drop=True)
    )


def _counterparty_key(value: object) -> str:
    return build_counterparty_identity(
        rail="LOCAL",
        counterparty_iban="",
        counterparty_swift_bic="",
        counterparty_account_number=value,
    ).counterparty_key


def _salary_metrics(
    *,
    inward: pd.DataFrame,
    run_date: date,
) -> dict[str, object]:
    cutoff = _inclusive_cutoff(run_date)
    start = cutoff - pd.Timedelta(days=365)
    recent = inward.loc[
        inward["transaction_at"].ge(start)
    ].copy()

    salary_mask = (
        recent["payment_purpose_key"]
        .astype("string")
        .str.upper()
        .str.contains("SALARY", na=False)
        | recent["payment_purpose_name"]
        .astype("string")
        .str.upper()
        .str.contains("SALARY", na=False)
    )

    salary = recent.loc[salary_mask].copy()
    salary_amounts = salary["amount_value"]
    salary_mean = float(salary_amounts.mean()) if not salary.empty else 0.0
    salary_std = float(salary_amounts.std(ddof=0)) if not salary.empty else 0.0

    if salary.empty:
        regular_amount_share = 0.0
    else:
        amount_counts = salary_amounts.round(2).value_counts()
        regular_amount_share = _safe_ratio(
            int(amount_counts.iloc[0]),
            len(salary),
        )

    return {
        "salary_event_count_365d": len(salary),
        "salary_month_count_365d": int(
            salary["transaction_at"].dt.to_period("M").nunique()
        ),
        "salary_distinct_source_count_365d": int(
            salary["source_iban"].nunique()
        ),
        "salary_amount_total_365d": _round(
            salary_amounts.sum(), 2
        ),
        "salary_amount_mean_365d": _round(
            salary_mean, 2
        ),
        "salary_amount_median_365d": _round(
            salary_amounts.median(), 2
        ),
        "salary_amount_cv_365d": _round(
            _safe_ratio(salary_std, salary_mean)
        ),
        "salary_regular_amount_share_365d": _round(
            regular_amount_share
        ),
        "non_salary_inward_event_count_365d": int(
            (~salary_mask).sum()
        ),
    }


def _counterparty_profiles(
    *,
    customer_id: str,
    outward: pd.DataFrame,
    beneficiaries: pd.DataFrame,
    run_date: date,
) -> list[dict[str, object]]:
    cutoff = _inclusive_cutoff(run_date)
    start = cutoff - pd.Timedelta(days=365)
    current = outward.loc[
        outward["customer_id"].eq(customer_id)
        & outward["transaction_at"].ge(start)
    ].copy()

    if current.empty:
        return []

    current["counterparty_key"] = current[
        "beneficiary_account_number"
    ].map(_counterparty_key)

    customer_beneficiaries = beneficiaries.loc[
        beneficiaries["customer_id"].eq(customer_id)
    ].copy()

    total_amount = float(current["amount_value"].sum())
    records: list[dict[str, object]] = []

    for counterparty_key, transfers in current.groupby(
        "counterparty_key",
        sort=True,
    ):
        beneficiary_rows = customer_beneficiaries.loc[
            customer_beneficiaries[
                "counterparty_key"
            ].eq(counterparty_key)
        ]
        beneficiary_age_days = (
            pd.Timestamp(run_date)
            - beneficiary_rows["beneficiary_created_at"]
        ).dt.total_seconds() / 86400

        amount = float(transfers["amount_value"].sum())

        records.append({
            "counterparty_key": counterparty_key,
            "transfer_event_count_365d": len(transfers),
            "transfer_amount_365d": _round(amount, 2),
            "customer_outward_amount_share_365d": _round(
                _safe_ratio(amount, total_amount)
            ),
            "active_day_count_365d": int(
                transfers["transaction_at"].dt.date.nunique()
            ),
            "active_month_count_365d": int(
                transfers["transaction_at"].dt.to_period("M").nunique()
            ),
            "first_transfer_timestamp": str(
                transfers["transaction_at"].min()
            ),
            "last_transfer_timestamp": str(
                transfers["transaction_at"].max()
            ),
            "beneficiary_relationship_count": len(
                beneficiary_rows
            ),
            "beneficiary_age_min_days": _round(
                beneficiary_age_days.min(), 2
            ),
            "beneficiary_age_median_days": _round(
                beneficiary_age_days.median(), 2
            ),
            "top_payment_purpose": str(
                transfers["payment_purpose_name"]
                .value_counts()
                .index[0]
            ),
        })

    return sorted(
        records,
        key=lambda record: (
            -float(record["transfer_amount_365d"]),
            str(record["counterparty_key"]),
        ),
    )


def _assessment_context(
    *,
    customer_key: str,
    projection: DecisionProjectionResult,
) -> dict[str, object]:
    nodes = projection.nodes.copy()
    edges = projection.edges.copy()

    customer_nodes = nodes.loc[
        nodes["node_type"].eq("CUSTOMER")
        & nodes["entity_key"].eq(customer_key)
    ].copy()

    if customer_nodes.empty:
        raise BehavioralFeatureError(
            "Customer is not present in the decision projection: "
            f"{customer_key}"
        )

    customer_node_keys = set(
        customer_nodes["node_key"].astype("string")
    )

    incident = edges.loc[
        edges["source_node_key"].isin(customer_node_keys)
        | edges["target_node_key"].isin(customer_node_keys)
    ].copy()

    node_lookup = {
        str(row.node_key): row
        for row in nodes.itertuples(index=False)
    }

    relationship_records: list[dict[str, object]] = []

    for row in incident.itertuples(index=False):
        source_is_customer = (
            str(row.source_node_key) in customer_node_keys
        )
        other_node_key = str(
            row.target_node_key
            if source_is_customer
            else row.source_node_key
        )
        other_node = node_lookup.get(other_node_key)

        record = {
            "edge_type": str(row.edge_type),
            "relationship_status": str(
                row.relationship_status
            ),
            "evidence_summary": str(row.evidence_summary),
            "source_event_count": str(row.source_event_count),
            "candidate_event_count": str(
                row.candidate_event_count
            ),
            "other_node_type": str(
                getattr(other_node, "node_type", "")
            ),
            "other_subject_key": "",
            "counterparty_decision": "",
            "counterparty_reason_code": "",
        }

        if other_node is not None:
            if str(other_node.node_type) == "COUNTERPARTY":
                record["other_subject_key"] = str(
                    other_node.counterparty_key
                )
                record["counterparty_decision"] = str(
                    getattr(
                        other_node,
                        "applied_decision",
                        "",
                    )
                )
                record["counterparty_reason_code"] = str(
                    getattr(
                        other_node,
                        "decision_reason_code",
                        "",
                    )
                )
            elif str(other_node.node_type) == "CUSTOMER":
                record["other_subject_key"] = str(
                    other_node.entity_key
                )

        relationship_records.append(record)

    relationship_records = sorted(
        relationship_records,
        key=lambda record: (
            str(record["edge_type"]),
            str(record["other_subject_key"]),
        ),
    )

    approved = [
        record
        for record in relationship_records
        if record["relationship_status"]
        == "COUNTERPARTY_APPROVED_SUSPICIOUS"
    ]
    suppressed = [
        record
        for record in relationship_records
        if str(record["relationship_status"]).startswith(
            "COUNTERPARTY_SUPPRESSED"
        )
    ]
    deterministic = [
        record
        for record in relationship_records
        if record["edge_type"]
        in {
            "SAME_EMIRATES_ID",
            "BENEFICIARY_ADDED_SEED_ACCOUNT",
            "BENEFICIARY_ADDED_MULE_ACCOUNT",
        }
    ]

    return {
        "assessment_trigger_count": (
            len(approved) + len(deterministic)
        ),
        "approved_suspicious_counterparty_count": len(approved),
        "suppressed_counterparty_count": len(suppressed),
        "deterministic_relationship_count": len(deterministic),
        "approved_suspicious_counterparties": approved[:5],
        "suppressed_nearby_counterparties": suppressed[:5],
        "deterministic_relationships": deterministic[:5],
    }



def _international_customer_activity_records(
    *,
    activity: pd.DataFrame | None,
    customer_id: str,
) -> list[dict[str, object]]:
    """Return currency-separated international customer evidence."""
    if activity is None:
        return []

    required_columns = {
        "customer_id",
        "direction",
        "currency",
        "event_count",
        "total_amount",
        "distinct_counterparty_count",
    }

    missing_columns = sorted(
        required_columns - set(activity.columns)
    )

    if missing_columns:
        raise BehavioralFeatureError(
            "International customer activity "
            f"is missing columns: {missing_columns}"
        )

    current = activity.loc[
        activity["customer_id"]
        .astype("string")
        .str.strip()
        .eq(customer_id)
    ].copy()

    current = current.sort_values(
        by=[
            "direction",
            "currency",
        ],
        kind="stable",
    )

    return [
        {
            "direction": str(row.direction),
            "currency": str(row.currency),
            "event_count": int(row.event_count),
            "total_amount": float(row.total_amount),
            "distinct_counterparty_count": int(
                row.distinct_counterparty_count
            ),
        }
        for row in current.itertuples(index=False)
    ]

def build_customer_behavioral_features(
    *,
    customer_keys: Iterable[str],
    projection: DecisionProjectionResult,
    run_date: date | str,
    source_directory: Path | str | None = None,
    raw_sources: RawDiscoverySources | None = None,
    international_customer_currency_activity: (
        pd.DataFrame | None
    ) = None,
) -> CustomerBehavioralFeatureResult:
    """Build bounded neutral evidence for one customer AI frontier."""
    resolved_run_date = parse_run_date(run_date)
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

    customer_lookup = _resolve_customer_lookup(
        customer_keys=customer_keys,
        customer_lookup=prepared["customer_lookup"],
    )
    inward = prepared["inward"]
    outward = prepared["outward"]
    beneficiaries = prepared["beneficiaries"]

    profile_records: list[dict[str, object]] = []
    counterparty_records: list[dict[str, object]] = []
    payload_records: list[dict[str, object]] = []

    for lookup_row in customer_lookup.itertuples(index=False):
        customer_id = str(lookup_row.customer_id)
        customer_key = str(lookup_row.entity_key)
        current_inward = inward.loc[
            inward["customer_id"].eq(customer_id)
        ].copy()
        current_outward = outward.loc[
            outward["customer_id"].eq(customer_id)
        ].copy()
        current_beneficiaries = beneficiaries.loc[
            beneficiaries["customer_id"].eq(customer_id)
        ].copy()

        window_metrics = _customer_window_metrics(
            customer_id=customer_id,
            inward=inward,
            outward=outward,
            beneficiaries=beneficiaries,
            run_date=resolved_run_date,
        )
        salary_metrics = _salary_metrics(
            inward=current_inward,
            run_date=resolved_run_date,
        )
        counterparty_profiles = _counterparty_profiles(
            customer_id=customer_id,
            outward=outward,
            beneficiaries=beneficiaries,
            run_date=resolved_run_date,
        )
        assessment_context = _assessment_context(
            customer_key=customer_key,
            projection=projection,
        )

        total_outward_365d = float(
            window_metrics["outward_amount_365d"]
        )
        top_counterparty_amount = (
            float(
                counterparty_profiles[0][
                    "transfer_amount_365d"
                ]
            )
            if counterparty_profiles
            else 0.0
        )

        beneficiary_age_days = (
            pd.Timestamp(resolved_run_date)
            - current_beneficiaries[
                "beneficiary_created_at"
            ]
        ).dt.total_seconds() / 86400

        event_timestamps = pd.concat(
            [
                current_inward["transaction_at"],
                current_outward["transaction_at"],
            ],
            ignore_index=True,
        ).dropna()

        profile = {
            "run_date": str(resolved_run_date),
            "assessment_policy_version": (
                CUSTOMER_ASSESSMENT_POLICY_VERSION
            ),
            "subject_type": "CUSTOMER",
            "subject_key": customer_key,
            "customer_id": customer_id,
            "entity_type": str(lookup_row.entity_type),
            "entity_id": str(lookup_row.entity_id),
            "customer_segment": str(
                lookup_row.customer_segment
            ),
            "account_currency": str(
                lookup_row.account_currency
            ),
            "customer_tenure_days": int(
                (
                    pd.Timestamp(resolved_run_date)
                    - lookup_row.customer_created_at
                ).days
            ),
            "account_tenure_days": int(
                (
                    pd.Timestamp(resolved_run_date)
                    - lookup_row.account_opened_at
                ).days
            ),
            "all_time_inward_event_count": len(current_inward),
            "all_time_inward_amount": _round(
                current_inward["amount_value"].sum(), 2
            ),
            "all_time_outward_event_count": len(current_outward),
            "all_time_outward_amount": _round(
                current_outward["amount_value"].sum(), 2
            ),
            "first_observed_transaction_timestamp": (
                str(event_timestamps.min())
                if not event_timestamps.empty
                else ""
            ),
            "last_observed_transaction_timestamp": (
                str(event_timestamps.max())
                if not event_timestamps.empty
                else ""
            ),
            "top_counterparty_amount_share_365d": _round(
                _safe_ratio(
                    top_counterparty_amount,
                    total_outward_365d,
                )
            ),
            "distinct_outward_counterparty_count_365d": len(
                counterparty_profiles
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
            **window_metrics,
            **salary_metrics,
            "assessment_trigger_count": assessment_context[
                "assessment_trigger_count"
            ],
            "approved_suspicious_counterparty_count": (
                assessment_context[
                    "approved_suspicious_counterparty_count"
                ]
            ),
            "suppressed_counterparty_count": assessment_context[
                "suppressed_counterparty_count"
            ],
            "deterministic_relationship_count": (
                assessment_context[
                    "deterministic_relationship_count"
                ]
            ),
        }
        profile_records.append(profile)

        for record in counterparty_profiles:
            counterparty_records.append({
                "run_date": str(resolved_run_date),
                "subject_key": customer_key,
                "customer_id": customer_id,
                **record,
            })

        payload = {
            "subject_type": "CUSTOMER",
            "subject_key": customer_key,
            "run_date": str(resolved_run_date),
            "assessment_policy_version": (
                CUSTOMER_ASSESSMENT_POLICY_VERSION
            ),
            "identity_and_tenure": {
                key: profile[key]
                for key in [
                    "entity_type",
                    "entity_id",
                    "customer_segment",
                    "account_currency",
                    "customer_tenure_days",
                    "account_tenure_days",
                ]
            },
            "transaction_behavior": {
                key: value
                for key, value in profile.items()
                if (
                    key.startswith("inward_")
                    or key.startswith("outward_")
                    or key.startswith("flow_through_")
                    or key.startswith("rapid_outward_")
                    or key.startswith("distinct_inward_")
                    or key.startswith("distinct_outward_")
                    or key.startswith("all_time_")
                    or key.startswith("top_counterparty_")
                    or key in {
                        "first_observed_transaction_timestamp",
                        "last_observed_transaction_timestamp",
                    }
                )
            },
            "international_currency_activity": (
                _international_customer_activity_records(
                    activity=(
                        international_customer_currency_activity
                    ),
                    customer_id=customer_id,
                )
            ),
            "salary_behavior": salary_metrics,
            "beneficiary_behavior": {
                key: value
                for key, value in profile.items()
                if key.startswith("beneficiary_")
            },
            "assessment_context": assessment_context,
            "highest_value_outward_counterparties": (
                counterparty_profiles[:5]
            ),
        }

        payload_records.append({
            "subject_type": "CUSTOMER",
            "subject_key": customer_key,
            "feature_payload_json": json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        })

    customer_profiles = (
        pd.DataFrame(profile_records)
        .sort_values(by=["subject_key"], kind="stable")
        .reset_index(drop=True)
    )
    customer_counterparty_profiles = pd.DataFrame(
        counterparty_records
    )

    if customer_counterparty_profiles.empty:
        customer_counterparty_profiles = pd.DataFrame(
            columns=[
                "run_date",
                "subject_key",
                "customer_id",
                "counterparty_key",
            ]
        )
    else:
        customer_counterparty_profiles = (
            customer_counterparty_profiles
            .sort_values(
                by=[
                    "subject_key",
                    "transfer_amount_365d",
                    "counterparty_key",
                ],
                ascending=[True, False, True],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    customer_payloads = (
        pd.DataFrame(payload_records)
        .sort_values(by=["subject_key"], kind="stable")
        .reset_index(drop=True)
    )

    return CustomerBehavioralFeatureResult(
        customer_profiles=customer_profiles,
        customer_counterparty_profiles=(
            customer_counterparty_profiles
        ),
        customer_payloads=customer_payloads,
    )


def write_customer_behavioral_features(
    *,
    source_directory: Path | str,
    customer_keys: Iterable[str],
    projection: DecisionProjectionResult,
    output_directory: Path | str,
    run_date: date | str,
) -> CustomerBehavioralFeatureResult:
    """Build and write neutral customer evidence for one frontier."""
    result = build_customer_behavioral_features(
        source_directory=source_directory,
        customer_keys=customer_keys,
        projection=projection,
        run_date=run_date,
    )

    resolved_output_directory = Path(output_directory)
    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    result.customer_profiles.to_csv(
        resolved_output_directory / CUSTOMER_PROFILE_FILENAME,
        index=False,
    )
    result.customer_counterparty_profiles.to_csv(
        resolved_output_directory
        / CUSTOMER_COUNTERPARTY_PROFILE_FILENAME,
        index=False,
    )
    result.customer_payloads.to_csv(
        resolved_output_directory / CUSTOMER_PAYLOAD_FILENAME,
        index=False,
    )

    return result
