"""Schema contracts and normalization for counterparty discovery."""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from network_mule_discovery.schemas import (
    SchemaValidationError,
    normalize_text,
    validate_required_columns,
)


SEED_MULE_EVENT_REQUIRED_COLUMNS = (
    "snapshot_date",
    "seed_event_id",
    "seed_customer_id",
    "cbs_txn_ref",
    "date_reported",
    "frc_rail",
    "frc_source",
    "source_payment_transfer_id",
    "seed_account_number",
    "seed_iban",
)

COUNTERPARTY_EVENT_REQUIRED_COLUMNS = (
    "snapshot_date",
    "event_id",
    "event_type",
    "rail",
    "customer_id",
    "event_timestamp",
    "transfer_id",
    "reference_number",
    "beneficiary_id",
    "status",
    "wio_account_number",
    "wio_iban",
    "counterparty_account_number",
    "counterparty_iban",
    "counterparty_swift_bic",
    "counterparty_name",
    "counterparty_country",
    "amount",
    "currency",
    "source_table",
)

VALID_FRC_RAILS = frozenset({
    "INTERNATIONAL",
    "LOCAL",
})

VALID_COUNTERPARTY_EVENT_TYPES = frozenset({
    "TRANSFER_RECEIVED",
    "TRANSFER_SENT",
    "BENEFICIARY_ADDED",
})


@dataclass(frozen=True)
class CounterpartyIdentity:
    """Normalized counterparty identity."""

    counterparty_key: str | None
    counterparty_key_type: str | None
    counterparty_key_quality: str
    account_match_key: str | None


def normalize_identifier(value: object) -> str | None:
    """Normalize an account, IBAN, or bank identifier."""
    text = normalize_text(value)

    if text is None:
        return None

    normalized = re.sub(
        r"[^0-9A-Z]",
        "",
        text.upper(),
    )

    return normalized or None


def build_account_match_key(
    iban: object,
    account_number: object,
) -> str | None:
    """
    Build a rail-independent account key.

    This key is used when comparing a saved beneficiary against a known
    seed mule account.
    """
    normalized_iban = normalize_identifier(iban)

    if normalized_iban is not None:
        return f"IBAN|{normalized_iban}"

    normalized_account = normalize_identifier(account_number)

    if normalized_account is not None:
        return f"ACCOUNT|{normalized_account}"

    return None


def build_counterparty_identity(
    rail: object,
    counterparty_iban: object,
    counterparty_swift_bic: object,
    counterparty_account_number: object,
) -> CounterpartyIdentity:
    """Build the strongest available cross-customer counterparty key."""
    normalized_rail = normalize_identifier(rail)
    normalized_iban = normalize_identifier(counterparty_iban)
    normalized_swift = normalize_identifier(
        counterparty_swift_bic
    )
    normalized_account = normalize_identifier(
        counterparty_account_number
    )

    account_match_key = build_account_match_key(
        iban=counterparty_iban,
        account_number=counterparty_account_number,
    )

    if normalized_iban is not None:
        return CounterpartyIdentity(
            counterparty_key=f"IBAN|{normalized_iban}",
            counterparty_key_type="IBAN",
            counterparty_key_quality="STRONG",
            account_match_key=account_match_key,
        )

    if (
        normalized_swift is not None
        and normalized_account is not None
    ):
        return CounterpartyIdentity(
            counterparty_key=(
                f"SWIFT_ACCOUNT|{normalized_swift}|"
                f"{normalized_account}"
            ),
            counterparty_key_type="SWIFT_ACCOUNT",
            counterparty_key_quality="STRONG",
            account_match_key=account_match_key,
        )

    if (
        normalized_rail == "LOCAL"
        and normalized_account is not None
    ):
        return CounterpartyIdentity(
            counterparty_key=(
                f"LOCAL_ACCOUNT|{normalized_account}"
            ),
            counterparty_key_type="LOCAL_ACCOUNT",
            counterparty_key_quality="MODERATE",
            account_match_key=account_match_key,
        )

    return CounterpartyIdentity(
        counterparty_key=None,
        counterparty_key_type=None,
        counterparty_key_quality="UNUSABLE",
        account_match_key=account_match_key,
    )


def _parse_date_column(
    frame: pd.DataFrame,
    column: str,
    dataset_name: str,
) -> pd.Series:
    parsed = pd.to_datetime(
        frame[column],
        errors="coerce",
    ).dt.date

    invalid_mask = parsed.isna()

    if invalid_mask.any():
        invalid_values = (
            frame.loc[invalid_mask, column]
            .astype("string")
            .drop_duplicates()
            .tolist()
        )

        raise SchemaValidationError(
            f"{dataset_name}.{column} contains invalid dates: "
            f"{invalid_values}"
        )

    return parsed


def _parse_timestamp_column(
    frame: pd.DataFrame,
    column: str,
    dataset_name: str,
) -> pd.Series:
    parsed = pd.to_datetime(
        frame[column],
        errors="coerce",
    )

    invalid_mask = parsed.isna()

    if invalid_mask.any():
        invalid_values = (
            frame.loc[invalid_mask, column]
            .astype("string")
            .drop_duplicates()
            .tolist()
        )

        raise SchemaValidationError(
            f"{dataset_name}.{column} contains invalid timestamps: "
            f"{invalid_values}"
        )

    return parsed


def _require_nonblank(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    dataset_name: str,
) -> None:
    for column in columns:
        blank_mask = (
            frame[column].isna()
            | frame[column]
            .astype("string")
            .str.strip()
            .eq("")
        )

        if blank_mask.any():
            raise SchemaValidationError(
                f"{dataset_name}.{column} contains null "
                "or blank values."
            )


def _normalize_text_columns(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> None:
    for column in columns:
        frame[column] = (
            frame[column]
            .astype("string")
            .str.strip()
        )


def _deduplicate_events(
    frame: pd.DataFrame,
    event_id_column: str,
    dataset_name: str,
) -> pd.DataFrame:
    """
    Remove exact duplicate source rows.

    Repeated event IDs with conflicting data remain invalid because one
    stable event identifier must describe one event.
    """
    deduplicated = frame.drop_duplicates().copy()

    conflicting_mask = deduplicated.duplicated(
        subset=[event_id_column],
        keep=False,
    )

    if conflicting_mask.any():
        conflicts = (
            deduplicated.loc[
                conflicting_mask,
                event_id_column,
            ]
            .drop_duplicates()
            .tolist()
        )

        raise SchemaValidationError(
            f"{dataset_name} contains conflicting rows for "
            f"{event_id_column}: {conflicts}"
        )

    return deduplicated.reset_index(drop=True)


def prepare_seed_mule_events(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """Validate and normalize FRC seed event records."""
    dataset_name = "seed_mule_events"

    validate_required_columns(
        frame=frame,
        required_columns=SEED_MULE_EVENT_REQUIRED_COLUMNS,
        dataset_name=dataset_name,
    )

    prepared = frame.copy()

    text_columns = tuple(
        column
        for column in SEED_MULE_EVENT_REQUIRED_COLUMNS
        if column not in {
            "snapshot_date",
            "date_reported",
        }
    )

    _normalize_text_columns(
        frame=prepared,
        columns=text_columns,
    )

    _require_nonblank(
        frame=prepared,
        columns=(
            "seed_event_id",
            "seed_customer_id",
            "cbs_txn_ref",
            "frc_rail",
            "frc_source",
        ),
        dataset_name=dataset_name,
    )

    prepared["snapshot_date"] = _parse_date_column(
        frame=prepared,
        column="snapshot_date",
        dataset_name=dataset_name,
    )

    prepared["date_reported"] = _parse_date_column(
        frame=prepared,
        column="date_reported",
        dataset_name=dataset_name,
    )

    prepared["frc_rail"] = prepared[
        "frc_rail"
    ].str.upper()

    invalid_rails = sorted(
        set(prepared["frc_rail"])
        - VALID_FRC_RAILS
    )

    if invalid_rails:
        raise SchemaValidationError(
            "seed_mule_events contains unsupported "
            f"frc_rail values: {invalid_rails}"
        )

    prepared["seed_account_number_normalized"] = (
        prepared["seed_account_number"]
        .map(normalize_identifier)
        .astype("string")
    )

    prepared["seed_iban_normalized"] = (
        prepared["seed_iban"]
        .map(normalize_identifier)
        .astype("string")
    )

    prepared["seed_account_match_key"] = (
        prepared.apply(
            lambda row: build_account_match_key(
                iban=row["seed_iban"],
                account_number=row[
                    "seed_account_number"
                ],
            ),
            axis=1,
        )
        .astype("string")
    )

    prepared = _deduplicate_events(
        frame=prepared,
        event_id_column="seed_event_id",
        dataset_name=dataset_name,
    )

    return (
        prepared
        .sort_values(
            by=[
                "date_reported",
                "seed_event_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def prepare_counterparty_events(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """Validate and normalize transfer and beneficiary events."""
    dataset_name = "counterparty_events"

    validate_required_columns(
        frame=frame,
        required_columns=COUNTERPARTY_EVENT_REQUIRED_COLUMNS,
        dataset_name=dataset_name,
    )

    prepared = frame.copy()

    text_columns = tuple(
        column
        for column in COUNTERPARTY_EVENT_REQUIRED_COLUMNS
        if column not in {
            "snapshot_date",
            "event_timestamp",
            "amount",
        }
    )

    _normalize_text_columns(
        frame=prepared,
        columns=text_columns,
    )

    _require_nonblank(
        frame=prepared,
        columns=(
            "event_id",
            "event_type",
            "rail",
            "customer_id",
            "status",
            "source_table",
        ),
        dataset_name=dataset_name,
    )

    prepared["snapshot_date"] = _parse_date_column(
        frame=prepared,
        column="snapshot_date",
        dataset_name=dataset_name,
    )

    prepared["event_timestamp"] = (
        _parse_timestamp_column(
            frame=prepared,
            column="event_timestamp",
            dataset_name=dataset_name,
        )
    )

    prepared["event_type"] = prepared[
        "event_type"
    ].str.upper()

    prepared["rail"] = prepared[
        "rail"
    ].str.upper()

    invalid_event_types = sorted(
        set(prepared["event_type"])
        - VALID_COUNTERPARTY_EVENT_TYPES
    )

    if invalid_event_types:
        raise SchemaValidationError(
            "counterparty_events contains unsupported "
            f"event_type values: {invalid_event_types}"
        )

    invalid_rails = sorted(
        set(prepared["rail"])
        - VALID_FRC_RAILS
    )

    if invalid_rails:
        raise SchemaValidationError(
            "counterparty_events contains unsupported "
            f"rail values: {invalid_rails}"
        )

    prepared["amount"] = pd.to_numeric(
        prepared["amount"].replace("", pd.NA),
        errors="coerce",
    )

    prepared["counterparty_account_number_normalized"] = (
        prepared["counterparty_account_number"]
        .map(normalize_identifier)
        .astype("string")
    )

    prepared["counterparty_iban_normalized"] = (
        prepared["counterparty_iban"]
        .map(normalize_identifier)
        .astype("string")
    )

    prepared["counterparty_swift_bic_normalized"] = (
        prepared["counterparty_swift_bic"]
        .map(normalize_identifier)
        .astype("string")
    )

    identities = [
        build_counterparty_identity(
            rail=row.rail,
            counterparty_iban=row.counterparty_iban,
            counterparty_swift_bic=(
                row.counterparty_swift_bic
            ),
            counterparty_account_number=(
                row.counterparty_account_number
            ),
        )
        for row in prepared.itertuples(index=False)
    ]

    prepared["counterparty_key"] = pd.Series(
        [
            identity.counterparty_key
            for identity in identities
        ],
        dtype="string",
    )

    prepared["counterparty_key_type"] = pd.Series(
        [
            identity.counterparty_key_type
            for identity in identities
        ],
        dtype="string",
    )

    prepared["counterparty_key_quality"] = [
        identity.counterparty_key_quality
        for identity in identities
    ]

    prepared["counterparty_account_match_key"] = (
        pd.Series(
            [
                identity.account_match_key
                for identity in identities
            ],
            dtype="string",
        )
    )

    prepared["counterparty_key_usable_flag"] = (
        prepared["counterparty_key"].notna()
    )

    prepared = _deduplicate_events(
        frame=prepared,
        event_id_column="event_id",
        dataset_name=dataset_name,
    )

    return (
        prepared
        .sort_values(
            by=[
                "event_timestamp",
                "event_id",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )
