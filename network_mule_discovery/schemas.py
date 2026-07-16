"""Schema contracts and normalization helpers for network discovery inputs."""

from __future__ import annotations

import re
from datetime import date

import pandas as pd


SEED_MULE_REQUIRED_COLUMNS = (
    "snapshot_date",
    "seed_customer_id",
    "seed_source",
)

CUSTOMER_IDENTITY_REQUIRED_COLUMNS = (
    "snapshot_date",
    "entity_type",
    "entity_id",
    "entity_key",
    "lookup_customer_id",
    "individual_id",
    "emirates_id_number",
    "entity_created_at",
)

VALID_ENTITY_TYPES = frozenset({"SME", "RETAIL"})


class SchemaValidationError(ValueError):
    """Raised when an input dataset violates its required contract."""


def normalize_text(value: object) -> str | None:
    """Return a stripped string or None for null and blank values."""
    if value is None or pd.isna(value):
        return None

    normalized = str(value).strip()
    return normalized or None


def normalize_emirates_id(value: object) -> str | None:
    """
    Normalize an Emirates ID consistently with the Databricks SQL contract.

    The normalization:
    - trims whitespace;
    - converts text to uppercase;
    - removes non-alphanumeric characters;
    - returns None when the result is blank.
    """
    text = normalize_text(value)

    if text is None:
        return None

    normalized = re.sub(r"[^0-9A-Z]", "", text.upper())
    return normalized or None


def is_linkable_emirates_id(value: object) -> bool:
    """
    Apply technical EID quality checks.

    This is not risk-based suppression. It only rejects values that cannot
    represent a usable Emirates ID for deterministic matching.
    """
    normalized = normalize_emirates_id(value)

    if normalized is None:
        return False

    return bool(re.fullmatch(r"784[0-9]{12}", normalized))


def parse_run_date(value: date | str) -> date:
    """Convert a date or ISO date string to a date object."""
    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(
            f"run_date must be an ISO date in YYYY-MM-DD format: {value!r}"
        ) from exc


def validate_required_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...],
    dataset_name: str,
) -> None:
    """Raise when required columns are absent."""
    missing_columns = [
        column
        for column in required_columns
        if column not in frame.columns
    ]

    if missing_columns:
        raise SchemaValidationError(
            f"{dataset_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def _parse_snapshot_date(
    frame: pd.DataFrame,
    dataset_name: str,
) -> pd.Series:
    parsed = pd.to_datetime(
        frame["snapshot_date"],
        errors="coerce",
    ).dt.date

    invalid_mask = parsed.isna()

    if invalid_mask.any():
        invalid_values = (
            frame.loc[invalid_mask, "snapshot_date"]
            .astype("string")
            .drop_duplicates()
            .tolist()
        )

        raise SchemaValidationError(
            f"{dataset_name} contains invalid snapshot_date values: "
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
            | frame[column].astype("string").str.strip().eq("")
        )

        if blank_mask.any():
            raise SchemaValidationError(
                f"{dataset_name}.{column} contains null or blank values."
            )


def prepare_seed_mules(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and standardize the seed mule dataset."""
    dataset_name = "seed_mule_pool"

    validate_required_columns(
        frame=frame,
        required_columns=SEED_MULE_REQUIRED_COLUMNS,
        dataset_name=dataset_name,
    )

    prepared = frame.copy()

    for column in ("seed_customer_id", "seed_source"):
        prepared[column] = (
            prepared[column]
            .astype("string")
            .str.strip()
        )

    _require_nonblank(
        frame=prepared,
        columns=("seed_customer_id", "seed_source"),
        dataset_name=dataset_name,
    )

    prepared["snapshot_date"] = _parse_snapshot_date(
        frame=prepared,
        dataset_name=dataset_name,
    )

    return prepared


def prepare_customer_identity(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and standardize the customer identity dataset."""
    dataset_name = "customer_identity"

    validate_required_columns(
        frame=frame,
        required_columns=CUSTOMER_IDENTITY_REQUIRED_COLUMNS,
        dataset_name=dataset_name,
    )

    prepared = frame.copy()

    text_columns = (
        "entity_type",
        "entity_id",
        "entity_key",
        "lookup_customer_id",
        "individual_id",
        "emirates_id_number",
    )

    for column in text_columns:
        prepared[column] = (
            prepared[column]
            .astype("string")
            .str.strip()
        )

    _require_nonblank(
        frame=prepared,
        columns=(
            "entity_type",
            "entity_id",
            "entity_key",
            "lookup_customer_id",
        ),
        dataset_name=dataset_name,
    )

    prepared["snapshot_date"] = _parse_snapshot_date(
        frame=prepared,
        dataset_name=dataset_name,
    )

    invalid_entity_types = sorted(
        set(prepared["entity_type"].dropna())
        - VALID_ENTITY_TYPES
    )

    if invalid_entity_types:
        raise SchemaValidationError(
            "customer_identity contains unsupported entity_type values: "
            f"{invalid_entity_types}"
        )

    expected_entity_key = (
        prepared["entity_type"]
        + "|"
        + prepared["entity_id"]
    )

    invalid_entity_key_mask = (
        prepared["entity_key"] != expected_entity_key
    )

    if invalid_entity_key_mask.any():
        invalid_keys = (
            prepared.loc[
                invalid_entity_key_mask,
                ["entity_type", "entity_id", "entity_key"],
            ]
            .drop_duplicates()
            .to_dict("records")
        )

        raise SchemaValidationError(
            "customer_identity contains entity_key values that do not "
            f"match entity_type and entity_id: {invalid_keys}"
        )

    prepared["emirates_id_raw"] = prepared["emirates_id_number"]

    prepared["emirates_id_number"] = (
        prepared["emirates_id_number"]
        .map(normalize_emirates_id)
        .astype("string")
    )

    prepared["eid_linkable_flag"] = (
        prepared["emirates_id_number"]
        .map(is_linkable_emirates_id)
        .astype(bool)
    )

    prepared["individual_id"] = (
        prepared["individual_id"]
        .replace("", pd.NA)
    )

    raw_created_at = prepared["entity_created_at"].astype("string")

    prepared["entity_created_at"] = pd.to_datetime(
        raw_created_at.replace("", pd.NA),
        errors="coerce",
    )

    invalid_created_at_mask = (
        raw_created_at.str.strip().ne("")
        & prepared["entity_created_at"].isna()
    )

    if invalid_created_at_mask.any():
        invalid_values = (
            raw_created_at.loc[invalid_created_at_mask]
            .drop_duplicates()
            .tolist()
        )

        raise SchemaValidationError(
            "customer_identity contains invalid entity_created_at values: "
            f"{invalid_values}"
        )

    return prepared
