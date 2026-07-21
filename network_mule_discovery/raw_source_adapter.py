"""Convert production-shaped source CSVs into canonical discovery inputs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from network_mule_discovery.counterparty_schemas import (
    COUNTERPARTY_EVENT_REQUIRED_COLUMNS,
    SEED_MULE_EVENT_REQUIRED_COLUMNS,
    prepare_counterparty_events,
    prepare_seed_mule_events,
)
from network_mule_discovery.schemas import (
    CUSTOMER_IDENTITY_REQUIRED_COLUMNS,
    SEED_MULE_REQUIRED_COLUMNS,
    parse_run_date,
    prepare_customer_identity,
    prepare_seed_mules,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_1_SOURCE_CONTRACTS,
    CsvSourceContract,
)


CANONICAL_SEED_MULE_POOL_FILENAME = "seed_mule_pool.csv"
CANONICAL_CUSTOMER_IDENTITY_FILENAME = "customer_identity.csv"
CANONICAL_SEED_MULE_EVENTS_FILENAME = "seed_mule_events.csv"
CANONICAL_COUNTERPARTY_EVENTS_FILENAME = "counterparty_events.csv"
CANONICAL_MANIFEST_FILENAME = "canonical_source_manifest.json"


class RawSourceAdapterError(RuntimeError):
    """Raw source data cannot be converted safely."""




@dataclass(frozen=True)
class RawDiscoverySources:
    """Validated production-shaped source frames."""

    seed_mule_pool: pd.DataFrame
    customer_identity: pd.DataFrame
    customer_accounts: pd.DataFrame
    local_inward_payments: pd.DataFrame
    local_outward_payments: pd.DataFrame
    retail_beneficiaries: pd.DataFrame
    sme_beneficiaries: pd.DataFrame


@dataclass(frozen=True)
class CanonicalDiscoveryInputs:
    """Canonical frames consumed by the existing discovery pipeline."""

    seed_mules: pd.DataFrame
    customer_identity: pd.DataFrame
    seed_mule_events: pd.DataFrame
    counterparty_events: pd.DataFrame


@dataclass(frozen=True)
class CanonicalDiscoveryPaths:
    """Paths written for the CSV-backed discovery data source."""

    output_directory: Path
    seed_mule_pool_path: Path
    customer_identity_path: Path
    seed_mule_events_path: Path
    counterparty_events_path: Path
    manifest_path: Path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        for chunk in iter(
            lambda: file_handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def _read_contract_csv(
    source_directory: Path,
    contract: CsvSourceContract,
) -> pd.DataFrame:
    path = source_directory / contract.filename

    if not path.is_file():
        raise RawSourceAdapterError(
            f"Required raw source file does not exist: {path}"
        )

    frame = pd.read_csv(
        path,
        dtype="string",
        keep_default_na=False,
    )

    missing_columns = [
        column
        for column in contract.columns
        if column not in frame.columns
    ]

    unexpected_columns = [
        column
        for column in frame.columns
        if column not in contract.columns
    ]

    if missing_columns or unexpected_columns:
        raise RawSourceAdapterError(
            f"{contract.filename} does not match its source contract. "
            f"Missing columns: {missing_columns}. "
            f"Unexpected columns: {unexpected_columns}."
        )

    frame = frame[list(contract.columns)].copy()

    for column in contract.required_nonblank:
        blank_mask = (
            frame[column]
            .astype("string")
            .fillna("")
            .str.strip()
            .eq("")
        )

        if blank_mask.any():
            raise RawSourceAdapterError(
                f"{contract.filename}.{column} contains blank values."
            )

    if contract.unique_keys:
        duplicate_mask = frame.duplicated(
            subset=list(contract.unique_keys),
            keep=False,
        )

        if duplicate_mask.any():
            examples = (
                frame.loc[
                    duplicate_mask,
                    list(contract.unique_keys),
                ]
                .drop_duplicates()
                .head(10)
                .to_dict("records")
            )

            raise RawSourceAdapterError(
                f"{contract.filename} contains duplicate keys: {examples}"
            )

    return frame


def load_raw_discovery_sources(
    source_directory: Path | str,
) -> RawDiscoverySources:
    """Load and validate all production-shaped source CSVs."""
    resolved_source_directory = Path(source_directory)

    loaded = {
        contract.filename: _read_contract_csv(
            resolved_source_directory,
            contract,
        )
        for contract in SCENARIO_1_SOURCE_CONTRACTS
    }

    return RawDiscoverySources(
        seed_mule_pool=loaded["seed_mule_pool.csv"],
        customer_identity=loaded["customer_identity.csv"],
        customer_accounts=loaded["customer_account_master.csv"],
        local_inward_payments=loaded["local_inward_payments.csv"],
        local_outward_payments=loaded["local_outward_payments.csv"],
        retail_beneficiaries=loaded["retail_beneficiary_master.csv"],
        sme_beneficiaries=loaded["sme_beneficiary_master.csv"],
    )


def _customer_account_lookup(
    accounts: pd.DataFrame,
) -> pd.DataFrame:
    """Return one active account per customer for event enrichment."""
    active_accounts = accounts.loc[
        accounts["account_status"]
        .astype("string")
        .str.upper()
        .eq("ACTIVE")
    ].copy()

    active_accounts["account_opened_date_parsed"] = pd.to_datetime(
        active_accounts["account_opened_date"],
        errors="coerce",
    )

    invalid_dates = active_accounts[
        "account_opened_date_parsed"
    ].isna()

    if invalid_dates.any():
        raise RawSourceAdapterError(
            "customer_account_master.csv contains invalid "
            "account_opened_date values."
        )

    duplicate_customer_mask = active_accounts.duplicated(
        subset=["customer_id"],
        keep=False,
    )

    if duplicate_customer_mask.any():
        active_accounts = (
            active_accounts
            .sort_values(
                by=[
                    "customer_id",
                    "account_opened_date_parsed",
                    "account_id",
                ],
                kind="stable",
            )
            .drop_duplicates(
                subset=["customer_id"],
                keep="first",
            )
        )

    return active_accounts[
        [
            "customer_id",
            "account_id",
            "account_number",
            "iban",
            "account_currency",
        ]
    ].reset_index(drop=True)


def _beneficiary_union(
    retail_beneficiaries: pd.DataFrame,
    sme_beneficiaries: pd.DataFrame,
) -> pd.DataFrame:
    retail = retail_beneficiaries.copy()

    sme = sme_beneficiaries.rename(
        columns={
            "business_id": "customer_id",
        }
    ).copy()

    combined = pd.concat(
        [retail, sme],
        ignore_index=True,
        sort=False,
    )

    combined["customer_id"] = (
        combined["customer_id"]
        .astype("string")
        .str.strip()
    )

    duplicate_mask = combined.duplicated(
        subset=["customer_id", "beneficiary_id"],
        keep=False,
    )

    if duplicate_mask.any():
        examples = (
            combined.loc[
                duplicate_mask,
                ["customer_id", "beneficiary_id"],
            ]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )

        raise RawSourceAdapterError(
            "Beneficiary sources contain conflicting customer-scoped "
            f"beneficiary IDs: {examples}"
        )

    return combined.reset_index(drop=True)


def _prepare_seed_mules_raw(
    seed_pool: pd.DataFrame,
) -> pd.DataFrame:
    canonical = seed_pool[
        [
            "snapshot_date",
            "seed_customer_id",
            "seed_source",
        ]
    ].copy()

    canonical = canonical.drop_duplicates().reset_index(drop=True)

    prepare_seed_mules(canonical)

    return canonical[list(SEED_MULE_REQUIRED_COLUMNS)]


def _prepare_customer_identity_raw(
    customer_identity: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    canonical = pd.DataFrame(
        {
            "snapshot_date": str(run_date),
            "entity_type": customer_identity["entity_type"],
            "entity_id": customer_identity["entity_id"],
            "entity_key": (
                customer_identity["entity_type"]
                .astype("string")
                .str.strip()
                + "|"
                + customer_identity["entity_id"]
                .astype("string")
                .str.strip()
            ),
            "lookup_customer_id": customer_identity["customer_id"],
            "individual_id": customer_identity["individual_id"],
            "emirates_id_number": customer_identity[
                "emirates_id_number"
            ],
            "entity_created_at": customer_identity[
                "customer_created_date"
            ],
        }
    )

    prepare_customer_identity(canonical)

    return canonical[list(CUSTOMER_IDENTITY_REQUIRED_COLUMNS)]


def _prepare_seed_mule_events_raw(
    seed_pool: pd.DataFrame,
) -> pd.DataFrame:
    source_event_type = (
        seed_pool["source_event_type"]
        .astype("string")
        .str.upper()
    )

    rail = source_event_type.map(
        lambda value: (
            "INTERNATIONAL"
            if "SWIFT" in value
            or "INTERNATIONAL" in value
            else "LOCAL"
        )
    )

    canonical = pd.DataFrame(
        {
            "snapshot_date": seed_pool["snapshot_date"],
            "seed_event_id": seed_pool["seed_event_id"],
            "seed_customer_id": seed_pool["seed_customer_id"],
            "cbs_txn_ref": seed_pool[
                "source_transaction_reference"
            ].where(
                seed_pool[
                    "source_transaction_reference"
                ].astype("string").str.strip().ne(""),
                seed_pool["seed_event_id"],
            ),
            "date_reported": seed_pool["date_reported"],
            "frc_rail": rail,
            "frc_source": seed_pool["seed_source"],
            "source_payment_transfer_id": seed_pool[
                "source_transaction_reference"
            ],
            "seed_account_number": seed_pool[
                "seed_account_number"
            ],
            "seed_iban": seed_pool["seed_iban"],
        }
    )

    prepare_seed_mule_events(canonical)

    return canonical[list(SEED_MULE_EVENT_REQUIRED_COLUMNS)]


def _completed_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["status"]
        .astype("string")
        .str.upper()
        .eq("COMPLETED")
    )


def _on_or_before_run_date_mask(
    frame: pd.DataFrame,
    timestamp_column: str,
    run_date: date,
    dataset_name: str,
) -> pd.Series:
    """Exclude invalid and future-dated source records."""
    timestamps = pd.to_datetime(
        frame[timestamp_column],
        errors="coerce",
    )

    if timestamps.isna().any():
        invalid_values = (
            frame.loc[
                timestamps.isna(),
                timestamp_column,
            ]
            .astype("string")
            .drop_duplicates()
            .tolist()
        )

        raise RawSourceAdapterError(
            f"{dataset_name}.{timestamp_column} contains "
            f"invalid timestamps: {invalid_values}"
        )

    exclusive_cutoff = (
        pd.Timestamp(run_date)
        + pd.Timedelta(days=1)
    )

    return timestamps.lt(exclusive_cutoff)


def _build_outward_events(
    outward: pd.DataFrame,
    accounts: pd.DataFrame,
    beneficiaries: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    completed = outward.loc[
        _completed_mask(outward)
        & _on_or_before_run_date_mask(
            frame=outward,
            timestamp_column="transaction_timestamp",
            run_date=run_date,
            dataset_name="local_outward_payments",
        )
    ].copy()

    account_enrichment = accounts.rename(
        columns={
            "account_number": "resolved_wio_account_number",
            "iban": "resolved_wio_iban",
            "account_currency": "resolved_currency",
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

    beneficiary_enrichment = beneficiaries[
        [
            "customer_id",
            "beneficiary_id",
            "beneficiary_account_number",
            "beneficiary_account_holder_name",
            "country_of_beneficiary",
            "currency",
            "swift_code",
        ]
    ].rename(
        columns={
            "beneficiary_account_number": (
                "master_beneficiary_account_number"
            ),
            "beneficiary_account_holder_name": (
                "counterparty_name"
            ),
            "country_of_beneficiary": (
                "counterparty_country"
            ),
            "currency": "beneficiary_currency",
            "swift_code": "counterparty_swift_bic",
        }
    )

    completed = completed.merge(
        beneficiary_enrichment,
        how="left",
        on=["customer_id", "beneficiary_id"],
        validate="many_to_one",
    )

    missing_account_mask = (
        completed["resolved_wio_account_number"].isna()
        | completed["resolved_wio_iban"].isna()
    )

    if missing_account_mask.any():
        missing_ids = (
            completed.loc[
                missing_account_mask,
                "source_account_id",
            ]
            .drop_duplicates()
            .tolist()
        )

        raise RawSourceAdapterError(
            "Local outward payments reference unknown Wio account IDs: "
            f"{missing_ids}"
        )

    missing_beneficiary_mask = (
        completed["counterparty_name"].isna()
        | completed["master_beneficiary_account_number"].isna()
    )

    if missing_beneficiary_mask.any():
        missing_keys = (
            completed.loc[
                missing_beneficiary_mask,
                ["customer_id", "beneficiary_id"],
            ]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )

        raise RawSourceAdapterError(
            "Local outward payments could not resolve customer-scoped "
            f"beneficiaries: {missing_keys}"
        )

    mismatch_mask = (
        completed["beneficiary_account_number"]
        .astype("string")
        .str.strip()
        != completed[
            "master_beneficiary_account_number"
        ]
        .astype("string")
        .str.strip()
    )

    if mismatch_mask.any():
        examples = (
            completed.loc[
                mismatch_mask,
                [
                    "customer_id",
                    "beneficiary_id",
                    "beneficiary_account_number",
                    "master_beneficiary_account_number",
                ],
            ]
            .head(10)
            .to_dict("records")
        )

        raise RawSourceAdapterError(
            "Local outward beneficiary account values conflict with "
            f"the beneficiary master: {examples}"
        )

    return pd.DataFrame(
        {
            "snapshot_date": str(run_date),
            "event_id": (
                "LOCAL_OUTWARD|"
                + completed["transfer_id"].astype("string")
            ),
            "event_type": "TRANSFER_SENT",
            "rail": "LOCAL",
            "customer_id": completed["customer_id"],
            "event_timestamp": completed[
                "transaction_timestamp"
            ],
            "transfer_id": completed["transfer_id"],
            "reference_number": completed["reference_number"],
            "beneficiary_id": completed["beneficiary_id"],
            "status": completed["status"],
            "wio_account_number": completed[
                "resolved_wio_account_number"
            ],
            "wio_iban": completed["resolved_wio_iban"],
            "counterparty_account_number": completed[
                "beneficiary_account_number"
            ],
            "counterparty_iban": "",
            "counterparty_swift_bic": completed[
                "counterparty_swift_bic"
            ].fillna(""),
            "counterparty_name": completed[
                "counterparty_name"
            ].fillna(""),
            "counterparty_country": completed[
                "counterparty_country"
            ].fillna("AE"),
            "amount": completed["target_amount"],
            "currency": completed[
                "beneficiary_currency"
            ].where(
                completed[
                    "beneficiary_currency"
                ].astype("string").str.strip().ne(""),
                completed["resolved_currency"],
            ),
            "source_table": "local_outward_payments",
        }
    )


def _build_inward_events(
    inward: pd.DataFrame,
    accounts: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    completed = inward.loc[
        _completed_mask(inward)
        & _on_or_before_run_date_mask(
            frame=inward,
            timestamp_column="transaction_timestamp",
            run_date=run_date,
            dataset_name="local_inward_payments",
        )
    ].copy()

    account_enrichment = accounts.rename(
        columns={
            "account_number": "resolved_wio_account_number",
            "iban": "resolved_wio_iban",
            "account_currency": "resolved_currency",
        }
    )

    completed = completed.merge(
        account_enrichment,
        how="left",
        on="customer_id",
        validate="many_to_one",
    )

    missing_account_mask = (
        completed["resolved_wio_account_number"].isna()
        | completed["resolved_wio_iban"].isna()
    )

    if missing_account_mask.any():
        missing_customers = (
            completed.loc[
                missing_account_mask,
                "customer_id",
            ]
            .drop_duplicates()
            .tolist()
        )

        raise RawSourceAdapterError(
            "Local inward payments reference customers without an "
            f"active Wio account: {missing_customers}"
        )

    destination_account_mismatch = (
        completed["beneficiary_account_number"]
        .astype("string")
        .str.strip()
        != completed["resolved_wio_account_number"]
        .astype("string")
        .str.strip()
    )

    destination_iban_mismatch = (
        completed["beneficiary_iban"]
        .astype("string")
        .str.strip()
        != completed["resolved_wio_iban"]
        .astype("string")
        .str.strip()
    )

    if (
        destination_account_mismatch.any()
        or destination_iban_mismatch.any()
    ):
        raise RawSourceAdapterError(
            "Local inward destination account details conflict with "
            "customer_account_master.csv."
        )

    return pd.DataFrame(
        {
            "snapshot_date": str(run_date),
            "event_id": (
                "LOCAL_INWARD|"
                + completed["transfer_id"].astype("string")
            ),
            "event_type": "TRANSFER_RECEIVED",
            "rail": "LOCAL",
            "customer_id": completed["customer_id"],
            "event_timestamp": completed[
                "transaction_timestamp"
            ],
            "transfer_id": completed["transfer_id"],
            "reference_number": completed["reference_number"],
            "beneficiary_id": completed["beneficiary_id"],
            "status": completed["status"],
            "wio_account_number": completed[
                "resolved_wio_account_number"
            ],
            "wio_iban": completed["resolved_wio_iban"],
            "counterparty_account_number": "",
            "counterparty_iban": completed["source_iban"],
            "counterparty_swift_bic": "",
            "counterparty_name": "",
            "counterparty_country": "AE",
            "amount": completed["source_amount"],
            "currency": completed["resolved_currency"],
            "source_table": "local_inward_payments",
        }
    )


def _build_beneficiary_events(
    beneficiaries: pd.DataFrame,
    accounts: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    eligible_beneficiaries = beneficiaries.loc[
        _on_or_before_run_date_mask(
            frame=beneficiaries,
            timestamp_column="beneficiary_created_date",
            run_date=run_date,
            dataset_name="beneficiary_master",
        )
    ].copy()

    account_enrichment = accounts.rename(
        columns={
            "account_number": "resolved_wio_account_number",
            "iban": "resolved_wio_iban",
        }
    )

    enriched = eligible_beneficiaries.merge(
        account_enrichment,
        how="left",
        on="customer_id",
        validate="many_to_one",
    )

    missing_account_mask = (
        enriched["resolved_wio_account_number"].isna()
        | enriched["resolved_wio_iban"].isna()
    )

    if missing_account_mask.any():
        missing_customers = (
            enriched.loc[
                missing_account_mask,
                "customer_id",
            ]
            .drop_duplicates()
            .tolist()
        )

        raise RawSourceAdapterError(
            "Beneficiary records reference customers without an active "
            f"Wio account: {missing_customers}"
        )

    active_status = (
        enriched["is_active"]
        .astype("string")
        .str.lower()
        .map(
            {
                "true": "ACTIVE",
                "1": "ACTIVE",
                "yes": "ACTIVE",
                "false": "INACTIVE",
                "0": "INACTIVE",
                "no": "INACTIVE",
            }
        )
        .fillna("UNKNOWN")
    )

    return pd.DataFrame(
        {
            "snapshot_date": str(run_date),
            "event_id": (
                "BENEFICIARY|"
                + enriched["beneficiary_id"].astype("string")
            ),
            "event_type": "BENEFICIARY_ADDED",
            "rail": "LOCAL",
            "customer_id": enriched["customer_id"],
            "event_timestamp": enriched[
                "beneficiary_created_date"
            ],
            "transfer_id": "",
            "reference_number": "",
            "beneficiary_id": enriched["beneficiary_id"],
            "status": active_status,
            "wio_account_number": enriched[
                "resolved_wio_account_number"
            ],
            "wio_iban": enriched["resolved_wio_iban"],
            "counterparty_account_number": enriched[
                "beneficiary_account_number"
            ],
            "counterparty_iban": "",
            "counterparty_swift_bic": enriched[
                "swift_code"
            ],
            "counterparty_name": enriched[
                "beneficiary_account_holder_name"
            ],
            "counterparty_country": enriched[
                "country_of_beneficiary"
            ],
            "amount": "",
            "currency": enriched["currency"],
            "source_table": "beneficiary_master",
        }
    )


def _prepare_counterparty_events_raw(
    *,
    local_inward: pd.DataFrame,
    local_outward: pd.DataFrame,
    accounts: pd.DataFrame,
    beneficiaries: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    account_lookup = _customer_account_lookup(
        accounts
    )

    events = pd.concat(
        [
            _build_outward_events(
                outward=local_outward,
                accounts=account_lookup,
                beneficiaries=beneficiaries,
                run_date=run_date,
            ),
            _build_inward_events(
                inward=local_inward,
                accounts=account_lookup,
                run_date=run_date,
            ),
            _build_beneficiary_events(
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
        events
        .sort_values(
            by=["event_timestamp", "event_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def build_canonical_discovery_inputs(
    *,
    source_directory: Path | str,
    run_date: date | str,
) -> CanonicalDiscoveryInputs:
    """Build canonical discovery frames from production-shaped sources."""
    resolved_source_directory = Path(
        source_directory
    )

    resolved_run_date = parse_run_date(
        run_date
    )

    sources = load_raw_discovery_sources(
        resolved_source_directory
    )

    seed_pool = sources.seed_mule_pool

    source_snapshot_dates = sorted(
        seed_pool["snapshot_date"]
        .astype("string")
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    if source_snapshot_dates != [
        str(resolved_run_date)
    ]:
        raise RawSourceAdapterError(
            "seed_mule_pool.csv snapshot_date must match run_date. "
            f"Found {source_snapshot_dates}; expected "
            f"{resolved_run_date}."
        )

    beneficiaries = _beneficiary_union(
        sources.retail_beneficiaries,
        sources.sme_beneficiaries,
    )

    return CanonicalDiscoveryInputs(
        seed_mules=_prepare_seed_mules_raw(
            seed_pool
        ),
        customer_identity=(
            _prepare_customer_identity_raw(
                sources.customer_identity,
                resolved_run_date,
            )
        ),
        seed_mule_events=(
            _prepare_seed_mule_events_raw(
                seed_pool
            )
        ),
        counterparty_events=(
            _prepare_counterparty_events_raw(
                local_inward=sources.local_inward_payments,
                local_outward=sources.local_outward_payments,
                accounts=sources.customer_accounts,
                beneficiaries=beneficiaries,
                run_date=resolved_run_date,
            )
        ),
    )


def write_canonical_discovery_inputs(
    *,
    source_directory: Path | str,
    output_directory: Path | str,
    run_date: date | str,
) -> CanonicalDiscoveryPaths:
    """Write canonical discovery inputs and an audit manifest."""
    resolved_output_directory = Path(
        output_directory
    )

    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    inputs = build_canonical_discovery_inputs(
        source_directory=source_directory,
        run_date=run_date,
    )

    paths = CanonicalDiscoveryPaths(
        output_directory=resolved_output_directory,
        seed_mule_pool_path=(
            resolved_output_directory
            / CANONICAL_SEED_MULE_POOL_FILENAME
        ),
        customer_identity_path=(
            resolved_output_directory
            / CANONICAL_CUSTOMER_IDENTITY_FILENAME
        ),
        seed_mule_events_path=(
            resolved_output_directory
            / CANONICAL_SEED_MULE_EVENTS_FILENAME
        ),
        counterparty_events_path=(
            resolved_output_directory
            / CANONICAL_COUNTERPARTY_EVENTS_FILENAME
        ),
        manifest_path=(
            resolved_output_directory
            / CANONICAL_MANIFEST_FILENAME
        ),
    )

    inputs.seed_mules.to_csv(
        paths.seed_mule_pool_path,
        index=False,
    )

    inputs.customer_identity.to_csv(
        paths.customer_identity_path,
        index=False,
    )

    inputs.seed_mule_events.to_csv(
        paths.seed_mule_events_path,
        index=False,
    )

    inputs.counterparty_events.to_csv(
        paths.counterparty_events_path,
        index=False,
    )

    manifest = {
        "run_date": str(parse_run_date(run_date)),
        "source_directory": str(
            Path(source_directory).resolve()
        ),
        "output_directory": str(
            resolved_output_directory.resolve()
        ),
        "row_counts": {
            CANONICAL_SEED_MULE_POOL_FILENAME: len(
                inputs.seed_mules
            ),
            CANONICAL_CUSTOMER_IDENTITY_FILENAME: len(
                inputs.customer_identity
            ),
            CANONICAL_SEED_MULE_EVENTS_FILENAME: len(
                inputs.seed_mule_events
            ),
            CANONICAL_COUNTERPARTY_EVENTS_FILENAME: len(
                inputs.counterparty_events
            ),
        },
        "sha256": {
            CANONICAL_SEED_MULE_POOL_FILENAME: _sha256_file(
                paths.seed_mule_pool_path
            ),
            CANONICAL_CUSTOMER_IDENTITY_FILENAME: _sha256_file(
                paths.customer_identity_path
            ),
            CANONICAL_SEED_MULE_EVENTS_FILENAME: _sha256_file(
                paths.seed_mule_events_path
            ),
            CANONICAL_COUNTERPARTY_EVENTS_FILENAME: _sha256_file(
                paths.counterparty_events_path
            ),
        },
        "prebuilt_groups": 0,
        "prebuilt_nodes": 0,
        "prebuilt_edges": 0,
        "supplied_ai_decisions": 0,
    }

    paths.manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return paths
