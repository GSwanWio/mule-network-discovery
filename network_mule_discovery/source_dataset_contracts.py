"""Dataset-level validation for provider-neutral source bundles."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import pandas as pd

from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceContractError,
)
from network_mule_discovery.synthetic_source_contracts import (
    SYNTHETIC_SOURCE_CONTRACTS,
)


@dataclass(frozen=True)
class SourceDatasetContract:
    dataset_name: str
    columns: tuple[str, ...]
    required_nonblank: tuple[str, ...] = ()
    unique_keys: tuple[str, ...] = ()
    allow_nonempty: bool = True


_existing = {
    Path(item.filename).stem: SourceDatasetContract(
        dataset_name=Path(item.filename).stem,
        columns=item.columns,
        required_nonblank=item.required_nonblank,
        unique_keys=item.unique_keys,
    )
    for item in SYNTHETIC_SOURCE_CONTRACTS
}


INTERNATIONAL_INWARD_PAYMENTS_CONTRACT = (
    SourceDatasetContract(
        dataset_name=(
            "international_inward_payments"
        ),
        columns=(
            "transfer_id",
            "status",
            "reference_number",
            "source_customer_iban",
            "quote_id",
            "account_currency",
            "direction",
            "customer_id",
            "transaction_timestamp",
            "beneficiary_account_number",
            "beneficiary_iban",
            "source_amount",
            "source_currency",
            "source_country_code",
            "target_amount",
            "target_currency",
            "payment_purpose_key",
            "product_type",
            "service_type",
            "total_fees",
            "gid_id",
            "transaction_key",
        ),
        required_nonblank=(
            "transfer_id",
            "status",
            "direction",
            "customer_id",
            "transaction_timestamp",
            "source_amount",
            "source_currency",
            "target_amount",
            "target_currency",
        ),
        unique_keys=(
            "transfer_id",
        ),
    )
)


INTERNATIONAL_OUTWARD_PAYMENTS_CONTRACT = (
    SourceDatasetContract(
        dataset_name=(
            "international_outward_payments"
        ),
        columns=(
            "transfer_id",
            "reference_number",
            "source_account_id",
            "quote_id",
            "customer_id",
            "transaction_timestamp",
            "status",
            "provider_type",
            "product_type",
            "service_type",
            "beneficiary_id",
            "beneficiary_account_number",
            "beneficiary_swift_code",
            "source_amount",
            "source_currency",
            "source_country_code",
            "target_amount",
            "target_currency",
            "target_country_code",
            "payment_purpose_key",
            "source_iban_number",
            "total_fees",
            "account_currency",
            "wio_fee_amount",
            "wio_fee_currency",
            "wio_fee_aed",
            "correspondent_fee_amount",
            "correspondent_fee_currency",
            "correspondent_bank_fee_aed",
        ),
        required_nonblank=(
            "transfer_id",
            "source_account_id",
            "customer_id",
            "transaction_timestamp",
            "status",
            "beneficiary_account_number",
            "source_amount",
            "source_currency",
            "target_amount",
            "target_currency",
        ),
        unique_keys=(
            "transfer_id",
        ),
    )
)


_existing.update(
    {
        "international_inward_payments": (
            INTERNATIONAL_INWARD_PAYMENTS_CONTRACT
        ),
        "international_outward_payments": (
            INTERNATIONAL_OUTWARD_PAYMENTS_CONTRACT
        ),
    }
)

SOURCE_DATASET_CONTRACTS: Mapping[
    str,
    SourceDatasetContract,
] = MappingProxyType(
    {
        name: _existing.get(
            name,
            SourceDatasetContract(
                dataset_name=name,
                columns=(),
                allow_nonempty=False,
            ),
        )
        for name in SOURCE_DATASET_NAMES
    }
)


def validate_source_frame(
    dataset_name: str,
    frame: pd.DataFrame,
) -> pd.DataFrame:
    contract = SOURCE_DATASET_CONTRACTS.get(
        dataset_name
    )

    if contract is None:
        raise SourceContractError(
            f"Unknown source dataset: {dataset_name}"
        )

    if not isinstance(frame, pd.DataFrame):
        raise SourceContractError(
            f"{dataset_name} must be a pandas DataFrame."
        )

    if not contract.allow_nonempty:
        if not frame.empty:
            raise SourceContractError(
                f"{dataset_name} is not yet supported "
                "as a populated source dataset."
            )

        if len(frame.columns) != 0:
            raise SourceContractError(
                f"{dataset_name} must currently be an "
                "empty DataFrame with no columns."
            )

        return frame.copy()

    missing = [
        column
        for column in contract.columns
        if column not in frame.columns
    ]

    unexpected = [
        column
        for column in frame.columns
        if column not in contract.columns
    ]

    if missing or unexpected:
        raise SourceContractError(
            f"{dataset_name} does not match its dataset contract. "
            f"Missing columns: {missing}. "
            f"Unexpected columns: {unexpected}."
        )

    result = frame.loc[
        :,
        list(contract.columns),
    ].copy()

    for column in contract.required_nonblank:
        blank = (
            result[column]
            .astype("string")
            .fillna("")
            .str.strip()
            .eq("")
        )

        if blank.any():
            raise SourceContractError(
                f"{dataset_name}.{column} "
                "contains blank values."
            )

    if contract.unique_keys:
        duplicate = result.duplicated(
            subset=list(contract.unique_keys),
            keep=False,
        )

        if duplicate.any():
            examples = (
                result.loc[
                    duplicate,
                    list(contract.unique_keys),
                ]
                .drop_duplicates()
                .head(10)
                .to_dict("records")
            )

            raise SourceContractError(
                f"{dataset_name} contains duplicate "
                f"keys: {examples}"
            )

    return result


def validate_source_bundle(
    bundle: DiscoverySourceBundle,
) -> DiscoverySourceBundle:
    if not isinstance(
        bundle,
        DiscoverySourceBundle,
    ):
        raise SourceContractError(
            "bundle must be a DiscoverySourceBundle."
        )

    frames = {
        name: validate_source_frame(
            name,
            frame,
        )
        for name, frame
        in bundle.as_mapping().items()
    }

    return DiscoverySourceBundle(
        metadata=bundle.metadata,
        **frames,
    )
