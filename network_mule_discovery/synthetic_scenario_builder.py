"""Reusable helpers for production-shaped synthetic source scenarios."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from network_mule_discovery.synthetic_source_contracts import (
    CUSTOMER_ACCOUNT_MASTER_CONTRACT,
    CUSTOMER_IDENTITY_CONTRACT,
    LOCAL_INWARD_PAYMENTS_CONTRACT,
    LOCAL_OUTWARD_PAYMENTS_CONTRACT,
    RETAIL_BENEFICIARY_MASTER_CONTRACT,
    SEED_MULE_POOL_CONTRACT,
    SME_BENEFICIARY_MASTER_CONTRACT,
)


@dataclass(frozen=True)
class SyntheticAccountRecord:
    """One production-shaped synthetic customer account."""

    account_id: str
    customer_id: str
    entity_type: str
    account_number: str
    iban: str
    account_currency: str
    account_status: str
    account_opened_date: str
    account_closed_date: str = ""


def format_timestamp(value: datetime) -> str:
    """Format a timestamp for source CSVs."""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def format_date(value: date) -> str:
    """Format a date for source CSVs."""
    return value.isoformat()


def format_amount(value: float) -> str:
    """Format a monetary amount deterministically."""
    return f"{value:.2f}"


def uae_iban(bank_code: str, account_number: str) -> str:
    """Create a structurally valid-looking UAE IBAN."""
    account_component = account_number[-16:].zfill(16)
    return f"AE07{bank_code}{account_component}"


def external_iban(sequence: int) -> str:
    """Create a structurally valid-looking external UAE IBAN."""
    account_component = f"{7000000000000000 + sequence:016d}"
    return f"AE12{sequence % 900 + 100:03d}{account_component}"


def synthetic_eid(
    year: int,
    sequence: int,
    style: int = 0,
) -> str:
    """Create one Emirates-ID-shaped value."""
    seven_digit_sequence = f"{sequence:07d}"
    check_digit = str((sequence * 7 + year) % 10)
    digits = f"784{year}{seven_digit_sequence}{check_digit}"

    if style == 1:
        return (
            f"{digits[:3]}-{digits[3:7]}-"
            f"{digits[7:14]}-{digits[14:]}"
        )

    if style == 2:
        return (
            f"{digits[:3]} {digits[3:7]} "
            f"{digits[7:14]} {digits[14:]}"
        )

    return digits


def add_months(value: date, months: int) -> date:
    """Advance by whole months without external dependencies."""
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    day = min(value.day, 28)
    return date(year, month, day)


def hash_file(path: Path) -> str:
    """Return the SHA-256 digest for one file."""
    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        for chunk in iter(
            lambda: file_handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


class SyntheticScenarioBuilder:
    """Collect reusable production-shaped source rows."""

    def __init__(self, generation_seed: int) -> None:
        self.random = random.Random(generation_seed)
        self.identities: list[dict[str, object]] = []
        self.accounts: dict[str, SyntheticAccountRecord] = {}
        self.local_inwards: list[dict[str, object]] = []
        self.local_outwards: list[dict[str, object]] = []
        self.retail_beneficiaries: list[dict[str, object]] = []
        self.sme_beneficiaries: list[dict[str, object]] = []
        self.customer_created_dates: dict[str, str] = {}
        self._transfer_sequence = 0
        self._beneficiary_sequence = 0
        self._external_source_sequence = 0

    def add_customer(
        self,
        *,
        customer_id: str,
        entity_type: str,
        emirates_id_number: str,
        segment: str,
        customer_created_date: str,
        account_sequence: int,
        individual_id: str = "",
    ) -> None:
        """Add one identity and its primary AED account."""
        business_id = (
            customer_id
            if entity_type == "SME"
            else ""
        )

        self.identities.append(
            {
                "entity_type": entity_type,
                "entity_id": customer_id,
                "customer_id": customer_id,
                "business_id": business_id,
                "individual_id": individual_id,
                "emirates_id_number": emirates_id_number,
                "customer_segment": segment,
                "customer_status": "ACTIVE",
                "customer_created_date": customer_created_date,
            }
        )

        account_number = f"{account_sequence:012d}"

        self.accounts[customer_id] = SyntheticAccountRecord(
            account_id=f"ACC-{customer_id}-001",
            customer_id=customer_id,
            entity_type=entity_type,
            account_number=account_number,
            iban=uae_iban("033", account_number),
            account_currency="AED",
            account_status="ACTIVE",
            account_opened_date=customer_created_date,
        )

        self.customer_created_dates[
            customer_id
        ] = customer_created_date

    def add_beneficiary(
        self,
        *,
        owner_customer_id: str,
        account_number: str,
        account_holder_name: str,
        created_at: datetime,
        nick_name: str,
        bank_name: str = "Emirates Commercial Bank",
    ) -> str:
        """Add one customer-scoped local beneficiary."""
        self._beneficiary_sequence += 1

        entity_type = self.accounts[
            owner_customer_id
        ].entity_type

        prefix = (
            "SME"
            if entity_type == "SME"
            else "RET"
        )

        beneficiary_id = (
            f"{prefix}-BEN-"
            f"{self._beneficiary_sequence:06d}"
        )

        row: dict[str, object] = {
            "beneficiary_account_number": account_number,
            "source": "payment",
            "beneficiary_id": beneficiary_id,
            "customer_type": entity_type,
            "beneficiary_type": "local",
            "customer_creation_date": (
                self.customer_created_dates[
                    owner_customer_id
                ]
            ),
            "beneficiary_account_holder_name": (
                account_holder_name
            ),
            "nick_name": nick_name,
            "is_active": True,
            "country_of_beneficiary": "AE",
            "currency": "AED",
            "bank_name": bank_name,
            "swift_code": "",
            "beneficiary_created_date": format_timestamp(
                created_at
            ),
            "beneficiary_updated_date": format_timestamp(
                created_at + timedelta(hours=1)
            ),
            "legal_type": (
                "BUSINESS"
                if entity_type == "SME"
                else "PERSON"
            ),
            "two_factor_auth_status": "COMPLETED",
            "cooldown": "false",
            "beneficiary_address_first_line": (
                "Sheikh Zayed Road"
            ),
            "beneficiary_address_city": "Dubai",
            "beneficiary_address_state": "Dubai",
            "beneficiary_address_country": "AE",
            "beneficiary_address_post_code": "",
        }

        if entity_type == "SME":
            row["business_id"] = owner_customer_id
            self.sme_beneficiaries.append(row)

        else:
            row["customer_id"] = owner_customer_id
            self.retail_beneficiaries.append(row)

        return beneficiary_id

    def add_outward(
        self,
        *,
        customer_id: str,
        timestamp: datetime,
        beneficiary_id: str,
        beneficiary_account_number: str,
        amount: float,
        purpose_key: str,
        purpose_name: str,
    ) -> None:
        """Add one completed local outward payment."""
        self._transfer_sequence += 1
        account = self.accounts[customer_id]
        transfer_id = (
            f"LOC-OUT-{self._transfer_sequence:07d}"
        )

        self.local_outwards.append(
            {
                "transfer_id": transfer_id,
                "quote_id": f"QUOTE-{transfer_id}",
                "status": "COMPLETED",
                "reference_number": f"REF-{transfer_id}",
                "source_account_id": account.account_id,
                "direction": "OUTWARD",
                "customer_id": customer_id,
                "transaction_timestamp": format_timestamp(
                    timestamp
                ),
                "beneficiary_id": beneficiary_id,
                "beneficiary_account_number": (
                    beneficiary_account_number
                ),
                "source_amount": format_amount(amount),
                "target_amount": format_amount(amount),
                "payment_purpose_key": purpose_key,
                "payment_purpose_name": purpose_name,
                "service_type": "FTS",
                "source_iban": account.iban,
                "total_fees": "0.00",
                "fee_details": "[]",
            }
        )

    def add_inward(
        self,
        *,
        customer_id: str,
        timestamp: datetime,
        amount: float,
        purpose_key: str,
        purpose_name: str,
        source_iban: str | None = None,
        source_account_id: str | None = None,
    ) -> None:
        """Add one completed local inward payment."""
        self._transfer_sequence += 1
        self._external_source_sequence += 1
        account = self.accounts[customer_id]

        resolved_source_iban = (
            source_iban
            or external_iban(
                self._external_source_sequence
            )
        )

        resolved_source_account_id = (
            source_account_id
            or (
                "EXT-SRC-"
                f"{self._external_source_sequence:06d}"
            )
        )

        transfer_id = (
            f"LOC-IN-{self._transfer_sequence:07d}"
        )

        self.local_inwards.append(
            {
                "transfer_id": transfer_id,
                "quote_id": f"QUOTE-{transfer_id}",
                "status": "COMPLETED",
                "reference_number": f"REF-{transfer_id}",
                "source_account_id": (
                    resolved_source_account_id
                ),
                "direction": "INWARD",
                "customer_id": customer_id,
                "transaction_timestamp": format_timestamp(
                    timestamp
                ),
                "beneficiary_id": "",
                "beneficiary_account_number": (
                    account.account_number
                ),
                "beneficiary_iban": account.iban,
                "source_amount": format_amount(amount),
                "target_amount": format_amount(amount),
                "payment_purpose_key": purpose_key,
                "payment_purpose_name": purpose_name,
                "service_type": "FTS",
                "source_iban": resolved_source_iban,
                "total_fees": "0.00",
                "fee_details": "[]",
            }
        )

    def build_frames(
        self,
        seed_rows: list[dict[str, object]],
    ) -> dict[str, pd.DataFrame]:
        """Return all source frames in canonical CSV order."""
        account_rows = [
            account.__dict__
            for account in self.accounts.values()
        ]

        return {
            SEED_MULE_POOL_CONTRACT.filename: pd.DataFrame(
                seed_rows,
                columns=list(
                    SEED_MULE_POOL_CONTRACT.columns
                ),
            ),
            CUSTOMER_IDENTITY_CONTRACT.filename: pd.DataFrame(
                self.identities,
                columns=list(
                    CUSTOMER_IDENTITY_CONTRACT.columns
                ),
            ),
            CUSTOMER_ACCOUNT_MASTER_CONTRACT.filename: pd.DataFrame(
                account_rows,
                columns=list(
                    CUSTOMER_ACCOUNT_MASTER_CONTRACT.columns
                ),
            ),
            LOCAL_INWARD_PAYMENTS_CONTRACT.filename: pd.DataFrame(
                self.local_inwards,
                columns=list(
                    LOCAL_INWARD_PAYMENTS_CONTRACT.columns
                ),
            ),
            LOCAL_OUTWARD_PAYMENTS_CONTRACT.filename: pd.DataFrame(
                self.local_outwards,
                columns=list(
                    LOCAL_OUTWARD_PAYMENTS_CONTRACT.columns
                ),
            ),
            RETAIL_BENEFICIARY_MASTER_CONTRACT.filename: pd.DataFrame(
                self.retail_beneficiaries,
                columns=list(
                    RETAIL_BENEFICIARY_MASTER_CONTRACT.columns
                ),
            ),
            SME_BENEFICIARY_MASTER_CONTRACT.filename: pd.DataFrame(
                self.sme_beneficiaries,
                columns=list(
                    SME_BENEFICIARY_MASTER_CONTRACT.columns
                ),
            ),
        }
