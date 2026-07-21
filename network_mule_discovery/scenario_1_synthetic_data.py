"""Generate production-shaped synthetic source data for Scenario 1."""

from __future__ import annotations

import hashlib
import json
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
    SCENARIO_1_SOURCE_CONTRACTS,
    SEED_MULE_POOL_CONTRACT,
    SME_BENEFICIARY_MASTER_CONTRACT,
)


GENERATION_SEED = 20260720
SCENARIO_NAME = "scenario_1_mixed_eid_and_counterparty_branches"
RUN_DATE = date(2026, 7, 20)

RISK_COUNTERPARTY_1_ACCOUNT = "990100000001"
RISK_COUNTERPARTY_2_ACCOUNT = "990200000001"
LEGITIMATE_COUNTERPARTY_ACCOUNT = "880100000001"

RISK_COUNTERPARTY_1_NAME = "Mosaic General Trading LLC"
RISK_COUNTERPARTY_2_NAME = "Orion Digital Services LLC"
LEGITIMATE_COUNTERPARTY_NAME = "Horizon Facilities Services LLC"

CORE_CUSTOMER_IDS = (
    "R1001",
    "B2001",
    "R1002",
    "R1003",
    "B2002",
    "R1004",
    "R1005",
    "R1006",
    "R1007",
)


@dataclass(frozen=True)
class AccountRecord:
    """Synthetic customer account used by the generator."""

    account_id: str
    customer_id: str
    entity_type: str
    account_number: str
    iban: str
    account_currency: str
    account_status: str
    account_opened_date: str
    account_closed_date: str = ""


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _date(value: date) -> str:
    return value.isoformat()


def _amount(value: float) -> str:
    return f"{value:.2f}"


def _digits_only(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _uae_iban(bank_code: str, account_number: str) -> str:
    """Create a structurally valid-looking UAE IBAN."""
    account_component = account_number[-16:].zfill(16)
    return f"AE07{bank_code}{account_component}"


def _external_iban(sequence: int) -> str:
    """Create a structurally valid-looking external UAE IBAN."""
    account_component = f"{7000000000000000 + sequence:016d}"
    return f"AE12{sequence % 900 + 100:03d}{account_component}"


def _eid(year: int, sequence: int, style: int = 0) -> str:
    """Create one Emirates-ID-shaped value with formatting variation."""
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


def _month_add(value: date, months: int) -> date:
    """Advance by whole months without external dependencies."""
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    day = min(value.day, 28)
    return date(year, month, day)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        for chunk in iter(
            lambda: file_handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def _identity_row(
    *,
    entity_type: str,
    customer_id: str,
    emirates_id_number: str,
    customer_segment: str,
    customer_created_date: str,
    individual_id: str = "",
) -> dict[str, object]:
    business_id = (
        customer_id
        if entity_type == "SME"
        else ""
    )

    return {
        "entity_type": entity_type,
        "entity_id": customer_id,
        "customer_id": customer_id,
        "business_id": business_id,
        "individual_id": individual_id,
        "emirates_id_number": emirates_id_number,
        "customer_segment": customer_segment,
        "customer_status": "ACTIVE",
        "customer_created_date": customer_created_date,
    }


def _account(
    *,
    customer_id: str,
    entity_type: str,
    sequence: int,
    opened_date: str,
) -> AccountRecord:
    account_number = f"{sequence:012d}"

    return AccountRecord(
        account_id=f"ACC-{customer_id}-001",
        customer_id=customer_id,
        entity_type=entity_type,
        account_number=account_number,
        iban=_uae_iban(
            "033",
            account_number,
        ),
        account_currency="AED",
        account_status="ACTIVE",
        account_opened_date=opened_date,
    )


def _beneficiary_row(
    *,
    owner_customer_id: str,
    owner_created_date: str,
    owner_entity_type: str,
    beneficiary_id: str,
    beneficiary_account_number: str,
    account_holder_name: str,
    created_at: datetime,
    nick_name: str,
    bank_name: str = "Emirates Commercial Bank",
) -> dict[str, object]:
    row = {
        "beneficiary_account_number": (
            beneficiary_account_number
        ),
        "source": "payment",
        "beneficiary_id": beneficiary_id,
        "customer_type": owner_entity_type,
        "beneficiary_type": "local",
        "customer_creation_date": owner_created_date,
        "beneficiary_account_holder_name": (
            account_holder_name
        ),
        "nick_name": nick_name,
        "is_active": True,
        "country_of_beneficiary": "AE",
        "currency": "AED",
        "bank_name": bank_name,
        "swift_code": "",
        "beneficiary_created_date": _timestamp(
            created_at
        ),
        "beneficiary_updated_date": _timestamp(
            created_at + timedelta(hours=1)
        ),
        "legal_type": (
            "BUSINESS"
            if owner_entity_type == "SME"
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

    if owner_entity_type == "SME":
        row["business_id"] = owner_customer_id

    else:
        row["customer_id"] = owner_customer_id

    return row


class Scenario1Builder:
    """Build all source-shaped rows for Scenario 1."""

    def __init__(self) -> None:
        self.random = random.Random(
            GENERATION_SEED
        )

        self.identities: list[
            dict[str, object]
        ] = []

        self.accounts: dict[
            str,
            AccountRecord,
        ] = {}

        self.local_inwards: list[
            dict[str, object]
        ] = []

        self.local_outwards: list[
            dict[str, object]
        ] = []

        self.retail_beneficiaries: list[
            dict[str, object]
        ] = []

        self.sme_beneficiaries: list[
            dict[str, object]
        ] = []

        self.customer_created_dates: dict[
            str,
            str,
        ] = {}

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
        self.identities.append(
            _identity_row(
                entity_type=entity_type,
                customer_id=customer_id,
                emirates_id_number=(
                    emirates_id_number
                ),
                customer_segment=segment,
                customer_created_date=(
                    customer_created_date
                ),
                individual_id=individual_id,
            )
        )

        self.accounts[customer_id] = _account(
            customer_id=customer_id,
            entity_type=entity_type,
            sequence=account_sequence,
            opened_date=customer_created_date,
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

        row = _beneficiary_row(
            owner_customer_id=owner_customer_id,
            owner_created_date=(
                self.customer_created_dates[
                    owner_customer_id
                ]
            ),
            owner_entity_type=entity_type,
            beneficiary_id=beneficiary_id,
            beneficiary_account_number=(
                account_number
            ),
            account_holder_name=(
                account_holder_name
            ),
            created_at=created_at,
            nick_name=nick_name,
            bank_name=bank_name,
        )

        if entity_type == "SME":
            self.sme_beneficiaries.append(
                row
            )

        else:
            self.retail_beneficiaries.append(
                row
            )

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
        self._transfer_sequence += 1

        account = self.accounts[
            customer_id
        ]

        transfer_id = (
            f"LOC-OUT-"
            f"{self._transfer_sequence:07d}"
        )

        self.local_outwards.append(
            {
                "transfer_id": transfer_id,
                "quote_id": (
                    f"QUOTE-{transfer_id}"
                ),
                "status": "COMPLETED",
                "reference_number": (
                    f"REF-{transfer_id}"
                ),
                "source_account_id": (
                    account.account_id
                ),
                "direction": "OUTWARD",
                "customer_id": customer_id,
                "transaction_timestamp": (
                    _timestamp(timestamp)
                ),
                "beneficiary_id": beneficiary_id,
                "beneficiary_account_number": (
                    beneficiary_account_number
                ),
                "source_amount": _amount(amount),
                "target_amount": _amount(amount),
                "payment_purpose_key": (
                    purpose_key
                ),
                "payment_purpose_name": (
                    purpose_name
                ),
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
        self._transfer_sequence += 1
        self._external_source_sequence += 1

        account = self.accounts[
            customer_id
        ]

        resolved_source_iban = (
            source_iban
            or _external_iban(
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
            f"LOC-IN-"
            f"{self._transfer_sequence:07d}"
        )

        self.local_inwards.append(
            {
                "transfer_id": transfer_id,
                "quote_id": (
                    f"QUOTE-{transfer_id}"
                ),
                "status": "COMPLETED",
                "reference_number": (
                    f"REF-{transfer_id}"
                ),
                "source_account_id": (
                    resolved_source_account_id
                ),
                "direction": "INWARD",
                "customer_id": customer_id,
                "transaction_timestamp": (
                    _timestamp(timestamp)
                ),
                "beneficiary_id": "",
                "beneficiary_account_number": (
                    account.account_number
                ),
                "beneficiary_iban": account.iban,
                "source_amount": _amount(amount),
                "target_amount": _amount(amount),
                "payment_purpose_key": (
                    purpose_key
                ),
                "payment_purpose_name": (
                    purpose_name
                ),
                "service_type": "FTS",
                "source_iban": (
                    resolved_source_iban
                ),
                "total_fees": "0.00",
                "fee_details": "[]",
            }
        )

    def build_core_customers(self) -> None:
        shared_eid_digits = (
            "784198810000013"
        )

        core_definitions = [
            (
                "R1001",
                "RETAIL",
                (
                    f"{shared_eid_digits[:3]}-"
                    f"{shared_eid_digits[3:7]}-"
                    f"{shared_eid_digits[7:14]}-"
                    f"{shared_eid_digits[14:]}"
                ),
                "RETAIL",
                "2024-03-15",
                100000000001,
                "",
            ),
            (
                "B2001",
                "SME",
                (
                    f"{shared_eid_digits[:3]} "
                    f"{shared_eid_digits[3:7]} "
                    f"{shared_eid_digits[7:14]} "
                    f"{shared_eid_digits[14:]}"
                ),
                "SME",
                "2023-06-10",
                200000000001,
                "IND-B2001-OWNER",
            ),
            (
                "R1002",
                "RETAIL",
                _eid(1997, 1002, 0),
                "RETAIL",
                "2026-06-20",
                100000000002,
                "",
            ),
            (
                "R1003",
                "RETAIL",
                _eid(1991, 1003, 1),
                "RETAIL",
                "2023-01-12",
                100000000003,
                "",
            ),
            (
                "B2002",
                "SME",
                _eid(1984, 2002, 2),
                "SME",
                "2024-01-05",
                200000000002,
                "IND-B2002-OWNER",
            ),
            (
                "R1004",
                "RETAIL",
                _eid(2000, 1004, 0),
                "RETAIL",
                "2026-07-01",
                100000000004,
                "",
            ),
            (
                "R1005",
                "RETAIL",
                _eid(1995, 1005, 1),
                "RETAIL",
                "2026-06-25",
                100000000005,
                "",
            ),
            (
                "R1006",
                "RETAIL",
                _eid(1989, 1006, 2),
                "RETAIL",
                "2022-09-10",
                100000000006,
                "",
            ),
            (
                "R1007",
                "RETAIL",
                _eid(1999, 1007, 0),
                "RETAIL",
                "2026-07-05",
                100000000007,
                "",
            ),
        ]

        for definition in core_definitions:
            self.add_customer(
                customer_id=definition[0],
                entity_type=definition[1],
                emirates_id_number=definition[2],
                segment=definition[3],
                customer_created_date=definition[4],
                account_sequence=definition[5],
                individual_id=definition[6],
            )

    def build_background_population(self) -> None:
        for index in range(78):
            customer_id = f"R8{index + 1:03d}"
            opened_date = date(
                2021 + index % 4,
                index % 12 + 1,
                index % 24 + 1,
            )

            self.add_customer(
                customer_id=customer_id,
                entity_type="RETAIL",
                emirates_id_number=_eid(
                    1978 + index % 25,
                    8000 + index,
                    index % 3,
                ),
                segment="RETAIL",
                customer_created_date=(
                    _date(opened_date)
                ),
                account_sequence=(
                    800000000000 + index + 1
                ),
            )

        for index in range(18):
            customer_id = f"B9{index + 1:03d}"
            opened_date = date(
                2020 + index % 5,
                index % 12 + 1,
                index % 24 + 1,
            )

            self.add_customer(
                customer_id=customer_id,
                entity_type="SME",
                emirates_id_number=_eid(
                    1970 + index % 20,
                    9000 + index,
                    index % 3,
                ),
                segment="SME",
                customer_created_date=(
                    _date(opened_date)
                ),
                account_sequence=(
                    900000000000 + index + 1
                ),
                individual_id=(
                    f"IND-{customer_id}-OWNER"
                ),
            )

    def build_seed_pool(
        self,
    ) -> list[dict[str, object]]:
        seed_account = self.accounts["R1001"]

        return [
            {
                "snapshot_date": "2026-07-20",
                "seed_event_id": (
                    "FRC|LOCAL|202607150001"
                ),
                "seed_customer_id": "R1001",
                "seed_account_id": (
                    seed_account.account_id
                ),
                "seed_account_number": (
                    seed_account.account_number
                ),
                "seed_iban": seed_account.iban,
                "seed_entity_type": "RETAIL",
                "date_reported": "2026-07-15",
                "seed_source": "FRC",
                "source_event_type": (
                    "FTS_REFUND_REQUEST"
                ),
                "source_transaction_reference": (
                    "LOC-IN-REPORTED-202607150001"
                ),
            }
        ]

    def build_first_risk_branch(self) -> None:
        seed_beneficiary = self.add_beneficiary(
            owner_customer_id="R1001",
            account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_1_NAME
            ),
            created_at=datetime(
                2026,
                7,
                9,
                14,
                0,
            ),
            nick_name="Mosaic Trading",
        )

        seed_payments = [
            (datetime(2026, 7, 10, 10, 5), 9000.0),
            (datetime(2026, 7, 11, 11, 20), 11000.0),
            (datetime(2026, 7, 12, 9, 10), 8000.0),
            (datetime(2026, 7, 12, 15, 35), 12000.0),
            (datetime(2026, 7, 13, 12, 15), 9500.0),
        ]

        for timestamp, amount in seed_payments:
            self.add_outward(
                customer_id="R1001",
                timestamp=timestamp,
                beneficiary_id=seed_beneficiary,
                beneficiary_account_number=(
                    RISK_COUNTERPARTY_1_ACCOUNT
                ),
                amount=amount,
                purpose_key="SUPPLIER_PAYMENT",
                purpose_name="Supplier Payment",
            )

        r1002_beneficiary = self.add_beneficiary(
            owner_customer_id="R1002",
            account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_1_NAME
            ),
            created_at=datetime(
                2026,
                7,
                6,
                9,
                0,
            ),
            nick_name="Mosaic",
        )

        inward_times = [
            datetime(2026, 7, 8, 8, 10),
            datetime(2026, 7, 8, 12, 25),
            datetime(2026, 7, 9, 9, 5),
            datetime(2026, 7, 9, 14, 40),
            datetime(2026, 7, 10, 8, 35),
            datetime(2026, 7, 10, 13, 15),
            datetime(2026, 7, 11, 9, 20),
            datetime(2026, 7, 11, 15, 5),
            datetime(2026, 7, 12, 8, 50),
            datetime(2026, 7, 12, 12, 10),
            datetime(2026, 7, 13, 9, 30),
        ]

        inward_amounts = [
            8000.0,
            6500.0,
            9200.0,
            7400.0,
            10500.0,
            6800.0,
            8900.0,
            7200.0,
            9600.0,
            7800.0,
            8100.0,
        ]

        for timestamp, amount in zip(
            inward_times,
            inward_amounts,
            strict=True,
        ):
            self.add_inward(
                customer_id="R1002",
                timestamp=timestamp,
                amount=amount,
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )

        r1002_outward_amounts = [
            11000.0,
            10500.0,
            12000.0,
            9500.0,
            11500.0,
            10000.0,
            9800.0,
        ]

        for index, amount in enumerate(
            r1002_outward_amounts
        ):
            timestamp = inward_times[
                min(index + 2, len(inward_times) - 1)
            ] + timedelta(
                minutes=35 + index * 7
            )

            self.add_outward(
                customer_id="R1002",
                timestamp=timestamp,
                beneficiary_id=r1002_beneficiary,
                beneficiary_account_number=(
                    RISK_COUNTERPARTY_1_ACCOUNT
                ),
                amount=amount,
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )

        for index in range(4):
            additional_account = (
                f"7701000000{index + 1:02d}"
            )

            additional_beneficiary = (
                self.add_beneficiary(
                    owner_customer_id="R1002",
                    account_number=(
                        additional_account
                    ),
                    account_holder_name=(
                        f"Personal Contact {index + 1}"
                    ),
                    created_at=datetime(
                        2026,
                        7,
                        7 + index,
                        10,
                        0,
                    ),
                    nick_name=(
                        f"Contact {index + 1}"
                    ),
                )
            )

            self.add_outward(
                customer_id="R1002",
                timestamp=datetime(
                    2026,
                    7,
                    10 + index,
                    17,
                    15,
                ),
                beneficiary_id=(
                    additional_beneficiary
                ),
                beneficiary_account_number=(
                    additional_account
                ),
                amount=2200.0 + index * 300,
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )

        salary_iban = _external_iban(500)

        for index in range(12):
            salary_date = _month_add(
                date(2025, 8, 28),
                index,
            )

            self.add_inward(
                customer_id="R1003",
                timestamp=datetime.combine(
                    salary_date,
                    datetime.min.time(),
                ).replace(
                    hour=8,
                    minute=30,
                ),
                amount=18500.0,
                purpose_key="SALARY",
                purpose_name="Salary",
                source_iban=salary_iban,
                source_account_id=(
                    "EMPLOYER-PAYROLL-001"
                ),
            )

        r1003_beneficiary = self.add_beneficiary(
            owner_customer_id="R1003",
            account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_1_NAME
            ),
            created_at=datetime(
                2026,
                7,
                11,
                16,
                30,
            ),
            nick_name="Mosaic",
        )

        self.add_outward(
            customer_id="R1003",
            timestamp=datetime(
                2026,
                7,
                12,
                11,
                15,
            ),
            beneficiary_id=r1003_beneficiary,
            beneficiary_account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            amount=18000.0,
            purpose_key="PERSONAL_TRANSFER",
            purpose_name="Personal Transfer",
        )

        b2002_beneficiary = self.add_beneficiary(
            owner_customer_id="B2002",
            account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_1_NAME
            ),
            created_at=datetime(
                2025,
                9,
                15,
                10,
                0,
            ),
            nick_name="Mosaic Supplier",
        )

        for index, amount in enumerate(
            [
                5200.0,
                6100.0,
                4800.0,
                7200.0,
                5600.0,
                6800.0,
            ]
        ):
            payment_date = _month_add(
                date(2025, 10, 10),
                index * 2,
            )

            self.add_outward(
                customer_id="B2002",
                timestamp=datetime.combine(
                    payment_date,
                    datetime.min.time(),
                ).replace(
                    hour=10,
                    minute=15,
                ),
                beneficiary_id=(
                    b2002_beneficiary
                ),
                beneficiary_account_number=(
                    RISK_COUNTERPARTY_1_ACCOUNT
                ),
                amount=amount,
                purpose_key="SUPPLIER_PAYMENT",
                purpose_name=(
                    f"Invoice Settlement INV-{index + 1:03d}"
                ),
            )

        for index in range(18):
            received_date = date(
                2025,
                10,
                5,
            ) + timedelta(
                days=index * 15
            )

            self.add_inward(
                customer_id="B2002",
                timestamp=datetime.combine(
                    received_date,
                    datetime.min.time(),
                ).replace(
                    hour=9,
                    minute=20,
                ),
                amount=(
                    18000.0
                    + (index % 5) * 2400
                ),
                purpose_key="BUSINESS_RECEIPT",
                purpose_name="Customer Invoice Receipt",
            )

        r1004_beneficiary = self.add_beneficiary(
            owner_customer_id="R1004",
            account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_1_NAME
            ),
            created_at=datetime(
                2026,
                7,
                12,
                18,
                0,
            ),
            nick_name="Mosaic",
        )

        self.add_inward(
            customer_id="R1004",
            timestamp=datetime(
                2026,
                7,
                10,
                12,
                0,
            ),
            amount=3000.0,
            purpose_key="ACCOUNT_FUNDING",
            purpose_name="Account Funding",
        )

        self.add_outward(
            customer_id="R1004",
            timestamp=datetime(
                2026,
                7,
                13,
                11,
                0,
            ),
            beneficiary_id=r1004_beneficiary,
            beneficiary_account_number=(
                RISK_COUNTERPARTY_1_ACCOUNT
            ),
            amount=2000.0,
            purpose_key="PERSONAL_TRANSFER",
            purpose_name="Personal Transfer",
        )

    def build_legitimate_branch(self) -> None:
        participating_customers = [
            "R1001",
            "B2001",
            "R1003",
            *[
                customer_id
                for customer_id
                in self.accounts
                if customer_id.startswith(
                    ("R8", "B9")
                )
            ],
        ]

        for customer_index, customer_id in enumerate(
            participating_customers
        ):
            account = self.accounts[
                customer_id
            ]

            first_payment_date = date(
                2025,
                11,
                5,
            ) + timedelta(
                days=customer_index % 24
            )

            beneficiary_created_at = (
                datetime.combine(
                    first_payment_date,
                    datetime.min.time(),
                )
                - timedelta(
                    days=(
                        20
                        + customer_index % 120
                    )
                )
            ).replace(
                hour=9,
                minute=15,
            )

            beneficiary_id = self.add_beneficiary(
                owner_customer_id=customer_id,
                account_number=(
                    LEGITIMATE_COUNTERPARTY_ACCOUNT
                ),
                account_holder_name=(
                    LEGITIMATE_COUNTERPARTY_NAME
                ),
                created_at=(
                    beneficiary_created_at
                ),
                nick_name="Horizon Services",
                bank_name="National Commercial Bank",
            )

            payment_count = (
                2
                + customer_index % 4
            )

            for payment_index in range(
                payment_count
            ):
                payment_date = _month_add(
                    first_payment_date,
                    payment_index * 2,
                )

                if payment_date > date(
                    2026,
                    7,
                    14,
                ):
                    break

                if account.entity_type == "SME":
                    amount = (
                        1250.0
                        + (customer_index % 7)
                        * 285.0
                        + payment_index * 45.0
                    )

                    purpose_key = (
                        "BUSINESS_SERVICES"
                    )

                    purpose_name = (
                        "Facilities Service Invoice"
                    )

                else:
                    amount = (
                        175.0
                        + (customer_index % 9)
                        * 32.5
                        + payment_index * 7.5
                    )

                    purpose_key = "SERVICE_PAYMENT"
                    purpose_name = (
                        "Monthly Service Payment"
                    )

                self.add_outward(
                    customer_id=customer_id,
                    timestamp=datetime.combine(
                        payment_date,
                        datetime.min.time(),
                    ).replace(
                        hour=(
                            8
                            + customer_index % 9
                        ),
                        minute=(
                            customer_index * 7
                        ) % 60,
                    ),
                    beneficiary_id=beneficiary_id,
                    beneficiary_account_number=(
                        LEGITIMATE_COUNTERPARTY_ACCOUNT
                    ),
                    amount=amount,
                    purpose_key=purpose_key,
                    purpose_name=purpose_name,
                )

            if customer_id.startswith(
                ("R8", "B9")
            ):
                inward_count = (
                    1
                    + customer_index % 2
                )

                for inward_index in range(
                    inward_count
                ):
                    if account.entity_type == "SME":
                        inward_amount = (
                            14000.0
                            + customer_index * 85
                            + inward_index * 2200
                        )

                        purpose_key = (
                            "BUSINESS_RECEIPT"
                        )

                        purpose_name = (
                            "Customer Invoice Receipt"
                        )

                    else:
                        inward_amount = (
                            6500.0
                            + (customer_index % 11)
                            * 350
                        )

                        purpose_key = "SALARY"
                        purpose_name = "Salary"

                    self.add_inward(
                        customer_id=customer_id,
                        timestamp=datetime(
                            2026,
                            5 + inward_index,
                            25,
                            8,
                            15,
                        ),
                        amount=inward_amount,
                        purpose_key=purpose_key,
                        purpose_name=purpose_name,
                    )

    def build_second_risk_branch(self) -> None:
        r1002_beneficiary = self.add_beneficiary(
            owner_customer_id="R1002",
            account_number=(
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_2_NAME
            ),
            created_at=datetime(
                2026,
                7,
                13,
                8,
                30,
            ),
            nick_name="Orion",
        )

        for index, amount in enumerate(
            [5200.0, 6800.0, 5900.0, 7300.0]
        ):
            self.add_outward(
                customer_id="R1002",
                timestamp=datetime(
                    2026,
                    7,
                    14 + index // 2,
                    10 + index * 2,
                    10,
                ),
                beneficiary_id=r1002_beneficiary,
                beneficiary_account_number=(
                    RISK_COUNTERPARTY_2_ACCOUNT
                ),
                amount=amount,
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )

        r1005_inward_times = [
            datetime(2026, 7, 15, 8, 5),
            datetime(2026, 7, 15, 11, 10),
            datetime(2026, 7, 16, 8, 40),
            datetime(2026, 7, 16, 13, 20),
            datetime(2026, 7, 17, 9, 15),
            datetime(2026, 7, 17, 15, 5),
            datetime(2026, 7, 18, 8, 55),
            datetime(2026, 7, 18, 12, 30),
        ]

        for index, timestamp in enumerate(
            r1005_inward_times
        ):
            self.add_inward(
                customer_id="R1005",
                timestamp=timestamp,
                amount=(
                    6200.0
                    + index * 450.0
                ),
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )

        r1005_beneficiary = self.add_beneficiary(
            owner_customer_id="R1005",
            account_number=(
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_2_NAME
            ),
            created_at=datetime(
                2026,
                7,
                14,
                16,
                0,
            ),
            nick_name="Orion",
        )

        for index, timestamp in enumerate(
            r1005_inward_times[:5]
        ):
            self.add_outward(
                customer_id="R1005",
                timestamp=timestamp + timedelta(
                    minutes=45 + index * 5
                ),
                beneficiary_id=r1005_beneficiary,
                beneficiary_account_number=(
                    RISK_COUNTERPARTY_2_ACCOUNT
                ),
                amount=(
                    5700.0
                    + index * 400.0
                ),
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )

        salary_iban = _external_iban(600)

        for index in range(12):
            salary_date = _month_add(
                date(2025, 8, 27),
                index,
            )

            self.add_inward(
                customer_id="R1006",
                timestamp=datetime.combine(
                    salary_date,
                    datetime.min.time(),
                ).replace(
                    hour=8,
                    minute=15,
                ),
                amount=16200.0,
                purpose_key="SALARY",
                purpose_name="Salary",
                source_iban=salary_iban,
                source_account_id=(
                    "EMPLOYER-PAYROLL-002"
                ),
            )

        r1006_beneficiary = self.add_beneficiary(
            owner_customer_id="R1006",
            account_number=(
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_2_NAME
            ),
            created_at=datetime(
                2026,
                7,
                17,
                18,
                0,
            ),
            nick_name="Orion",
        )

        self.add_outward(
            customer_id="R1006",
            timestamp=datetime(
                2026,
                7,
                18,
                10,
                30,
            ),
            beneficiary_id=r1006_beneficiary,
            beneficiary_account_number=(
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
            amount=12000.0,
            purpose_key="PERSONAL_TRANSFER",
            purpose_name="Personal Transfer",
        )

        self.add_inward(
            customer_id="R1007",
            timestamp=datetime(
                2026,
                7,
                17,
                11,
                0,
            ),
            amount=1800.0,
            purpose_key="ACCOUNT_FUNDING",
            purpose_name="Account Funding",
        )

        r1007_beneficiary = self.add_beneficiary(
            owner_customer_id="R1007",
            account_number=(
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
            account_holder_name=(
                RISK_COUNTERPARTY_2_NAME
            ),
            created_at=datetime(
                2026,
                7,
                18,
                13,
                0,
            ),
            nick_name="Orion",
        )

        self.add_outward(
            customer_id="R1007",
            timestamp=datetime(
                2026,
                7,
                19,
                10,
                10,
            ),
            beneficiary_id=r1007_beneficiary,
            beneficiary_account_number=(
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
            amount=1200.0,
            purpose_key="PERSONAL_TRANSFER",
            purpose_name="Personal Transfer",
        )

    def build(
        self,
    ) -> dict[str, pd.DataFrame]:
        self.build_core_customers()
        self.build_background_population()
        seed_rows = self.build_seed_pool()
        self.build_first_risk_branch()
        self.build_legitimate_branch()
        self.build_second_risk_branch()

        account_rows = [
            account.__dict__
            for account in self.accounts.values()
        ]

        return {
            SEED_MULE_POOL_CONTRACT.filename: (
                pd.DataFrame(
                    seed_rows,
                    columns=list(
                        SEED_MULE_POOL_CONTRACT.columns
                    ),
                )
            ),
            CUSTOMER_IDENTITY_CONTRACT.filename: (
                pd.DataFrame(
                    self.identities,
                    columns=list(
                        CUSTOMER_IDENTITY_CONTRACT.columns
                    ),
                )
            ),
            CUSTOMER_ACCOUNT_MASTER_CONTRACT.filename: (
                pd.DataFrame(
                    account_rows,
                    columns=list(
                        CUSTOMER_ACCOUNT_MASTER_CONTRACT.columns
                    ),
                )
            ),
            LOCAL_INWARD_PAYMENTS_CONTRACT.filename: (
                pd.DataFrame(
                    self.local_inwards,
                    columns=list(
                        LOCAL_INWARD_PAYMENTS_CONTRACT.columns
                    ),
                )
            ),
            LOCAL_OUTWARD_PAYMENTS_CONTRACT.filename: (
                pd.DataFrame(
                    self.local_outwards,
                    columns=list(
                        LOCAL_OUTWARD_PAYMENTS_CONTRACT.columns
                    ),
                )
            ),
            RETAIL_BENEFICIARY_MASTER_CONTRACT.filename: (
                pd.DataFrame(
                    self.retail_beneficiaries,
                    columns=list(
                        RETAIL_BENEFICIARY_MASTER_CONTRACT.columns
                    ),
                )
            ),
            SME_BENEFICIARY_MASTER_CONTRACT.filename: (
                pd.DataFrame(
                    self.sme_beneficiaries,
                    columns=list(
                        SME_BENEFICIARY_MASTER_CONTRACT.columns
                    ),
                )
            ),
        }


def generate_scenario_1_source_data(
    output_directory: Path | str,
) -> dict[str, object]:
    """Generate deterministic source CSVs and a source-only manifest."""
    resolved_output_directory = Path(
        output_directory
    )

    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    frames = Scenario1Builder().build()

    for contract in SCENARIO_1_SOURCE_CONTRACTS:
        frame = frames[contract.filename]

        frame.to_csv(
            resolved_output_directory
            / contract.filename,
            index=False,
        )

    file_hashes = {
        filename: _hash_file(
            resolved_output_directory
            / filename
        )
        for filename in sorted(frames)
    }

    row_counts = {
        filename: len(frame)
        for filename, frame in sorted(
            frames.items()
        )
    }

    manifest = {
        "scenario_name": SCENARIO_NAME,
        "generation_seed": GENERATION_SEED,
        "run_date": str(RUN_DATE),
        "source_only": True,
        "contains_prebuilt_groups": False,
        "contains_prebuilt_nodes": False,
        "contains_prebuilt_edges": False,
        "contains_ai_decisions": False,
        "source_files": sorted(frames),
        "row_counts": row_counts,
        "sha256": file_hashes,
        "design_reference": {
            "seed_customer_id": "R1001",
            "eid_linked_entity_id": "B2001",
            "first_layer_counterparty_accounts": [
                RISK_COUNTERPARTY_1_ACCOUNT,
                LEGITIMATE_COUNTERPARTY_ACCOUNT,
            ],
            "second_layer_counterparty_account": (
                RISK_COUNTERPARTY_2_ACCOUNT
            ),
        },
    }

    manifest_path = (
        resolved_output_directory
        / "source_manifest.json"
    )

    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return manifest
