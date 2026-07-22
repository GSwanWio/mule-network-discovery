"""Generate production-shaped synthetic source data for Scenario 2."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from network_mule_discovery.synthetic_scenario_builder import (
    SyntheticScenarioBuilder,
    add_months,
    format_date,
    hash_file,
    synthetic_eid,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_2_SOURCE_CONTRACTS,
)


GENERATION_SEED = 20260722
SCENARIO_NAME = "scenario_2_common_public_high_degree_hub"
RUN_DATE = date(2026, 7, 20)

SEED_CUSTOMER_ID = "R2001"
COMMON_PUBLIC_COUNTERPARTY_ACCOUNT = "770200000001"
COMMON_PUBLIC_COUNTERPARTY_NAME = (
    "National Utility Services PJSC"
)

NON_SEED_RETAIL_COUNT = 450
NON_SEED_SME_COUNT = 50
NON_SEED_CUSTOMER_COUNT = (
    NON_SEED_RETAIL_COUNT
    + NON_SEED_SME_COUNT
)
TOTAL_CUSTOMER_COUNT = NON_SEED_CUSTOMER_COUNT + 1
PAYMENTS_PER_CUSTOMER = 5
TOTAL_RECURRING_PAYMENT_COUNT = (
    TOTAL_CUSTOMER_COUNT
    * PAYMENTS_PER_CUSTOMER
)


class Scenario2Builder:
    """Build a broad common/public counterparty source pack."""

    def __init__(self) -> None:
        self.builder = SyntheticScenarioBuilder(
            GENERATION_SEED
        )
        self.customer_ids: list[str] = []

    def _add_customer(
        self,
        *,
        customer_id: str,
        entity_type: str,
        sequence: int,
    ) -> None:
        created_date = date(2021, 1, 1) + timedelta(
            days=(sequence * 17) % 1250
        )

        self.builder.add_customer(
            customer_id=customer_id,
            entity_type=entity_type,
            emirates_id_number=synthetic_eid(
                1980 + sequence % 25,
                2000000 + sequence,
                style=sequence % 3,
            ),
            segment=(
                "SME"
                if entity_type == "SME"
                else "RETAIL"
            ),
            customer_created_date=format_date(
                created_date
            ),
            account_sequence=220000000000 + sequence,
            individual_id=(
                f"IND-S2-{sequence:05d}"
                if entity_type == "SME"
                else ""
            ),
        )

        self.customer_ids.append(customer_id)

    def build_population(self) -> None:
        """Create the seed and 500 non-seed customers."""
        self._add_customer(
            customer_id=SEED_CUSTOMER_ID,
            entity_type="RETAIL",
            sequence=1,
        )

        for offset in range(
            1,
            NON_SEED_RETAIL_COUNT + 1,
        ):
            self._add_customer(
                customer_id=f"R{3000 + offset}",
                entity_type="RETAIL",
                sequence=offset + 1,
            )

        for offset in range(
            1,
            NON_SEED_SME_COUNT + 1,
        ):
            self._add_customer(
                customer_id=f"B{4000 + offset}",
                entity_type="SME",
                sequence=(
                    NON_SEED_RETAIL_COUNT
                    + offset
                    + 1
                ),
            )

    def build_seed_pool(
        self,
    ) -> list[dict[str, object]]:
        """Create one confirmed seed event."""
        account = self.builder.accounts[
            SEED_CUSTOMER_ID
        ]

        return [
            {
                "snapshot_date": str(RUN_DATE),
                "seed_event_id": "S2-SEED-0001",
                "seed_customer_id": SEED_CUSTOMER_ID,
                "seed_account_id": account.account_id,
                "seed_account_number": (
                    account.account_number
                ),
                "seed_iban": account.iban,
                "seed_entity_type": "RETAIL",
                "date_reported": "2026-07-01",
                "seed_source": "FRC_CONFIRMED_MULE",
                "source_event_type": "SWIFT_RECALL",
                "source_transaction_reference": (
                    "S2-FRC-TXN-0001"
                ),
            }
        ]

    def build_common_public_branch(self) -> None:
        """Create recurring, low-concentration service payments."""
        first_payment_month = date(2026, 1, 1)

        for customer_index, customer_id in enumerate(
            self.customer_ids,
            start=1,
        ):
            beneficiary_created_at = datetime(
                2023,
                1,
                1,
                9,
                0,
            ) + timedelta(
                days=(customer_index * 11) % 480,
                minutes=customer_index % 60,
            )

            beneficiary_id = self.builder.add_beneficiary(
                owner_customer_id=customer_id,
                account_number=(
                    COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
                ),
                account_holder_name=(
                    COMMON_PUBLIC_COUNTERPARTY_NAME
                ),
                created_at=beneficiary_created_at,
                nick_name="Utility Services",
                bank_name="National Clearing Bank",
            )

            entity_type = self.builder.accounts[
                customer_id
            ].entity_type

            for payment_index in range(
                PAYMENTS_PER_CUSTOMER
            ):
                payment_month = add_months(
                    first_payment_month,
                    payment_index,
                )

                day = 3 + (
                    customer_index * 7
                    + payment_index * 3
                ) % 22

                outward_timestamp = datetime(
                    payment_month.year,
                    payment_month.month,
                    day,
                    8 + customer_index % 11,
                    customer_index % 60,
                )

                inward_timestamp = (
                    outward_timestamp
                    - timedelta(days=7)
                )

                outward_amount = (
                    350.0
                    + (
                        customer_index * 19
                        + payment_index * 13
                    )
                    % 301
                    if entity_type == "SME"
                    else 85.0
                    + (
                        customer_index * 17
                        + payment_index * 11
                    )
                    % 266
                )

                inward_amount = (
                    25000.0
                    + (
                        customer_index * 137
                        + payment_index * 71
                    )
                    % 35001
                    if entity_type == "SME"
                    else 6500.0
                    + (
                        customer_index * 83
                        + payment_index * 47
                    )
                    % 5501
                )

                self.builder.add_inward(
                    customer_id=customer_id,
                    timestamp=inward_timestamp,
                    amount=inward_amount,
                    purpose_key=(
                        "BUSINESS_INCOME"
                        if entity_type == "SME"
                        else "SALARY"
                    ),
                    purpose_name=(
                        "Business Income"
                        if entity_type == "SME"
                        else "Salary"
                    ),
                )

                self.builder.add_outward(
                    customer_id=customer_id,
                    timestamp=outward_timestamp,
                    beneficiary_id=beneficiary_id,
                    beneficiary_account_number=(
                        COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
                    ),
                    amount=outward_amount,
                    purpose_key="UTILITY_PAYMENT",
                    purpose_name="Utility Bill Payment",
                )

    def build(
        self,
    ) -> dict[str, pd.DataFrame]:
        """Build all Scenario 2 source frames."""
        self.build_population()
        seed_rows = self.build_seed_pool()
        self.build_common_public_branch()
        return self.builder.build_frames(seed_rows)


def generate_scenario_2_source_data(
    output_directory: Path | str,
) -> dict[str, object]:
    """Generate deterministic Scenario 2 source CSVs."""
    resolved_output_directory = Path(
        output_directory
    )

    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    frames = Scenario2Builder().build()

    for contract in SCENARIO_2_SOURCE_CONTRACTS:
        frames[contract.filename].to_csv(
            resolved_output_directory
            / contract.filename,
            index=False,
        )

    file_hashes = {
        filename: hash_file(
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
            "seed_customer_id": SEED_CUSTOMER_ID,
            "common_public_counterparty_account": (
                COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
            ),
            "common_public_counterparty_name": (
                COMMON_PUBLIC_COUNTERPARTY_NAME
            ),
            "non_seed_customer_count": (
                NON_SEED_CUSTOMER_COUNT
            ),
            "total_linked_customer_count": (
                TOTAL_CUSTOMER_COUNT
            ),
            "payments_per_customer": (
                PAYMENTS_PER_CUSTOMER
            ),
            "total_recurring_payment_count": (
                TOTAL_RECURRING_PAYMENT_COUNT
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
