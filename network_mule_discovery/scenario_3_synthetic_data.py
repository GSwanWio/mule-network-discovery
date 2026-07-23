"""Generate production-shaped synthetic source data for Scenario 3."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from network_mule_discovery.synthetic_scenario_builder import (
    SyntheticScenarioBuilder,
    format_date,
    hash_file,
    synthetic_eid,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_3_SOURCE_CONTRACTS,
)


GENERATION_SEED = 20260723
SCENARIO_NAME = "scenario_3_beneficiary_to_confirmed_mule"
RUN_DATE = date(2026, 7, 20)

SEED_CUSTOMER_ID = "R3001"
PAYMENT_BACKED_CUSTOMER_ID = "R3002"
ADD_ONLY_CUSTOMER_ID = "B3001"
PAYMENT_PAIR_COUNT = 8


class Scenario3Builder:
    """Build beneficiary-to-confirmed-mule source evidence."""

    def __init__(self) -> None:
        self.builder = SyntheticScenarioBuilder(
            GENERATION_SEED
        )

    def build_population(self) -> None:
        """Create one seed and two beneficiary-link candidates."""
        self.builder.add_customer(
            customer_id=SEED_CUSTOMER_ID,
            entity_type="RETAIL",
            emirates_id_number=synthetic_eid(
                1988,
                3000001,
                style=1,
            ),
            segment="RETAIL",
            customer_created_date=format_date(
                date(2024, 2, 1)
            ),
            account_sequence=330000000001,
        )

        self.builder.add_customer(
            customer_id=PAYMENT_BACKED_CUSTOMER_ID,
            entity_type="RETAIL",
            emirates_id_number=synthetic_eid(
                1994,
                3000002,
                style=2,
            ),
            segment="RETAIL",
            customer_created_date=format_date(
                date(2026, 6, 20)
            ),
            account_sequence=330000000002,
        )

        self.builder.add_customer(
            customer_id=ADD_ONLY_CUSTOMER_ID,
            entity_type="SME",
            emirates_id_number=synthetic_eid(
                1982,
                3000003,
            ),
            segment="SME",
            customer_created_date=format_date(
                date(2023, 1, 15)
            ),
            account_sequence=330000000003,
            individual_id="IND-S3-00001",
        )

    def build_seed_pool(
        self,
    ) -> list[dict[str, object]]:
        """Create one confirmed mule seed record."""
        account = self.builder.accounts[
            SEED_CUSTOMER_ID
        ]

        return [
            {
                "snapshot_date": str(RUN_DATE),
                "seed_event_id": "S3-SEED-0001",
                "seed_customer_id": SEED_CUSTOMER_ID,
                "seed_account_id": account.account_id,
                "seed_account_number": account.account_number,
                "seed_iban": account.iban,
                "seed_entity_type": "RETAIL",
                "date_reported": "2026-07-05",
                "seed_source": "FRC_CONFIRMED_MULE",
                "source_event_type": "FTS_REFUND_REQUEST",
                "source_transaction_reference": (
                    "S3-FRC-TXN-0001"
                ),
            }
        ]

    def build_beneficiary_links(self) -> None:
        """Create one payment-backed and one add-only link."""
        seed_account = self.builder.accounts[
            SEED_CUSTOMER_ID
        ]

        payment_beneficiary_id = (
            self.builder.add_beneficiary(
                owner_customer_id=(
                    PAYMENT_BACKED_CUSTOMER_ID
                ),
                account_number=seed_account.account_number,
                account_holder_name="Confirmed Mule R3001",
                created_at=datetime(
                    2026,
                    7,
                    10,
                    8,
                    30,
                ),
                nick_name="R3001",
            )
        )

        for sequence in range(PAYMENT_PAIR_COUNT):
            inward_timestamp = datetime(
                2026,
                7,
                11 + sequence,
                9,
                0,
            )
            outward_timestamp = (
                inward_timestamp
                + timedelta(minutes=35 + sequence)
            )
            inward_amount = 10000.0 + sequence * 750.0
            outward_amount = inward_amount * 0.9

            self.builder.add_inward(
                customer_id=PAYMENT_BACKED_CUSTOMER_ID,
                timestamp=inward_timestamp,
                amount=inward_amount,
                purpose_key="PERSONAL_TRANSFER",
                purpose_name="Personal Transfer",
            )
            self.builder.add_outward(
                customer_id=PAYMENT_BACKED_CUSTOMER_ID,
                timestamp=outward_timestamp,
                beneficiary_id=payment_beneficiary_id,
                beneficiary_account_number=(
                    seed_account.account_number
                ),
                amount=outward_amount,
                purpose_key="FAMILY_SUPPORT",
                purpose_name="Family Support",
            )

        self.builder.add_beneficiary(
            owner_customer_id=ADD_ONLY_CUSTOMER_ID,
            account_number=seed_account.account_number,
            account_holder_name="Confirmed Mule R3001",
            created_at=datetime(
                2026,
                7,
                18,
                14,
                15,
            ),
            nick_name="New Supplier",
        )

    def build(
        self,
    ) -> dict[str, pd.DataFrame]:
        """Build all Scenario 3 source frames."""
        self.build_population()
        seed_rows = self.build_seed_pool()
        self.build_beneficiary_links()
        return self.builder.build_frames(seed_rows)


def generate_scenario_3_source_data(
    output_directory: Path | str,
) -> dict[str, object]:
    """Generate deterministic Scenario 3 source CSVs."""
    resolved_output_directory = Path(
        output_directory
    )
    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    frames = Scenario3Builder().build()

    for contract in SCENARIO_3_SOURCE_CONTRACTS:
        frames[contract.filename].to_csv(
            resolved_output_directory
            / contract.filename,
            index=False,
        )

    file_hashes = {
        filename: hash_file(
            resolved_output_directory / filename
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
            "payment_backed_customer_id": (
                PAYMENT_BACKED_CUSTOMER_ID
            ),
            "add_only_customer_id": (
                ADD_ONLY_CUSTOMER_ID
            ),
            "payment_pair_count": PAYMENT_PAIR_COUNT,
            "beneficiary_to_seed_link_count": 2,
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
