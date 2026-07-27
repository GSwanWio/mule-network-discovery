"""Generate production-shaped synthetic source data for Scenario 4."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from network_mule_discovery.synthetic_scenario_builder import (
    SyntheticScenarioBuilder,
    format_date,
    hash_file,
    synthetic_eid,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_4_SOURCE_CONTRACTS,
)


GENERATION_SEED = 20260724
SCENARIO_NAME = "scenario_4_eid_only_groups"
RUN_DATE = date(2026, 7, 20)

GROUP_A_SEED_ID = "R4001"
GROUP_A_SME_IDS = ("B4001", "B4002")
GROUP_B_SEED_ID = "B4101"
GROUP_B_RETAIL_ID = "R4101"

GROUP_A_EID = synthetic_eid(1986, 4000001, style=1)
GROUP_B_EID = synthetic_eid(1979, 4100001, style=2)


class Scenario4Builder:
    """Build two seed-anchored Emirates-ID-only groups."""

    def __init__(self) -> None:
        self.builder = SyntheticScenarioBuilder(
            GENERATION_SEED
        )

    def build_population(self) -> None:
        """Create five entities across two shared-EID components."""
        self.builder.add_customer(
            customer_id=GROUP_A_SEED_ID,
            entity_type="RETAIL",
            emirates_id_number=GROUP_A_EID,
            segment="RETAIL",
            customer_created_date=format_date(
                date(2022, 4, 10)
            ),
            account_sequence=440000000001,
        )
        self.builder.add_customer(
            customer_id=GROUP_A_SME_IDS[0],
            entity_type="SME",
            emirates_id_number=GROUP_A_EID.replace("-", " "),
            segment="SME",
            customer_created_date=format_date(
                date(2021, 8, 15)
            ),
            account_sequence=440000000002,
            individual_id="IND-S4-A-0001",
        )
        self.builder.add_customer(
            customer_id=GROUP_A_SME_IDS[1],
            entity_type="SME",
            emirates_id_number=GROUP_A_EID.replace("-", ""),
            segment="SME",
            customer_created_date=format_date(
                date(2023, 2, 1)
            ),
            account_sequence=440000000003,
            individual_id="IND-S4-A-0002",
        )
        self.builder.add_customer(
            customer_id=GROUP_B_SEED_ID,
            entity_type="SME",
            emirates_id_number=GROUP_B_EID,
            segment="SME",
            customer_created_date=format_date(
                date(2020, 11, 20)
            ),
            account_sequence=441000000001,
            individual_id="IND-S4-B-0001",
        )
        self.builder.add_customer(
            customer_id=GROUP_B_RETAIL_ID,
            entity_type="RETAIL",
            emirates_id_number=GROUP_B_EID.replace(" ", "-"),
            segment="RETAIL",
            customer_created_date=format_date(
                date(2024, 5, 5)
            ),
            account_sequence=441000000002,
        )

    def build_seed_pool(
        self,
    ) -> list[dict[str, object]]:
        """Create one retail and one SME confirmed-mule seed."""
        seed_definitions = (
            (
                "S4-SEED-0001",
                GROUP_A_SEED_ID,
                "RETAIL",
                "S4-FRC-TXN-0001",
            ),
            (
                "S4-SEED-0002",
                GROUP_B_SEED_ID,
                "SME",
                "S4-FRC-TXN-0002",
            ),
        )
        rows: list[dict[str, object]] = []

        for (
            event_id,
            customer_id,
            entity_type,
            reference,
        ) in seed_definitions:
            account = self.builder.accounts[customer_id]
            rows.append(
                {
                    "snapshot_date": str(RUN_DATE),
                    "seed_event_id": event_id,
                    "seed_customer_id": customer_id,
                    "seed_account_id": account.account_id,
                    "seed_account_number": account.account_number,
                    "seed_iban": account.iban,
                    "seed_entity_type": entity_type,
                    "date_reported": "2026-07-10",
                    "seed_source": "FRC_CONFIRMED_MULE",
                    "source_event_type": "FRC_CONFIRMED",
                    "source_transaction_reference": reference,
                }
            )

        return rows

    def build(
        self,
    ) -> dict[str, pd.DataFrame]:
        """Build all Scenario 4 source frames."""
        self.build_population()
        return self.builder.build_frames(
            self.build_seed_pool()
        )


def generate_scenario_4_source_data(
    output_directory: Path | str,
) -> dict[str, object]:
    """Generate deterministic Scenario 4 source CSVs."""
    resolved_output_directory = Path(
        output_directory
    )
    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    frames = Scenario4Builder().build()

    for contract in SCENARIO_4_SOURCE_CONTRACTS:
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
            "seed_customer_ids": [
                GROUP_A_SEED_ID,
                GROUP_B_SEED_ID,
            ],
            "group_a_entity_ids": [
                GROUP_A_SEED_ID,
                *GROUP_A_SME_IDS,
            ],
            "group_b_entity_ids": [
                GROUP_B_SEED_ID,
                GROUP_B_RETAIL_ID,
            ],
            "expected_group_count": 2,
            "expected_entity_count": 5,
            "expected_eid_link_count": 3,
            "payment_event_count": 0,
            "beneficiary_relationship_count": 0,
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
