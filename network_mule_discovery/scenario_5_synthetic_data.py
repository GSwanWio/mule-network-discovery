"""Generate production-shaped synthetic source data for Scenario 5."""

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
    SCENARIO_5_SOURCE_CONTRACTS,
)


GENERATION_SEED = 20260727
SCENARIO_NAME = "scenario_5_insufficient_counterparty_evidence"
RUN_DATE = date(2026, 7, 20)

SEED_CUSTOMER_ID = "R5001"
LINKED_CUSTOMER_IDS = ("R5002", "B5001")
AMBIGUOUS_COUNTERPARTY_ACCOUNT = "750500000001"
AMBIGUOUS_COUNTERPARTY_NAME = "Al Noor General Trading"
INITIAL_PAYMENT_COUNT = 3
CHANGED_PAYMENT_COUNT = 6


class Scenario5Builder:
    """Build a small, genuinely ambiguous shared-counterparty branch."""

    def __init__(self, *, changed_evidence: bool = False) -> None:
        self.builder = SyntheticScenarioBuilder(GENERATION_SEED)
        self.changed_evidence = changed_evidence

    def build_population(self) -> None:
        customers = (
            (SEED_CUSTOMER_ID, "RETAIL", 1, date(2022, 3, 10)),
            ("R5002", "RETAIL", 2, date(2024, 9, 15)),
            ("B5001", "SME", 3, date(2023, 5, 20)),
        )

        for customer_id, entity_type, sequence, created_date in customers:
            self.builder.add_customer(
                customer_id=customer_id,
                entity_type=entity_type,
                emirates_id_number=synthetic_eid(
                    1987 + sequence,
                    5000000 + sequence,
                    style=sequence % 3,
                ),
                segment=("SME" if entity_type == "SME" else "RETAIL"),
                customer_created_date=format_date(created_date),
                account_sequence=250000000000 + sequence,
                individual_id=(
                    f"IND-S5-{sequence:03d}"
                    if entity_type == "SME"
                    else ""
                ),
            )

    def build_seed_pool(self) -> list[dict[str, object]]:
        account = self.builder.accounts[SEED_CUSTOMER_ID]
        return [
            {
                "snapshot_date": str(RUN_DATE),
                "seed_event_id": "S5-SEED-0001",
                "seed_customer_id": SEED_CUSTOMER_ID,
                "seed_account_id": account.account_id,
                "seed_account_number": account.account_number,
                "seed_iban": account.iban,
                "seed_entity_type": "RETAIL",
                "date_reported": "2026-07-05",
                "seed_source": "FRC_CONFIRMED_MULE",
                "source_event_type": "FTS_REFUND_REQUEST",
                "source_transaction_reference": "S5-FRC-TXN-0001",
            }
        ]

    def build_ambiguous_branch(self) -> None:
        payment_plan: list[tuple[str, datetime, float]] = [
            (SEED_CUSTOMER_ID, datetime(2026, 4, 12, 13, 15), 420.0),
            ("R5002", datetime(2026, 5, 8, 18, 40), 275.0),
            ("B5001", datetime(2026, 6, 3, 11, 25), 610.0),
        ]

        if self.changed_evidence:
            payment_plan.extend(
                [
                    ("R5002", datetime(2026, 7, 2, 10, 5), 3200.0),
                    ("B5001", datetime(2026, 7, 3, 10, 20), 4600.0),
                    (SEED_CUSTOMER_ID, datetime(2026, 7, 4, 9, 55), 3900.0),
                ]
            )

        beneficiary_ids: dict[str, str] = {}
        for index, customer_id in enumerate(
            (SEED_CUSTOMER_ID, *LINKED_CUSTOMER_IDS),
            start=1,
        ):
            beneficiary_ids[customer_id] = self.builder.add_beneficiary(
                owner_customer_id=customer_id,
                account_number=AMBIGUOUS_COUNTERPARTY_ACCOUNT,
                account_holder_name=AMBIGUOUS_COUNTERPARTY_NAME,
                created_at=datetime(2025, 8, 1, 9, 0) + timedelta(days=index * 31),
                nick_name="General Trading",
                bank_name="Emirates Commercial Bank",
            )

            self.builder.add_inward(
                customer_id=customer_id,
                timestamp=datetime(2026, 1, 15, 8, 0) + timedelta(days=index),
                amount=(7200.0 if customer_id.startswith("R") else 28000.0),
                purpose_key=("SALARY" if customer_id.startswith("R") else "BUSINESS_INCOME"),
                purpose_name=("Salary" if customer_id.startswith("R") else "Business Income"),
            )

        for customer_id, timestamp, amount in payment_plan:
            self.builder.add_outward(
                customer_id=customer_id,
                timestamp=timestamp,
                beneficiary_id=beneficiary_ids[customer_id],
                beneficiary_account_number=AMBIGUOUS_COUNTERPARTY_ACCOUNT,
                amount=amount,
                purpose_key="GOODS_SERVICES",
                purpose_name="Goods and Services",
            )

    def build(self) -> dict[str, pd.DataFrame]:
        self.build_population()
        seed_rows = self.build_seed_pool()
        self.build_ambiguous_branch()
        return self.builder.build_frames(seed_rows)


def generate_scenario_5_source_data(
    output_directory: Path | str,
    *,
    changed_evidence: bool = False,
) -> dict[str, object]:
    """Generate deterministic Scenario 5 source CSVs."""
    resolved_output_directory = Path(output_directory)
    resolved_output_directory.mkdir(parents=True, exist_ok=True)

    frames = Scenario5Builder(changed_evidence=changed_evidence).build()

    for contract in SCENARIO_5_SOURCE_CONTRACTS:
        frames[contract.filename].to_csv(
            resolved_output_directory / contract.filename,
            index=False,
        )

    manifest = {
        "scenario_name": SCENARIO_NAME,
        "generation_seed": GENERATION_SEED,
        "run_date": str(RUN_DATE),
        "evidence_phase": "CHANGED" if changed_evidence else "INITIAL",
        "source_only": True,
        "contains_prebuilt_groups": False,
        "contains_prebuilt_nodes": False,
        "contains_prebuilt_edges": False,
        "contains_ai_decisions": False,
        "source_files": sorted(frames),
        "row_counts": {
            filename: len(frame)
            for filename, frame in sorted(frames.items())
        },
        "sha256": {
            filename: hash_file(resolved_output_directory / filename)
            for filename in sorted(frames)
        },
        "design_reference": {
            "seed_customer_id": SEED_CUSTOMER_ID,
            "linked_customer_ids": list(LINKED_CUSTOMER_IDS),
            "ambiguous_counterparty_account": AMBIGUOUS_COUNTERPARTY_ACCOUNT,
            "initial_payment_count": INITIAL_PAYMENT_COUNT,
            "changed_payment_count": CHANGED_PAYMENT_COUNT,
        },
    }

    (resolved_output_directory / "source_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest
