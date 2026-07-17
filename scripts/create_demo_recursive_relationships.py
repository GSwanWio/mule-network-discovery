"""Create production-shaped recursive expansion evidence."""

from __future__ import annotations

import csv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = (
    PROJECT_ROOT
    / "data/demo/recursive_relationship_candidates.csv"
)


ROWS = [
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "RETAIL|R3002",
        "relationship_type": "SAME_EMIRATES_ID",
        "counterparty_key": "",
        "counterparty_name": "",
        "target_entity_type": "RETAIL",
        "target_entity_id": "R4001",
        "target_entity_key": "RETAIL|R4001",
        "evidence_key": "784199000000101",
        "evidence_summary": "Same Emirates ID",
        "source_event_count": "",
        "candidate_event_count": "",
    },
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "RETAIL|R3002",
        "relationship_type": "SHARED_EXTERNAL_COUNTERPARTY",
        "counterparty_key": "LOCAL_ACCOUNT|004440001",
        "counterparty_name": "Second Layer Local Trading",
        "target_entity_type": "SME",
        "target_entity_id": "B4002",
        "target_entity_key": "SME|B4002",
        "evidence_key": "R3002|LOCAL_ACCOUNT|004440001|B4002",
        "evidence_summary": "Shared local beneficiary account",
        "source_event_count": "3",
        "candidate_event_count": "2",
    },
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "RETAIL|R3003",
        "relationship_type": "SAME_EMIRATES_ID",
        "counterparty_key": "",
        "counterparty_name": "",
        "target_entity_type": "SME",
        "target_entity_id": "B4003",
        "target_entity_key": "SME|B4003",
        "evidence_key": "784199000000102",
        "evidence_summary": "Same Emirates ID",
        "source_event_count": "",
        "candidate_event_count": "",
    },
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "RETAIL|R3003",
        "relationship_type": "SHARED_EXTERNAL_COUNTERPARTY",
        "counterparty_key": "IBAN|GB82WEST12345698765432",
        "counterparty_name": "Established UK Supplier",
        "target_entity_type": "RETAIL",
        "target_entity_id": "R4004",
        "target_entity_key": "RETAIL|R4004",
        "evidence_key": "R3003|IBAN|GB82WEST12345698765432|R4004",
        "evidence_summary": "Shared international beneficiary",
        "source_event_count": "2",
        "candidate_event_count": "4",
    },
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "RETAIL|R3003",
        "relationship_type": "BENEFICIARY_ADDED_MULE_ACCOUNT",
        "counterparty_key": "",
        "counterparty_name": "",
        "target_entity_type": "RETAIL",
        "target_entity_id": "R4005",
        "target_entity_key": "RETAIL|R4005",
        "evidence_key": "R4005|BENEFICIARY|R3003",
        "evidence_summary": (
            "Customer added the approved mule account "
            "as a beneficiary"
        ),
        "source_event_count": "1",
        "candidate_event_count": "1",
    },
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "RETAIL|R4001",
        "relationship_type": "SHARED_EXTERNAL_COUNTERPARTY",
        "counterparty_key": (
            "SWIFT_ACCOUNT|BOFAUS3N|998877"
        ),
        "counterparty_name": "Third Layer Settlement Account",
        "target_entity_type": "RETAIL",
        "target_entity_id": "R5002",
        "target_entity_key": "RETAIL|R5002",
        "evidence_key": (
            "R4001|SWIFT_ACCOUNT|BOFAUS3N|998877|R5002"
        ),
        "evidence_summary": "Shared international account",
        "source_event_count": "2",
        "candidate_event_count": "2",
    },
    {
        "snapshot_date": "2026-07-16",
        "source_entity_key": "SME|B4002",
        "relationship_type": "SAME_EMIRATES_ID",
        "counterparty_key": "",
        "counterparty_name": "",
        "target_entity_type": "RETAIL",
        "target_entity_id": "R5001",
        "target_entity_key": "RETAIL|R5001",
        "evidence_key": "784199000000103",
        "evidence_summary": "Same Emirates ID",
        "source_event_count": "",
        "candidate_event_count": "",
    },
]


def main() -> None:
    """Write the recursive evidence contract."""
    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_PATH.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(ROWS[0].keys()),
            lineterminator="\n",
        )

        writer.writeheader()
        writer.writerows(ROWS)

    print(f"Recursive evidence rows: {len(ROWS)}")
    print(f"Path: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
