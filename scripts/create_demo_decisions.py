"""Create persisted demo AI decisions from current feature hashes."""

from __future__ import annotations

import csv
import hashlib
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from network_mule_discovery.decision_engine import (
    build_subject_snapshots,
)


OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"
DECISION_PATH = PROJECT_ROOT / "data/demo/ai_decisions.csv"


def stable_decision_id(
    subject_type: str,
    subject_key: str,
    feature_snapshot_hash: str,
    decision: str,
) -> str:
    """Create a deterministic demo decision ID."""
    value = "|".join(
        [
            subject_type,
            subject_key,
            feature_snapshot_hash,
            decision,
            "demo-v1",
        ]
    )

    digest = hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()[:16]

    return f"D{digest}"


def main() -> None:
    """Build matching and stale cached decisions."""
    nodes = pd.read_csv(
        OUTPUT_DIRECTORY / "unified_group_nodes.csv",
        dtype="string",
        keep_default_na=False,
    )

    edges = pd.read_csv(
        OUTPUT_DIRECTORY / "unified_group_edges.csv",
        dtype="string",
        keep_default_na=False,
    )

    snapshots = build_subject_snapshots(
        nodes=nodes,
        edges=edges,
    )

    snapshot_map = {
        (
            row.subject_type,
            row.subject_key,
        ): row.feature_snapshot_hash
        for row in snapshots.itertuples(
            index=False
        )
    }

    decision_definitions = [
        (
            "COUNTERPARTY",
            "IBAN|DE89370400440532013000",
            "LEGITIMATE_SUPPRESS",
            "ESTABLISHED_BUSINESS_COUNTERPARTY",
            False,
        ),
        (
            "COUNTERPARTY",
            "SWIFT_ACCOUNT|BARCGB22|GBACC9002",
            "SUSPICIOUS_EXPAND",
            "UNEXPLAINED_SHARED_COUNTERPARTY",
            False,
        ),
        (
            "COUNTERPARTY",
            "IBAN|AE120260000000000077701",
            "SUSPICIOUS_EXPAND",
            "RAPID_SHARED_SENDER_PATTERN",
            True,
        ),
        (
            "CUSTOMER",
            "RETAIL|R3003",
            "MULE_LIKE",
            "KNOWN_MULE_ADDED_AS_BENEFICIARY",
            False,
        ),
        (
            "CUSTOMER",
            "SME|B3003",
            "LOW_CONCERN",
            "PLAUSIBLE_DOCUMENTED_RELATIONSHIP",
            False,
        ),
        (
            "CUSTOMER",
            "RETAIL|R3002",
            "MULE_LIKE",
            "SHARED_SUSPICIOUS_COUNTERPARTY",
            False,
        ),
    ]

    rows: list[dict[str, str]] = []

    for (
        subject_type,
        subject_key,
        decision,
        reason_code,
        stale_flag,
    ) in decision_definitions:
        current_hash = snapshot_map[
            (
                subject_type,
                subject_key,
            )
        ]

        feature_snapshot_hash = (
            f"stale_{current_hash[6:]}"
            if stale_flag
            else current_hash
        )

        rows.append(
            {
                "decision_id": stable_decision_id(
                    subject_type=subject_type,
                    subject_key=subject_key,
                    feature_snapshot_hash=(
                        feature_snapshot_hash
                    ),
                    decision=decision,
                ),
                "subject_type": subject_type,
                "subject_key": subject_key,
                "feature_snapshot_hash": (
                    feature_snapshot_hash
                ),
                "decision": decision,
                "reason_code": reason_code,
                "decision_version": "demo-v1",
                "decided_at": (
                    "2026-07-16 18:00:00"
                ),
                "source": "DEMO_AI",
            }
        )

    DECISION_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with DECISION_PATH.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(rows[0].keys()),
            lineterminator="\n",
        )

        writer.writeheader()
        writer.writerows(rows)

    print(f"Created decisions: {len(rows)}")
    print(f"Path: {DECISION_PATH}")


if __name__ == "__main__":
    main()
