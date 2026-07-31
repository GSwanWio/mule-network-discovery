"""Validate international-only counterparty AI payloads."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.bundle_behavioral_sources import (
    build_bundle_counterparty_payloads,
)
from network_mule_discovery.counterparty_schemas import (
    build_counterparty_identity,
)
from network_mule_discovery.decision_policy import (
    COUNTERPARTY_ASSESSMENT_POLICY_VERSION,
)
from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceMetadata,
)
from network_mule_discovery.source_dataset_contracts import (
    SOURCE_DATASET_CONTRACTS,
)


RUN_DATE = "2026-07-30"


def empty_frames() -> dict[str, pd.DataFrame]:
    return {
        dataset_name: pd.DataFrame(
            columns=SOURCE_DATASET_CONTRACTS[
                dataset_name
            ].columns
        )
        for dataset_name in SOURCE_DATASET_NAMES
    }


def outward_row(
    *,
    transfer_id: str,
    currency: str,
    amount: str,
    customer_id: str,
) -> dict[str, object]:
    dataset_name = "international_outward_payments"

    row = {
        column: ""
        for column in SOURCE_DATASET_CONTRACTS[
            dataset_name
        ].columns
    }

    row.update(
        {
            "transfer_id": transfer_id,
            "status": "COMPLETED",
            "customer_id": customer_id,
            "transaction_timestamp": (
                "2026-07-29 12:00:00"
            ),
            "beneficiary_account_number": "ACC-9001",
            "beneficiary_swift_code": "BANKUS33",
            "source_amount": amount,
            "source_currency": currency,
        }
    )

    return row


def main() -> None:
    frames = empty_frames()

    dataset_name = "international_outward_payments"

    frames[dataset_name] = pd.DataFrame(
        [
            outward_row(
                transfer_id="IO1",
                currency="USD",
                amount="100",
                customer_id="C1",
            ),
            outward_row(
                transfer_id="IO2",
                currency="USD",
                amount="50",
                customer_id="C2",
            ),
            outward_row(
                transfer_id="IO3",
                currency="EUR",
                amount="80",
                customer_id="C1",
            ),
        ],
        columns=SOURCE_DATASET_CONTRACTS[
            dataset_name
        ].columns,
    )

    bundle = DiscoverySourceBundle(
        metadata=SourceMetadata(
            provider_name="synthetic",
            dataset_id=(
                "international-counterparty-smoke"
            ),
            run_date=RUN_DATE,
            state_namespace=(
                "international-counterparty-smoke"
            ),
            source_manifest={
                "contract_version": "1",
            },
            source_snapshot_hash="0" * 64,
        ),
        **frames,
    )

    counterparty_key = build_counterparty_identity(
        rail="INTERNATIONAL",
        counterparty_iban="",
        counterparty_swift_bic="BANKUS33",
        counterparty_account_number="ACC-9001",
    ).counterparty_key

    payloads = build_bundle_counterparty_payloads(
        source_bundle=bundle,
        counterparty_keys=[counterparty_key],
    )

    assert len(payloads) == 1
    assert payloads.iloc[0]["subject_type"] == (
        "COUNTERPARTY"
    )
    assert payloads.iloc[0]["subject_key"] == (
        counterparty_key
    )

    payload = json.loads(
        payloads.iloc[0]["feature_payload_json"]
    )

    assert payload["subject_type"] == "COUNTERPARTY"
    assert payload["subject_key"] == counterparty_key
    assert payload[
        "counterparty_assessment_policy_version"
    ] == COUNTERPARTY_ASSESSMENT_POLICY_VERSION
    assert payload["evidence_scope"] == (
        "INTERNATIONAL_CURRENCY_SUMMARY"
    )

    activity = {
        row["currency"]: row
        for row in payload[
            "international_currency_activity"
        ]
    }

    assert set(activity) == {"EUR", "USD"}

    assert activity["USD"]["event_count"] == 2
    assert activity["USD"]["total_amount"] == 150.0
    assert activity["USD"][
        "distinct_customer_count"
    ] == 2

    assert activity["EUR"]["event_count"] == 1
    assert activity["EUR"]["total_amount"] == 80.0
    assert activity["EUR"][
        "distinct_customer_count"
    ] == 1

    assert payload["aggregate_behavior"][
        "transfer_event_count"
    ] == 3
    assert payload["aggregate_behavior"][
        "amounts_kept_separate_by_currency"
    ] is True

    print(
        "Bundle counterparty payload smoke test passed."
    )
    print("International-only counterparties: 1")
    print("Currency groups: 2")
    print("Completed transfer events: 3")
    print("USD total: 150.0")
    print("EUR total: 80.0")
    print("Cross-currency aggregation performed: 0")
    print("Policy version validation: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
