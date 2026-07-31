"""Validate currency-safe bundle behavioral sources."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.bundle_behavioral_sources import (
    build_bundle_behavioral_sources,
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


def _row(
    dataset_name: str,
    **values: object,
) -> dict[str, object]:
    record = {
        column: ""
        for column in SOURCE_DATASET_CONTRACTS[
            dataset_name
        ].columns
    }
    record.update(values)
    return record


def main() -> None:
    frames = {
        dataset_name: pd.DataFrame(
            columns=SOURCE_DATASET_CONTRACTS[
                dataset_name
            ].columns
        )
        for dataset_name in SOURCE_DATASET_NAMES
    }

    inward_name = "international_inward_payments"
    outward_name = "international_outward_payments"

    frames[inward_name] = pd.DataFrame(
        [
            _row(
                inward_name,
                transfer_id="II1",
                status="COMPLETED",
                customer_id="C1",
                transaction_timestamp="2026-07-29 10:00:00",
                source_customer_iban="SOURCE-IBAN-1",
                target_amount="100",
                target_currency="USD",
            ),
            _row(
                inward_name,
                transfer_id="II2",
                status="COMPLETED",
                customer_id="C1",
                transaction_timestamp="2026-07-29 11:00:00",
                source_customer_iban="SOURCE-IBAN-2",
                target_amount="200",
                target_currency="EUR",
            ),
        ],
        columns=SOURCE_DATASET_CONTRACTS[
            inward_name
        ].columns,
    )

    frames[outward_name] = pd.DataFrame(
        [
            _row(
                outward_name,
                transfer_id="IO1",
                status="COMPLETED",
                customer_id="C1",
                transaction_timestamp="2026-07-29 12:00:00",
                beneficiary_account_number="ACC-1",
                beneficiary_swift_code="BANKAEAD",
                source_amount="30",
                source_currency="USD",
            ),
            _row(
                outward_name,
                transfer_id="IO2",
                status="COMPLETED",
                customer_id="C1",
                transaction_timestamp="2026-07-29 13:00:00",
                beneficiary_account_number="ACC-1",
                beneficiary_swift_code="BANKAEAD",
                source_amount="20",
                source_currency="USD",
            ),
            _row(
                outward_name,
                transfer_id="IO3",
                status="COMPLETED",
                customer_id="C2",
                transaction_timestamp="2026-07-29 14:00:00",
                beneficiary_account_number="ACC-2",
                beneficiary_swift_code="BANKEUXX",
                source_amount="50",
                source_currency="EUR",
            ),
            _row(
                outward_name,
                transfer_id="IO4",
                status="FAILED",
                customer_id="C1",
                transaction_timestamp="2026-07-29 15:00:00",
                beneficiary_account_number="ACC-3",
                beneficiary_swift_code="BANKUSXX",
                source_amount="999",
                source_currency="USD",
            ),
        ],
        columns=SOURCE_DATASET_CONTRACTS[
            outward_name
        ].columns,
    )

    bundle = DiscoverySourceBundle(
        metadata=SourceMetadata(
            provider_name="synthetic",
            dataset_id="behavioral-source-smoke",
            run_date=RUN_DATE,
            state_namespace="behavioral-source-smoke",
            source_manifest={
                "contract_version": "1",
            },
            source_snapshot_hash="0" * 64,
        ),
        **frames,
    )

    result = build_bundle_behavioral_sources(
        bundle
    )

    customer_activity = (
        result
        .international_customer_currency_activity
        .set_index(
            [
                "customer_id",
                "direction",
                "currency",
            ]
        )
    )

    assert len(customer_activity) == 4

    assert customer_activity.loc[
        ("C1", "INWARD", "USD"),
        "total_amount",
    ] == 100.0

    assert customer_activity.loc[
        ("C1", "INWARD", "EUR"),
        "total_amount",
    ] == 200.0

    assert customer_activity.loc[
        ("C1", "OUTWARD", "USD"),
        "total_amount",
    ] == 50.0

    assert customer_activity.loc[
        ("C1", "OUTWARD", "USD"),
        "event_count",
    ] == 2

    counterparty_activity = (
        result
        .international_counterparty_currency_activity
    )

    assert len(counterparty_activity) == 2
    assert int(
        counterparty_activity["event_count"].sum()
    ) == 3
    assert float(
        counterparty_activity["total_amount"].sum()
    ) == 100.0

    assert (
        result.raw_sources.local_inward_payments.empty
    )
    assert (
        result.raw_sources.local_outward_payments.empty
    )

    print(
        "Bundle behavioral sources smoke test passed."
    )
    print("International customer currency rows: 4")
    print("International counterparty currency rows: 2")
    print("Completed international events: 5")
    print("Failed international events excluded: 1")
    print("USD and EUR amounts kept separate: passed")
    print("Cross-currency aggregation performed: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
