"""Smoke test for provider-neutral dataset validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(ROOT),
    )

from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceContractError,
    SourceMetadata,
)
from network_mule_discovery.source_dataset_contracts import (
    SOURCE_DATASET_CONTRACTS,
    validate_source_bundle,
    validate_source_frame,
)


def frame_for(
    name: str,
) -> pd.DataFrame:
    contract = SOURCE_DATASET_CONTRACTS[
        name
    ]

    if not contract.allow_nonempty:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                column: f"{name}|{column}"
                for column in contract.columns
            }
        ],
        columns=contract.columns,
    )


def expect_error(
    action,
    message: str,
) -> None:
    try:
        action()
    except SourceContractError as exc:
        assert message in str(exc)
        return

    raise AssertionError(
        "Invalid source data was accepted."
    )


def main() -> None:
    assert (
        tuple(SOURCE_DATASET_CONTRACTS)
        == SOURCE_DATASET_NAMES
    )

    bundle = DiscoverySourceBundle(
        metadata=SourceMetadata(
            provider_name="synthetic",
            dataset_id="scenario_1",
            state_namespace="source-contract-smoke",
            run_date="2026-07-30",
            source_manifest={
                "contract_version": "1",
            },
            source_snapshot_hash="b" * 64,
        ),
        **{
            name: frame_for(name)
            for name in SOURCE_DATASET_NAMES
        },
    )

    validated = validate_source_bundle(
        bundle
    )

    assert (
        tuple(validated.as_mapping())
        == SOURCE_DATASET_NAMES
    )

    supported = [
        name
        for name, contract
        in SOURCE_DATASET_CONTRACTS.items()
        if contract.allow_nonempty
    ]

    placeholders = [
        name
        for name, contract
        in SOURCE_DATASET_CONTRACTS.items()
        if not contract.allow_nonempty
    ]

    assert len(supported) == 9
    assert placeholders == []

    contract = SOURCE_DATASET_CONTRACTS[
        "seed_mule_pool"
    ]

    valid = frame_for(
        "seed_mule_pool"
    )

    empty = validate_source_frame(
        "seed_mule_pool",
        pd.DataFrame(
            columns=contract.columns
        ),
    )

    assert empty.empty
    assert (
        tuple(empty.columns)
        == contract.columns
    )

    expect_error(
        lambda: validate_source_frame(
            "seed_mule_pool",
            valid.drop(
                columns=[
                    contract.columns[0]
                ]
            ),
        ),
        "Missing columns",
    )

    expect_error(
        lambda: validate_source_frame(
            "seed_mule_pool",
            valid.assign(
                extra="unexpected"
            ),
        ),
        "Unexpected columns",
    )

    blank = valid.copy()

    blank.loc[
        0,
        contract.required_nonblank[0],
    ] = " "

    expect_error(
        lambda: validate_source_frame(
            "seed_mule_pool",
            blank,
        ),
        "contains blank values",
    )

    duplicate = pd.concat(
        [
            valid,
            valid,
        ],
        ignore_index=True,
    )

    expect_error(
        lambda: validate_source_frame(
            "seed_mule_pool",
            duplicate,
        ),
        "contains duplicate keys",
    )

    for name in (
        "international_inward_payments",
        "international_outward_payments",
    ):
        international = validate_source_frame(
            name,
            frame_for(name),
        )

        assert len(international) == 1

        assert tuple(
            international.columns
        ) == SOURCE_DATASET_CONTRACTS[
            name
        ].columns

    expect_error(
        lambda: validate_source_frame(
            "unknown_dataset",
            pd.DataFrame(),
        ),
        "Unknown source dataset",
    )

    print(
        "Provider-neutral dataset validation "
        "smoke test passed."
    )
    print(
        f"Dataset contracts: "
        f"{len(SOURCE_DATASET_CONTRACTS)}"
    )
    print(
        f"Populated datasets supported: "
        f"{len(supported)}"
    )
    print(
        f"Explicit empty placeholders: "
        f"{len(placeholders)}"
    )
    print("Column validation: passed")
    print("Required-value validation: passed")
    print("Unique-key validation: passed")
    print("Bundle normalization: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
