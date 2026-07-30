"""Smoke test for deterministic source snapshot hashing."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable

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
)
from network_mule_discovery.source_snapshot import (
    SOURCE_SNAPSHOT_CONTRACT_VERSION,
    calculate_source_snapshot_hash,
    verify_source_bundle_snapshot,
)


def frame_for(
    dataset_name: str,
) -> pd.DataFrame:
    contract = SOURCE_DATASET_CONTRACTS[
        dataset_name
    ]

    rows = []

    for row_number in (1, 2):
        rows.append(
            {
                column: (
                    f"{dataset_name}|"
                    f"{column}|"
                    f"{row_number}"
                )
                for column in contract.columns
            }
        )

    return pd.DataFrame(
        rows,
        columns=contract.columns,
    )


def source_frames() -> dict[
    str,
    pd.DataFrame,
]:
    return {
        dataset_name: frame_for(
            dataset_name
        )
        for dataset_name
        in SOURCE_DATASET_NAMES
    }


def copy_frames(
    frames: dict[
        str,
        pd.DataFrame,
    ],
) -> dict[
    str,
    pd.DataFrame,
]:
    return {
        dataset_name: frame.copy(
            deep=True
        )
        for dataset_name, frame
        in frames.items()
    }


def expect_error(
    action: Callable[[], object],
    expected_message: str,
) -> None:
    try:
        action()
    except SourceContractError as exc:
        assert expected_message in str(exc)
        return

    raise AssertionError(
        "Invalid source snapshot was accepted."
    )


def calculate_hash(
    frames: dict[
        str,
        pd.DataFrame,
    ],
    *,
    dataset_id: str = "scenario_1",
    run_date: str = "2026-07-30",
) -> str:
    return calculate_source_snapshot_hash(
        dataset_id=dataset_id,
        run_date=run_date,
        frames=frames,
    )


def main() -> None:
    frames = source_frames()

    baseline_hash = calculate_hash(
        frames
    )

    assert len(baseline_hash) == 64

    assert all(
        character in "0123456789abcdef"
        for character in baseline_hash
    )

    reordered_frames = {}

    for dataset_name, frame in frames.items():
        reordered_frames[dataset_name] = (
            frame
            .iloc[::-1]
            .reset_index(drop=True)
            .loc[
                :,
                list(frame.columns)[::-1],
            ]
        )

    reordered_hash = calculate_hash(
        reordered_frames
    )

    assert reordered_hash == baseline_hash

    changed_frames = copy_frames(
        frames
    )

    changed_dataset = "customer_identity"

    changed_column = (
        SOURCE_DATASET_CONTRACTS[
            changed_dataset
        ].columns[-1]
    )

    original_value = changed_frames[
        changed_dataset
    ].loc[
        0,
        changed_column,
    ]

    changed_frames[
        changed_dataset
    ].loc[
        0,
        changed_column,
    ] = f"{original_value}|changed"

    assert (
        calculate_hash(changed_frames)
        != baseline_hash
    )

    assert (
        calculate_hash(
            frames,
            dataset_id="scenario_2",
        )
        != baseline_hash
    )

    assert (
        calculate_hash(
            frames,
            run_date="2026-07-31",
        )
        != baseline_hash
    )

    metadata = SourceMetadata(
        provider_name="synthetic",
        dataset_id="scenario_1",
        state_namespace="snapshot-smoke",
        run_date="2026-07-30",
        source_manifest={
            "contract_version": "1",
        },
        source_snapshot_hash=baseline_hash,
    )

    bundle = DiscoverySourceBundle(
        metadata=metadata,
        **copy_frames(frames),
    )

    assert (
        verify_source_bundle_snapshot(
            bundle
        )
        == baseline_hash
    )

    invalid_metadata = SourceMetadata(
        provider_name="synthetic",
        dataset_id="scenario_1",
        state_namespace="snapshot-smoke",
        run_date="2026-07-30",
        source_manifest={
            "contract_version": "1",
        },
        source_snapshot_hash="0" * 64,
    )

    invalid_bundle = DiscoverySourceBundle(
        metadata=invalid_metadata,
        **copy_frames(frames),
    )

    expect_error(
        lambda: verify_source_bundle_snapshot(
            invalid_bundle
        ),
        "does not match",
    )

    missing_frames = copy_frames(
        frames
    )

    del missing_frames[
        "sme_beneficiary_master"
    ]

    expect_error(
        lambda: calculate_hash(
            missing_frames
        ),
        "Missing datasets",
    )

    unexpected_frames = copy_frames(
        frames
    )

    unexpected_frames[
        "unexpected_dataset"
    ] = pd.DataFrame()

    expect_error(
        lambda: calculate_hash(
            unexpected_frames
        ),
        "Unexpected datasets",
    )

    print(
        "Provider-neutral source snapshot "
        "smoke test passed."
    )
    print(
        f"Snapshot contract: "
        f"{SOURCE_SNAPSHOT_CONTRACT_VERSION}"
    )
    print(
        f"Datasets hashed: "
        f"{len(SOURCE_DATASET_NAMES)}"
    )
    print("SHA-256 format: passed")
    print("Row-order independence: passed")
    print("Column-order independence: passed")
    print("Value-change detection: passed")
    print("Dataset identity detection: passed")
    print("Run-date detection: passed")
    print("Bundle hash verification: passed")
    print("Missing-dataset rejection: passed")
    print("Unexpected-dataset rejection: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
