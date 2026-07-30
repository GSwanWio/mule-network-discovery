"""Smoke test for provider-neutral source contracts."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(REPOSITORY_ROOT),
    )

from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    DiscoverySourceProvider,
    SourceContractError,
    SourceLoadRequest,
    SourceMetadata,
)


def _frame(
    dataset_name: str,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "dataset_name": [
                dataset_name
            ],
        }
    )


def _bundle() -> DiscoverySourceBundle:
    metadata = SourceMetadata(
        provider_name="synthetic",
        dataset_id="scenario_1",
        state_namespace="smoke-test",
        run_date="2026-07-30",
        source_manifest={
            "contract_version": "1",
        },
        source_snapshot_hash="a" * 64,
    )

    return DiscoverySourceBundle(
        metadata=metadata,
        **{
            dataset_name: _frame(
                dataset_name
            )
            for dataset_name
            in SOURCE_DATASET_NAMES
        },
    )


class SmokeTestProvider:
    """Minimal provider used only by this test."""

    provider_name = "synthetic"

    def load(
        self,
        request: SourceLoadRequest,
    ) -> DiscoverySourceBundle:
        bundle = _bundle()

        assert (
            request.dataset_id
            == bundle.metadata.dataset_id
        )

        assert (
            request.run_date
            == bundle.metadata.run_date
        )

        assert (
            request.state_namespace
            == bundle.metadata.state_namespace
        )

        return bundle


def _assert_invalid_hash_is_rejected() -> None:
    try:
        SourceMetadata(
            provider_name="synthetic",
            dataset_id="scenario_1",
            state_namespace="smoke-test",
            run_date="2026-07-30",
            source_manifest={},
            source_snapshot_hash="not-a-sha256",
        )
    except SourceContractError:
        return

    raise AssertionError(
        "Invalid source snapshot hash was accepted."
    )


def _assert_non_frame_is_rejected() -> None:
    bundle_arguments = {
        dataset_name: _frame(
            dataset_name
        )
        for dataset_name
        in SOURCE_DATASET_NAMES
    }

    bundle_arguments[
        "seed_mule_pool"
    ] = "not-a-frame"

    try:
        DiscoverySourceBundle(
            metadata=_bundle().metadata,
            **bundle_arguments,
        )
    except SourceContractError:
        return

    raise AssertionError(
        "A non-DataFrame source dataset was accepted."
    )


def main() -> None:
    request = SourceLoadRequest.create(
        dataset_id=" scenario_1 ",
        run_date="2026-07-30",
        state_namespace=" smoke-test ",
    )

    provider = SmokeTestProvider()

    assert isinstance(
        provider,
        DiscoverySourceProvider,
    )

    bundle = provider.load(
        request
    )

    datasets = bundle.as_mapping()

    assert (
        tuple(datasets)
        == SOURCE_DATASET_NAMES
    )

    assert bundle.row_counts() == {
        dataset_name: 1
        for dataset_name
        in SOURCE_DATASET_NAMES
    }

    assert (
        bundle.metadata.provider_name
        == "synthetic"
    )

    assert (
        bundle.metadata.source_snapshot_hash
        == "a" * 64
    )

    _assert_invalid_hash_is_rejected()
    _assert_non_frame_is_rejected()

    print(
        "Provider-neutral source contracts "
        "smoke test passed."
    )
    print(
        f"Datasets defined: "
        f"{len(SOURCE_DATASET_NAMES)}"
    )
    print("Provider protocol: passed")
    print("Source metadata validation: passed")
    print("Source bundle validation: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
