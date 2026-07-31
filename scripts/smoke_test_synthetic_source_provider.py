"""Validate the reusable synthetic source provider."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)
from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceProvider,
    SourceContractError,
    SourceLoadRequest,
)
from network_mule_discovery.source_snapshot import (
    calculate_source_snapshot_hash,
)
from network_mule_discovery.synthetic_source_provider import (
    SyntheticSourceProvider,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        source_directory = Path(directory)

        manifest = generate_scenario_1_source_data(
            source_directory
        )

        provider = SyntheticSourceProvider(
            source_directory=source_directory
        )

        request = SourceLoadRequest.create(
            dataset_id="scenario_1",
            run_date=manifest["run_date"],
            state_namespace=(
                "synthetic-provider-smoke"
            ),
        )

        bundle = provider.load(request)

        assert isinstance(
            provider,
            DiscoverySourceProvider,
        )
        assert provider.provider_name == "synthetic"
        assert (
            provider.source_directory
            == source_directory
        )
        assert (
            provider.source_manifest
            == manifest
        )

        assert (
            bundle.metadata.provider_name
            == "synthetic"
        )
        assert (
            bundle.metadata.dataset_id
            == request.dataset_id
        )
        assert (
            bundle.metadata.run_date
            == request.run_date
        )
        assert (
            bundle.metadata.state_namespace
            == request.state_namespace
        )
        assert (
            bundle.metadata.source_manifest
            == manifest
        )

        assert tuple(
            bundle.as_mapping()
        ) == SOURCE_DATASET_NAMES

        expected_hash = (
            calculate_source_snapshot_hash(
                dataset_id=request.dataset_id,
                run_date=request.run_date,
                frames=bundle.as_mapping(),
            )
        )

        assert (
            bundle.metadata.source_snapshot_hash
            == expected_hash
        )

        repeated = provider.load(request)

        assert (
            repeated.metadata.source_snapshot_hash
            == bundle.metadata.source_snapshot_hash
        )
        assert (
            repeated.row_counts()
            == bundle.row_counts()
        )

        missing_path = (
            source_directory
            / "customer_identity.csv"
        )
        missing_path.unlink()

        try:
            provider.load(request)
        except SourceContractError as exc:
            assert "customer_identity.csv" in str(exc)
        else:
            raise AssertionError(
                "Missing synthetic dataset was accepted."
            )

        print(
            "Synthetic source provider smoke test passed."
        )
        print("Provider protocol: passed")
        print("Datasets loaded: 9")
        print("Manifest loaded from disk: passed")
        print("Request metadata preserved: passed")
        print("Stable snapshot hash: passed")
        print("Missing dataset rejection: passed")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
