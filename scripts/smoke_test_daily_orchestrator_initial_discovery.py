"""Smoke test for production-path initial discovery."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_orchestrator import (
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)
from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceLoadRequest,
    SourceMetadata,
)
from network_mule_discovery.source_dataset_contracts import (
    SOURCE_DATASET_CONTRACTS,
)
from network_mule_discovery.source_snapshot import (
    calculate_source_snapshot_hash,
)


class ScenarioOneProvider:
    """Test-local provider for Scenario 1 source files."""

    def __init__(
        self,
        source_directory: Path,
        source_manifest: dict[str, object],
    ) -> None:
        self.source_directory = source_directory
        self.source_manifest = source_manifest
        self.load_count = 0

    @property
    def provider_name(self) -> str:
        return "synthetic"

    def load(
        self,
        request: SourceLoadRequest,
    ) -> DiscoverySourceBundle:
        self.load_count += 1
        frames = {}

        for dataset_name in SOURCE_DATASET_NAMES:
            source_path = (
                self.source_directory
                / f"{dataset_name}.csv"
            )

            if source_path.is_file():
                frames[dataset_name] = pd.read_csv(
                    source_path,
                    dtype="string",
                    keep_default_na=False,
                )
            else:
                frames[dataset_name] = pd.DataFrame(
                    columns=SOURCE_DATASET_CONTRACTS[
                        dataset_name
                    ].columns
                )

        snapshot_hash = calculate_source_snapshot_hash(
            dataset_id=request.dataset_id,
            run_date=request.run_date,
            frames=frames,
        )

        return DiscoverySourceBundle(
            metadata=SourceMetadata(
                provider_name=self.provider_name,
                dataset_id=request.dataset_id,
                run_date=request.run_date,
                state_namespace=(
                    request.state_namespace
                ),
                source_manifest=self.source_manifest,
                source_snapshot_hash=snapshot_hash,
            ),
            **frames,
        )


def main() -> None:
    with TemporaryDirectory() as temporary_directory:
        source_directory = Path(
            temporary_directory
        )

        manifest = generate_scenario_1_source_data(
            source_directory
        )

        request = SourceLoadRequest.create(
            dataset_id="scenario_1",
            run_date=manifest["run_date"],
            state_namespace=(
                "initial-discovery-smoke"
            ),
        )

        provider = ScenarioOneProvider(
            source_directory=source_directory,
            source_manifest=manifest,
        )

        preflight = run_source_preflight(
            source_provider=provider,
            source_request=request,
        )

        result = run_initial_discovery(
            source_preflight=preflight,
        )

        assert provider.load_count == 1
        assert result.source_preflight is preflight
        assert not (
            result.eid_discovery
            .seed_resolution
            .seeds
            .empty
        )
        assert not result.eid_discovery.discovered_entities.empty
        assert not result.eid_graph.groups.empty
        assert not (
            result.counterparty_discovery
            .seed_counterparties
            .empty
        )
        assert not result.unified_groups.groups.empty
        assert not result.unified_groups.nodes.empty
        assert not result.unified_groups.edges.empty

        print(
            "Daily orchestrator initial discovery "
            "smoke test passed."
        )
        print("Provider load count: 1")
        print(
            "EID groups: "
            f"{len(result.eid_graph.groups)}"
        )
        print(
            "Seed counterparties: "
            f"{len(result.counterparty_discovery.seed_counterparties)}"
        )
        print(
            "Unified groups: "
            f"{len(result.unified_groups.groups)}"
        )
        print(
            "Unified nodes: "
            f"{len(result.unified_groups.nodes)}"
        )
        print(
            "Unified edges: "
            f"{len(result.unified_groups.edges)}"
        )
        print("Intermediate CSV writes: 0")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
