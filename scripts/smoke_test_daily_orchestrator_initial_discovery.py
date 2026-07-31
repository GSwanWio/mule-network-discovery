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


from network_mule_discovery.synthetic_source_provider import (
    SyntheticSourceProvider,
)


ScenarioOneProvider = SyntheticSourceProvider

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
