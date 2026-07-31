"""Validate all registered scenarios through production discovery."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_orchestrator import (
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)
from network_mule_discovery.synthetic_scenario_registry import (
    SUPPORTED_SYNTHETIC_SCENARIOS,
    create_synthetic_source_provider,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        summaries: list[dict[str, object]] = []

        for scenario_id in SUPPORTED_SYNTHETIC_SCENARIOS:
            source_directory = root / scenario_id

            provider = create_synthetic_source_provider(
                scenario_id=scenario_id,
                output_directory=source_directory,
            )
            manifest = provider.source_manifest

            request = SourceLoadRequest.create(
                dataset_id=scenario_id,
                run_date=manifest["run_date"],
                state_namespace=(
                    f"provider-orchestrator-{scenario_id}"
                ),
            )

            preflight = run_source_preflight(
                source_provider=provider,
                source_request=request,
            )
            discovery = run_initial_discovery(
                source_preflight=preflight,
            )

            assert provider.load_count == 1
            assert (
                preflight.source_bundle.metadata.dataset_id
                == scenario_id
            )
            assert (
                preflight.source_bundle.metadata.provider_name
                == "synthetic"
            )
            assert manifest["source_only"] is True
            assert (
                manifest["contains_prebuilt_groups"]
                is False
            )
            assert (
                manifest["contains_prebuilt_nodes"]
                is False
            )
            assert (
                manifest["contains_prebuilt_edges"]
                is False
            )
            assert (
                manifest["contains_ai_decisions"]
                is False
            )

            assert not discovery.unified_groups.groups.empty
            assert not discovery.unified_groups.nodes.empty

            summaries.append(
                {
                    "scenario_id": scenario_id,
                    "groups": len(
                        discovery.unified_groups.groups
                    ),
                    "nodes": len(
                        discovery.unified_groups.nodes
                    ),
                    "edges": len(
                        discovery.unified_groups.edges
                    ),
                    "seed_counterparties": len(
                        discovery
                        .counterparty_discovery
                        .seed_counterparties
                    ),
                }
            )

        assert len(summaries) == 5

        print(
            "Synthetic provider orchestrator "
            "smoke test passed."
        )
        print("Registered scenarios executed: 5")
        print("Provider loads per scenario: 1")
        print("Nine-dataset preflight per scenario: passed")
        print("Initial discovery per scenario: passed")
        print("Embedded graph outcomes: 0")
        print("Embedded AI decisions: 0")

        for summary in summaries:
            print(
                "{scenario_id}: groups={groups}, "
                "nodes={nodes}, edges={edges}, "
                "seed_counterparties={seed_counterparties}".format(
                    **summary
                )
            )

        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
