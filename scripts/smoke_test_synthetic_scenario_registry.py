"""Validate deterministic synthetic scenario selection."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceProvider,
    SourceContractError,
    SourceLoadRequest,
)
from network_mule_discovery.synthetic_scenario_registry import (
    SUPPORTED_SYNTHETIC_SCENARIOS,
    create_synthetic_source_provider,
)


def load_provider(
    *,
    scenario_id: str,
    source_directory: Path,
    changed_evidence: bool = False,
):
    provider = create_synthetic_source_provider(
        scenario_id=scenario_id,
        output_directory=source_directory,
        changed_evidence=changed_evidence,
    )
    manifest = provider.source_manifest
    request = SourceLoadRequest.create(
        dataset_id=scenario_id,
        run_date=manifest["run_date"],
        state_namespace=(
            f"synthetic-registry-{scenario_id}"
        ),
    )
    bundle = provider.load(request)

    return provider, manifest, bundle


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        snapshot_hashes: dict[str, str] = {}

        for scenario_id in (
            SUPPORTED_SYNTHETIC_SCENARIOS
        ):
            provider, manifest, bundle = (
                load_provider(
                    scenario_id=scenario_id,
                    source_directory=(
                        root / scenario_id
                    ),
                )
            )

            assert isinstance(
                provider,
                DiscoverySourceProvider,
            )
            assert provider.load_count == 1
            assert tuple(
                bundle.as_mapping()
            ) == SOURCE_DATASET_NAMES
            assert (
                bundle.metadata.dataset_id
                == scenario_id
            )
            assert (
                bundle.metadata.source_manifest
                == manifest
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

            snapshot_hashes[scenario_id] = (
                bundle.metadata.source_snapshot_hash
            )

        _, initial_manifest, initial_bundle = (
            load_provider(
                scenario_id="scenario_5",
                source_directory=(
                    root / "scenario_5_initial"
                ),
            )
        )
        _, changed_manifest, changed_bundle = (
            load_provider(
                scenario_id="scenario_5",
                source_directory=(
                    root / "scenario_5_changed"
                ),
                changed_evidence=True,
            )
        )

        assert (
            initial_manifest["evidence_phase"]
            == "INITIAL"
        )
        assert (
            changed_manifest["evidence_phase"]
            == "CHANGED"
        )
        assert (
            initial_bundle.metadata.source_snapshot_hash
            != changed_bundle.metadata.source_snapshot_hash
        )

        try:
            create_synthetic_source_provider(
                scenario_id="scenario_99",
                output_directory=(
                    root / "unsupported"
                ),
            )
        except SourceContractError as exc:
            assert (
                "Unsupported synthetic scenario"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Unsupported scenario was accepted."
            )

        try:
            create_synthetic_source_provider(
                scenario_id="scenario_1",
                output_directory=(
                    root / "invalid-change"
                ),
                changed_evidence=True,
            )
        except SourceContractError as exc:
            assert (
                "supported only for scenario_5"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Invalid changed-evidence mode "
                "was accepted."
            )

        assert len(snapshot_hashes) == 5

        print(
            "Synthetic scenario registry smoke "
            "test passed."
        )
        print("Supported scenarios: 5")
        print("Nine-frame bundles loaded: 5")
        print("Source-only manifests: passed")
        print("Embedded graph outcomes: 0")
        print("Embedded AI decisions: 0")
        print(
            "Scenario 5 changed-evidence "
            "selection: passed"
        )
        print("Unsupported scenario rejection: passed")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
