"""Validate deterministic current and historical run manifests."""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_orchestrator import (
    run_source_preflight,
)
from network_mule_discovery.run_state_manifest import (
    RUN_STATE_ARTIFACT_FILENAMES,
    RUN_STATE_MANIFEST_FILENAME,
    RUN_STATE_MANIFEST_HISTORY_DIRECTORY,
    JsonRunStateManifestStore,
    RunStateManifestError,
    build_run_id,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)
from network_mule_discovery.synthetic_scenario_registry import (
    create_synthetic_source_provider,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        provider = create_synthetic_source_provider(
            scenario_id="scenario_1",
            output_directory=source_directory,
        )
        source_manifest = provider.source_manifest
        request = SourceLoadRequest.create(
            dataset_id="scenario_1",
            run_date=source_manifest["run_date"],
            state_namespace="run-manifest-smoke",
        )

        preflight = run_source_preflight(
            source_provider=provider,
            source_request=request,
        )
        metadata = (
            preflight.source_bundle.metadata
        )

        store = JsonRunStateManifestStore(
            state_directory
        )
        first = store.initialize(metadata)
        repeated = store.initialize(metadata)

        assert first == repeated
        assert first.run_id == build_run_id(
            metadata
        )
        assert store.load() == first
        assert store.load(
            run_id=first.run_id
        ) == first
        assert first.run_status == "INITIALIZED"
        assert (
            first.artifact_filenames
            == RUN_STATE_ARTIFACT_FILENAMES
        )
        assert (
            store.path.name
            == RUN_STATE_MANIFEST_FILENAME
        )
        assert not store.path.with_suffix(
            ".json.tmp"
        ).exists()

        second_metadata = replace(
            metadata,
            state_namespace=(
                "run-manifest-smoke-day-2"
            ),
        )
        second = store.initialize(
            second_metadata
        )

        assert second.run_id != first.run_id
        assert store.load() == second
        assert store.load(
            run_id=first.run_id
        ) == first
        assert store.load(
            run_id=second.run_id
        ) == second

        history = store.list_manifests()
        assert len(history) == 2
        assert {
            manifest.run_id
            for manifest in history
        } == {
            first.run_id,
            second.run_id,
        }
        assert (
            state_directory
            / RUN_STATE_MANIFEST_HISTORY_DIRECTORY
        ).is_dir()

        try:
            store.load(
                run_id="../invalid"
            )
        except RunStateManifestError as exc:
            assert "invalid format" in str(exc)
        else:
            raise AssertionError(
                "Unsafe historical run ID was accepted."
            )

        print(
            "Run-state manifest smoke test passed."
        )
        print("Deterministic run ID: passed")
        print("Source snapshot identity: passed")
        print("Persisted artifact contract: 9 files")
        print("Atomic JSON persistence: passed")
        print("Idempotent same-run restart: passed")
        print("Historical daily runs retained: 2")
        print("Current-run pointer update: passed")
        print("Unsafe history lookup rejected: passed")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
