"""Validate unified access to existing persisted state stores."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.consolidated_state import (
    ConsolidatedStateStore,
)
from network_mule_discovery.daily_orchestrator import (
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.run_state_manifest import (
    RunStateManifestError,
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
            state_namespace=(
                "consolidated-state-smoke"
            ),
        )

        preflight = run_source_preflight(
            source_provider=provider,
            source_request=request,
        )
        discovery = run_initial_discovery(
            source_preflight=preflight,
        )

        store = ConsolidatedStateStore(
            state_directory
        )
        initialized = store.initialize(
            preflight.source_bundle.metadata
        )

        store.daily_state.save_network_state(
            network=discovery.unified_groups,
            run_date=request.run_date,
        )
        store.daily_state.save_frontier_queue(
            pd.DataFrame()
        )

        running = store.update_run_status(
            run_status="RUNNING"
        )
        assert running.run_id == initialized.run_id
        assert running.run_status == "RUNNING"

        snapshot = store.load()

        assert (
            snapshot.manifest.run_id
            == initialized.run_id
        )
        assert not snapshot.daily_state.network.groups.empty
        assert not snapshot.daily_state.network.nodes.empty
        assert snapshot.daily_state.decision_store.empty
        assert snapshot.daily_state.expansion_ledger.empty
        assert snapshot.daily_state.frontier_queue.empty
        assert snapshot.ai_call_ledger.empty
        assert (
            snapshot
            .technical_reprocessing_ledger
            .empty
        )
        assert (
            snapshot.operational_resilience_report
            is None
        )

        assert snapshot.artifact_presence[
            "network_state_groups.csv"
        ]
        assert snapshot.artifact_presence[
            "network_state_nodes.csv"
        ]
        assert snapshot.artifact_presence[
            "network_state_edges.csv"
        ]
        assert snapshot.artifact_presence[
            "frontier_queue.csv"
        ]
        assert (
            "decision_store.csv"
            in snapshot.missing_artifacts
        )
        assert (
            "ai_call_ledger.csv"
            in snapshot.missing_artifacts
        )

        stopped = store.update_run_status(
            run_status="STOPPED",
            termination_status="STOPPED",
            termination_reason=(
                "MAX_FRONTIER_STEPS_REACHED"
            ),
        )
        assert stopped.run_id == initialized.run_id
        assert stopped.run_status == "STOPPED"
        assert (
            stopped.termination_reason
            == "MAX_FRONTIER_STEPS_REACHED"
        )

        artifact_snapshot = (
            store.snapshot_current_run()
        )
        historical = store.load(
            run_id=stopped.run_id
        )

        assert artifact_snapshot.is_dir()
        assert (
            historical.artifact_directory
            == artifact_snapshot
        )
        assert historical.manifest == stopped
        assert not (
            historical
            .daily_state
            .network
            .groups
            .empty
        )
        assert not (
            historical
            .daily_state
            .network
            .nodes
            .empty
        )
        assert historical.artifact_presence[
            "network_state_groups.csv"
        ]

        try:
            store.update_run_status(
                run_status="TERMINATED",
            )
        except RunStateManifestError as exc:
            assert (
                "termination_reason"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Terminal status without a reason "
                "was accepted."
            )

        print(
            "Consolidated state facade smoke test passed."
        )
        print("Manifest access: passed")
        print("Graph state access: passed")
        print("Decision and frontier access: passed")
        print("AI call ledger access: passed")
        print("Technical reprocessing access: passed")
        print("Artifact presence inventory: passed")
        print("Historical artifact snapshot: passed")
        print("Restart-stable status update: passed")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
