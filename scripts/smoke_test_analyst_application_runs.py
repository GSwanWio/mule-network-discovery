"""Validate analyst access to persisted runs."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_application_state import (
    ANALYST_RUN_TABLE_COLUMNS,
    AnalystApplicationStateError,
    AnalystApplicationStateStore,
)
from network_mule_discovery.consolidated_state import (
    ConsolidatedStateStore,
)
from network_mule_discovery.daily_orchestrator import (
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)
from network_mule_discovery.synthetic_scenario_registry import (
    create_synthetic_source_provider,
)


def build_run(
    *,
    provider: object,
    state_store: ConsolidatedStateStore,
    state_namespace: str,
    run_status: str,
    termination_status: str = "",
    termination_reason: str = "",
) -> str:
    """Create one persisted synthetic run."""
    source_manifest = provider.source_manifest

    request = SourceLoadRequest.create(
        dataset_id="scenario_1",
        run_date=source_manifest["run_date"],
        state_namespace=state_namespace,
    )
    preflight = run_source_preflight(
        source_provider=provider,
        source_request=request,
    )
    discovery = run_initial_discovery(
        source_preflight=preflight,
    )
    manifest = state_store.initialize(
        preflight.source_bundle.metadata
    )

    state_store.daily_state.save_network_state(
        network=discovery.unified_groups,
        run_date=request.run_date,
    )
    state_store.daily_state.save_frontier_queue(
        pd.DataFrame()
    )
    state_store.update_run_status(
        run_status=run_status,
        termination_status=termination_status,
        termination_reason=termination_reason,
    )

    if run_status in {
        "STOPPED",
        "TERMINATED",
        "FAILED",
    }:
        state_store.snapshot_current_run()

    return manifest.run_id


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        provider = create_synthetic_source_provider(
            scenario_id="scenario_1",
            output_directory=source_directory,
        )
        state_store = ConsolidatedStateStore(
            state_directory
        )

        historical_run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="analyst-history",
            run_status="STOPPED",
            termination_status="STOPPED",
            termination_reason=(
                "MAX_FRONTIER_STEPS_REACHED"
            ),
        )
        current_run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="analyst-current",
            run_status="RUNNING",
        )

        application = (
            AnalystApplicationStateStore(
                state_directory
            )
        )
        summaries = application.list_runs()

        assert len(summaries) == 2
        assert summaries[0].run_id == current_run_id
        assert summaries[0].is_current
        assert summaries[0].run_status == "RUNNING"

        historical_summary = next(
            summary
            for summary in summaries
            if summary.run_id
            == historical_run_id
        )

        assert not historical_summary.is_current
        assert (
            historical_summary.run_status
            == "STOPPED"
        )
        assert (
            historical_summary
            .termination_reason
            == "MAX_FRONTIER_STEPS_REACHED"
        )

        run_table = application.run_table()

        assert tuple(
            run_table.columns
        ) == ANALYST_RUN_TABLE_COLUMNS
        assert len(run_table) == 2

        current = application.load_run(
            current_run_id
        )
        historical = application.load_run(
            historical_run_id
        )

        assert (
            current.manifest.run_id
            == current_run_id
        )
        assert (
            historical.manifest.run_id
            == historical_run_id
        )
        assert not (
            current.daily_state.network.groups.empty
        )
        assert not (
            historical
            .daily_state
            .network
            .groups
            .empty
        )

        try:
            application.load_run(
                "RUN_00000000000000000000"
            )
        except AnalystApplicationStateError:
            pass
        else:
            raise AssertionError(
                "Unknown run was accepted."
            )

        print(
            "Analyst application run catalogue "
            "smoke test passed."
        )
        print("Persisted runs listed: 2")
        print("Current run first: passed")
        print("Historical run loading: passed")
        print("Artifact inventory exposed: passed")
        print("Unknown run rejected: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
