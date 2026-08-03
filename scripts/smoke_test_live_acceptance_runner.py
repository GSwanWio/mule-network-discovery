"""Validate the Step 8 production-path acceptance runner."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from network_mule_discovery.live_acceptance_runner import (
    run_live_acceptance_case,
)
from smoke_test_scenario_5_live_counterparty_decision import (
    Scenario5InsufficientEvidenceAdapter,
)


def forbidden_factory() -> object:
    """Reject any unexpected AI adapter creation."""
    raise AssertionError(
        "Unchanged or exhausted work instantiated "
        "an AI adapter."
    )


def main() -> None:
    """Validate initial execution and unchanged reuse."""
    with TemporaryDirectory() as directory:
        workspace = Path(directory)
        adapter = (
            Scenario5InsufficientEvidenceAdapter()
        )

        first = run_live_acceptance_case(
            scenario_id="scenario_5",
            workspace_directory=workspace,
            execute_live_ai=True,
            daily_call_limit=5,
            reset_state=True,
            counterparty_adapter_factory=(
                lambda: adapter
            ),
            customer_adapter_factory=(
                forbidden_factory
            ),
        )

        assert len(adapter.calls) == 1
        assert first.provider_load_count == 1
        assert first.calls_before_run == 0
        assert first.calls_executed == 1
        assert first.calls_after_run == 1
        assert not first.reused_existing_run
        assert first.breadth_first_result is not None
        assert (
            first.snapshot.manifest.run_status
            == "TERMINATED"
        )
        assert (
            first.snapshot.manifest
            .termination_reason
            == "FRONTIER_EXHAUSTED"
        )
        assert len(
            first.snapshot
            .daily_state
            .network
            .groups
        ) == 1
        assert len(
            first.snapshot
            .daily_state
            .network
            .nodes
        ) == 4
        assert len(
            first.snapshot
            .daily_state
            .network
            .edges
        ) == 3
        assert set(
            first.snapshot
            .daily_state
            .decision_store[
                "decision"
            ]
        ) == {
            "INSUFFICIENT_EVIDENCE_SUPPRESS"
        }
        assert first.snapshot.artifact_directory.name == (
            first.run_id
        )

        artifact_bytes_before = {
            path.name: path.read_bytes()
            for path
            in first.snapshot
            .artifact_directory
            .iterdir()
            if path.is_file()
        }

        repeated = run_live_acceptance_case(
            scenario_id="scenario_5",
            workspace_directory=workspace,
            execute_live_ai=True,
            daily_call_limit=5,
            reset_state=False,
            counterparty_adapter_factory=(
                forbidden_factory
            ),
            customer_adapter_factory=(
                forbidden_factory
            ),
        )

        assert repeated.run_id == first.run_id
        assert repeated.calls_before_run == 1
        assert repeated.calls_executed == 0
        assert repeated.calls_after_run == 1
        assert repeated.reused_existing_run
        assert repeated.breadth_first_result is None
        assert len(
            repeated.snapshot.ai_call_ledger
        ) == 1
        assert len(
            repeated.snapshot
            .daily_state
            .decision_store
        ) == 1
        assert (
            repeated.snapshot
            .daily_state
            .frontier_queue
            .empty
        )

        artifact_bytes_after = {
            path.name: path.read_bytes()
            for path
            in repeated.snapshot
            .artifact_directory
            .iterdir()
            if path.is_file()
        }

        assert (
            artifact_bytes_after
            == artifact_bytes_before
        )

        print(
            "Live acceptance runner smoke test passed."
        )
        print("Scenario executed: scenario_5")
        print("Production-path phases: passed")
        print("Initial AI calls: 1")
        print("Persisted graph: 1/4/3")
        print(
            "Termination reason: FRONTIER_EXHAUSTED"
        )
        print("Historical snapshot loaded: passed")
        print("Unchanged rerun AI calls: 0")
        print("Finalized-run reuse barrier: passed")
        print("Historical artifacts unchanged: passed")
        print("Stable run ID: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
