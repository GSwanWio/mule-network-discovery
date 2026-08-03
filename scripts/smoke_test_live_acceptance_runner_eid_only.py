"""Validate zero-AI EID-only acceptance execution."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from network_mule_discovery.live_acceptance_runner import (
    run_live_acceptance_case,
)


def forbidden_factory() -> object:
    """Reject unexpected AI adapter creation."""
    raise AssertionError(
        "EID-only acceptance instantiated "
        "an AI adapter."
    )


def artifact_bytes(
    directory: Path,
) -> dict[str, bytes]:
    """Read retained artifacts byte-for-byte."""
    return {
        path.name: path.read_bytes()
        for path in directory.iterdir()
        if path.is_file()
    }


def main() -> None:
    """Validate execution and immutable reuse."""
    with TemporaryDirectory() as directory:
        workspace = Path(directory)

        first = run_live_acceptance_case(
            scenario_id="scenario_4",
            workspace_directory=workspace,
            execute_live_ai=False,
            daily_call_limit=0,
            reset_state=True,
            counterparty_adapter_factory=(
                forbidden_factory
            ),
            customer_adapter_factory=(
                forbidden_factory
            ),
        )

        state = first.snapshot.daily_state
        network = state.network

        assert not first.reused_existing_run
        assert first.breadth_first_result is not None
        assert first.provider_load_count == 1

        assert first.calls_before_run == 0
        assert first.calls_executed == 0
        assert first.calls_after_run == 0

        assert first.snapshot.ai_call_ledger.empty
        assert state.decision_store.empty
        assert state.frontier_queue.empty

        assert (
            first.snapshot.manifest.run_status
            == "TERMINATED"
        )
        assert (
            first.snapshot.manifest
            .termination_reason
            == "FRONTIER_EXHAUSTED"
        )

        assert (
            len(network.groups),
            len(network.nodes),
            len(network.edges),
        ) == (2, 5, 3)

        before_artifacts = artifact_bytes(
            first.snapshot.artifact_directory
        )

        repeated = run_live_acceptance_case(
            scenario_id="scenario_4",
            workspace_directory=workspace,
            execute_live_ai=False,
            daily_call_limit=0,
            reset_state=False,
            counterparty_adapter_factory=(
                forbidden_factory
            ),
            customer_adapter_factory=(
                forbidden_factory
            ),
        )

        assert repeated.run_id == first.run_id
        assert repeated.reused_existing_run
        assert repeated.breadth_first_result is None

        assert repeated.calls_before_run == 0
        assert repeated.calls_executed == 0
        assert repeated.calls_after_run == 0

        after_artifacts = artifact_bytes(
            repeated.snapshot
            .artifact_directory
        )

        assert (
            after_artifacts
            == before_artifacts
        )

        print(
            "EID-only live acceptance runner "
            "smoke test passed."
        )
        print("Scenario executed: scenario_4")
        print(
            "EID customer AI assessment: disabled"
        )
        print("AI calls executed: 0")
        print("Persisted decisions: 0")
        print("Persisted graph: 2/5/3")
        print(
            "Termination reason: "
            "FRONTIER_EXHAUSTED"
        )
        print("Immutable rerun: passed")
        print("AI adapters instantiated: 0")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
