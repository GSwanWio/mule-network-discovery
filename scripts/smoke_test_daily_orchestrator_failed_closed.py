"""Validate unified orchestration stops on persisted failed AI work."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = ROOT / "scripts"

for path in (ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from smoke_test_daily_orchestrator_initial_discovery import (
    ScenarioOneProvider,
)
from smoke_test_scenario_5_live_counterparty_decision import (
    Scenario5FailureAdapter,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_orchestrator import (
    DailyBreadthFirstSettings,
    run_breadth_first_frontier,
    run_counterparty_ai_phase,
    run_customer_ai_phase,
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    RUN_DATE,
    generate_scenario_5_source_data,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)


def forbidden_factory() -> object:
    raise AssertionError(
        "Failed-closed state attempted another AI call."
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        manifest = generate_scenario_5_source_data(
            source_directory
        )

        request = SourceLoadRequest.create(
            dataset_id="scenario_5",
            run_date=RUN_DATE,
            state_namespace=(
                "failed-closed-orchestrator-smoke"
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

        initial_discovery = run_initial_discovery(
            source_preflight=preflight,
        )

        failure_adapter = Scenario5FailureAdapter()

        counterparty_phase = run_counterparty_ai_phase(
            initial_discovery=initial_discovery,
            state_directory=state_directory,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=5,
                run_call_limit=1,
            ),
            reset_state=True,
            adapter_factory=lambda: failure_adapter,
        )

        counterparty_plan = (
            counterparty_phase
            .counterparty_frontier
            .controlled_run
            .final_plan
        )

        assert len(failure_adapter.calls) == 1
        assert counterparty_plan.failed_closed_item_count == 1
        assert (
            counterparty_phase
            .counterparty_frontier
            .decision_store
            .empty
        )

        customer_phase = run_customer_ai_phase(
            counterparty_phase=counterparty_phase,
            state_directory=state_directory,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=5,
                run_call_limit=5,
            ),
            adapter_factory=forbidden_factory,
        )

        assert customer_phase.customer_payloads.empty
        assert (
            customer_phase
            .customer_frontier
            .controlled_run
            .calls_executed
            == 0
        )

        result = run_breadth_first_frontier(
            customer_phase=customer_phase,
            state_directory=state_directory,
            ai_settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=5,
                run_call_limit=5,
            ),
            breadth_first_settings=(
                DailyBreadthFirstSettings(
                    max_frontier_steps=3
                )
            ),
            counterparty_adapter_factory=(
                forbidden_factory
            ),
            customer_adapter_factory=(
                forbidden_factory
            ),
        )

        assert provider.load_count == 1
        assert [
            step.selection.action_type
            for step in result.steps
        ] == ["FAIL_CLOSED"]

        assert result.termination_status == "STOPPED"
        assert result.termination_reason == (
            "FAILED_CLOSED_FRONTIER"
        )
        assert result.final_plan.actionable_queue.empty
        assert result.final_plan.failed_closed_item_count == 1
        assert result.frontier_termination is None
        assert result.recursive_termination is None

        print(
            "Daily orchestrator failed-closed "
            "smoke test passed."
        )
        print("Provider load count: 1")
        print("Failed counterparty AI calls: 1")
        print("Persisted failed frontier count: 1")
        print("Customer payloads created: 0")
        print("Customer AI calls: 0")
        print("Additional frontier AI calls: 0")
        print("Termination status: STOPPED")
        print("Termination reason: FAILED_CLOSED_FRONTIER")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
