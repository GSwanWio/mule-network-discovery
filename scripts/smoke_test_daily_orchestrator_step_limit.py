"""Validate bounded orchestration stops at the configured step limit."""

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
from smoke_test_scenario_1_customer_frontier import (
    CustomerDecisionAdapter,
    append_counterparty_decisions,
)
from smoke_test_scenario_1_recursive_counterparty_frontier import (
    RecursiveCounterpartyAdapter,
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
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)


def forbidden_factory() -> object:
    raise AssertionError(
        "The step-limited run attempted a later AI phase."
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        manifest = generate_scenario_1_source_data(
            source_directory
        )

        request = SourceLoadRequest.create(
            dataset_id="scenario_1",
            run_date=manifest["run_date"],
            state_namespace=(
                "step-limit-orchestrator-smoke"
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

        counterparty_phase = run_counterparty_ai_phase(
            initial_discovery=initial_discovery,
            state_directory=state_directory,
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=20,
                run_call_limit=20,
            ),
            reset_state=True,
            adapter_factory=forbidden_factory,
        )

        state_store = CsvDailyStateStore(
            state_directory
        )

        append_counterparty_decisions(
            state_store=state_store,
            counterparty_payloads=(
                counterparty_phase.counterparty_payloads
            ),
        )

        customer_adapter = CustomerDecisionAdapter()

        customer_phase = run_customer_ai_phase(
            counterparty_phase=counterparty_phase,
            state_directory=state_directory,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=20,
                run_call_limit=5,
            ),
            adapter_factory=lambda: customer_adapter,
        )

        recursive_counterparty_adapter = (
            RecursiveCounterpartyAdapter()
        )

        result = run_breadth_first_frontier(
            customer_phase=customer_phase,
            state_directory=state_directory,
            ai_settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=20,
                run_call_limit=10,
            ),
            breadth_first_settings=(
                DailyBreadthFirstSettings(
                    max_frontier_steps=1
                )
            ),
            counterparty_adapter_factory=(
                lambda: recursive_counterparty_adapter
            ),
            customer_adapter_factory=(
                forbidden_factory
            ),
        )

        assert provider.load_count == 1
        assert len(customer_adapter.calls) == 5
        assert len(
            recursive_counterparty_adapter.calls
        ) == 1

        assert [
            step.selection.action_type
            for step in result.steps
        ] == [
            "DISCOVER_CUSTOMER_RELATIONSHIPS",
        ]

        assert result.termination_status == "STOPPED"
        assert result.termination_reason == (
            "MAX_FRONTIER_STEPS_REACHED"
        )
        assert result.frontier_termination is None
        assert result.recursive_termination is None
        assert (
            result.final_plan.failed_closed_item_count
            == 0
        )
        assert not result.final_plan.actionable_queue.empty
        assert result.final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI").sum() == 3

        print(
            "Daily orchestrator step-limit "
            "smoke test passed."
        )
        print("Provider load count: 1")
        print("Initial customer AI calls: 5")
        print("Recursive counterparty AI calls: 1")
        print("Frontier steps completed: 1")
        print("Remaining customer AI actions: 3")
        print("Failed frontier count: 0")
        print("Termination status: STOPPED")
        print(
            "Termination reason: "
            "MAX_FRONTIER_STEPS_REACHED"
        )
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
