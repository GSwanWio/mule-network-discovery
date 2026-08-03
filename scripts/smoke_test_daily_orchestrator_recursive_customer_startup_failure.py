"""Validate recursive-customer startup failure finalization."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = ROOT / "scripts"

for path in (
    ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from network_mule_discovery.consolidated_state import (
    ConsolidatedStateStore,
)
from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
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
from network_mule_discovery.production_ai_runtime import (
    PRODUCTION_AI_STARTUP_FAILURE_FILENAME,
    ProductionAiStartupError,
)
from network_mule_discovery.run_state_manifest import (
    RUN_STATE_ARTIFACT_HISTORY_DIRECTORY,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)
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


def forbidden_factory() -> object:
    raise AssertionError(
        "Planning-only counterparty phase created an adapter."
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
                "recursive-customer-startup-failure"
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
        counterparty_phase = (
            run_counterparty_ai_phase(
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
        )

        append_counterparty_decisions(
            state_store=CsvDailyStateStore(
                state_directory
            ),
            counterparty_payloads=(
                counterparty_phase
                .counterparty_payloads
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

        ledger_before = CsvAiCallLedger(
            state_directory
        ).load()

        assert len(ledger_before) == 5

        recursive_counterparty_adapter = (
            RecursiveCounterpartyAdapter()
        )

        with patch.dict(
            "os.environ",
            {"OPENAI_API_KEY": ""},
            clear=False,
        ):
            try:
                run_breadth_first_frontier(
                    customer_phase=customer_phase,
                    state_directory=state_directory,
                    ai_settings=DailyAiSettings(
                        live_ai_enabled=True,
                        daily_call_limit=20,
                        run_call_limit=10,
                    ),
                    breadth_first_settings=(
                        DailyBreadthFirstSettings(
                            max_frontier_steps=10
                        )
                    ),
                    counterparty_adapter_factory=(
                        lambda: recursive_counterparty_adapter
                    ),
                )
            except ProductionAiStartupError:
                pass
            else:
                raise AssertionError(
                    "Recursive customer startup failure "
                    "did not escape the orchestrator."
                )

        store = ConsolidatedStateStore(
            state_directory
        )
        current = store.manifest.load()
        historical = store.manifest.load(
            run_id=current.run_id
        )

        for run_manifest in (
            current,
            historical,
        ):
            assert run_manifest.run_status == "FAILED"
            assert (
                run_manifest.termination_status
                == "FAILED"
            )
            assert (
                run_manifest.termination_reason
                == "PRODUCTION_AI_STARTUP_FAILED"
            )

        current_failure = (
            state_directory
            / PRODUCTION_AI_STARTUP_FAILURE_FILENAME
        )
        historical_failure = (
            state_directory
            / RUN_STATE_ARTIFACT_HISTORY_DIRECTORY
            / current.run_id
            / PRODUCTION_AI_STARTUP_FAILURE_FILENAME
        )

        assert current_failure.is_file()
        assert historical_failure.is_file()

        ledger_after = CsvAiCallLedger(
            state_directory
        ).load()

        assert len(customer_adapter.calls) == 5
        assert len(
            recursive_counterparty_adapter.calls
        ) == 1
        assert len(ledger_after) == 6

        print(
            "Recursive customer startup-failure "
            "smoke test passed."
        )
        print("Manifest status: FAILED")
        print(
            "Termination reason: "
            "PRODUCTION_AI_STARTUP_FAILED"
        )
        print("Historical failure artifact: passed")
        print("Initial customer AI calls retained: 5")
        print("Recursive counterparty calls retained: 1")
        print("Startup failure consumed calls: 0")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
