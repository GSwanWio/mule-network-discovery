"""Test the production-path customer AI phase offline."""

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
    append_counterparty_decisions,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_orchestrator import (
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
        "Planning-only orchestration created a live adapter."
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
                "customer-orchestrator-smoke"
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
                daily_call_limit=10,
                run_call_limit=10,
            ),
            reset_state=True,
            adapter_factory=forbidden_factory,
        )

        try:
            run_customer_ai_phase(
                counterparty_phase=counterparty_phase,
                state_directory=state_directory,
                settings=DailyAiSettings(
                    live_ai_enabled=False,
                    daily_call_limit=10,
                    run_call_limit=10,
                ),
                adapter_factory=forbidden_factory,
            )
        except RuntimeError as exc:
            assert (
                "counterparty decisions remain unresolved"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Customer phase started before "
                "counterparty decisions closed."
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

        result = run_customer_ai_phase(
            counterparty_phase=counterparty_phase,
            state_directory=state_directory,
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=10,
                run_call_limit=10,
            ),
            adapter_factory=forbidden_factory,
        )

        customer_queue = (
            result
            .customer_frontier
            .controlled_run
            .initial_plan
            .actionable_queue
        )

        customer_queue = customer_queue.loc[
            customer_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI")
        ]

        unresolved_counterparties = (
            result
            .customer_frontier
            .controlled_run
            .initial_plan
            .actionable_queue
        )

        unresolved_counterparties = (
            unresolved_counterparties.loc[
                unresolved_counterparties[
                    "action_type"
                ].eq("RUN_COUNTERPARTY_AI")
            ]
        )

        assert provider.load_count == 1
        assert len(
            counterparty_phase.counterparty_payloads
        ) == 2
        assert len(result.customer_payloads) == 5
        assert len(
            result.supplemental_subject_payloads
        ) == 7
        assert set(
            result.customer_payloads[
                "subject_type"
            ]
        ) == {"CUSTOMER"}
        assert len(customer_queue) == 5
        assert unresolved_counterparties.empty
        assert (
            result
            .customer_frontier
            .controlled_run
            .calls_executed
            == 0
        )

        print(
            "Daily orchestrator customer AI "
            "smoke test passed."
        )
        print("Provider load count: 1")
        print("Counterparty barrier before decisions: passed")
        print("Counterparty payloads: 2")
        print("Customer payloads: 5")
        print("Combined supplemental payloads: 7")
        print("Customer AI actions queued: 5")
        print("Unresolved counterparty actions: 0")
        print("Planning-only adapter calls: 0")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
