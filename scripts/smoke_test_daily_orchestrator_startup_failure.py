"""Validate orchestrator finalization after AI startup failure."""

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
    run_counterparty_ai_phase,
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.production_ai_runtime import (
    PRODUCTION_AI_STARTUP_FAILURE_FILENAME,
    ProductionAiStartupError,
)
from network_mule_discovery.run_state_manifest import (
    RUN_STATE_ARTIFACT_HISTORY_DIRECTORY,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    RUN_DATE,
    generate_scenario_5_source_data,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)
from smoke_test_daily_orchestrator_initial_discovery import (
    ScenarioOneProvider,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        source_manifest = (
            generate_scenario_5_source_data(
                source_directory
            )
        )
        request = SourceLoadRequest.create(
            dataset_id="scenario_5",
            run_date=RUN_DATE,
            state_namespace=(
                "startup-failure-orchestrator-smoke"
            ),
        )
        provider = ScenarioOneProvider(
            source_directory=source_directory,
            source_manifest=source_manifest,
        )

        preflight = run_source_preflight(
            source_provider=provider,
            source_request=request,
        )
        initial_discovery = run_initial_discovery(
            source_preflight=preflight,
        )

        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "",
            },
            clear=False,
        ):
            try:
                run_counterparty_ai_phase(
                    initial_discovery=(
                        initial_discovery
                    ),
                    state_directory=state_directory,
                    settings=DailyAiSettings(
                        live_ai_enabled=True,
                        daily_call_limit=5,
                        run_call_limit=1,
                    ),
                    reset_state=True,
                )
            except ProductionAiStartupError:
                pass
            else:
                raise AssertionError(
                    "Production startup failure "
                    "did not escape the orchestrator."
                )

        store = ConsolidatedStateStore(
            state_directory
        )
        current = store.manifest.load()
        historical = store.manifest.load(
            run_id=current.run_id
        )

        for manifest in (
            current,
            historical,
        ):
            assert manifest.run_status == "FAILED"
            assert (
                manifest.termination_status
                == "FAILED"
            )
            assert (
                manifest.termination_reason
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
        assert CsvAiCallLedger(
            state_directory
        ).load().empty

        print(
            "Daily orchestrator startup-failure "
            "smoke test passed."
        )
        print("Manifest status: FAILED")
        print(
            "Termination reason: "
            "PRODUCTION_AI_STARTUP_FAILED"
        )
        print("Historical failure artifact: passed")
        print("AI calls consumed: 0")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
