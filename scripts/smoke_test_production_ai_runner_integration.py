"""Validate production runtime preflight in the default AI path."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = ROOT / "scripts"

for path in (
    ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
    run_controlled_daily_ai,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionAdapter,
)
from network_mule_discovery.production_ai_runtime import (
    PRODUCTION_AI_RUNTIME_FILENAME,
    PRODUCTION_AI_STARTUP_FAILURE_FILENAME,
    JsonProductionAiRuntimeStore,
    JsonProductionAiStartupFailureStore,
    ProductionAiStartupError,
)
from network_mule_discovery.run_state_manifest import (
    RUN_STATE_ARTIFACT_FILENAMES,
)
from smoke_test_controlled_daily_ai_runner import (
    SuppressingAdapter,
)
from smoke_test_daily_changed_evidence import (
    add_day_two_counterparty_evidence,
    select_source_customer,
)
from smoke_test_daily_incremental_state import (
    DAY_ONE,
    DAY_TWO,
    run_day_one,
)
from smoke_test_incremental_fail_closed import (
    select_counterparties,
)


def main() -> None:
    day_one_result = run_day_one()

    environment = {
        "OPENAI_API_KEY": "synthetic-integration-key",
        "OPENAI_MODEL": "synthetic-model-v1",
        "OPENAI_PROMPT_VERSION": (
            "synthetic-prompt-v1"
        ),
        "OPENAI_TIMEOUT_SECONDS": "30",
        "OPENAI_MAX_OUTPUT_TOKENS": "2500",
    }

    with (
        TemporaryDirectory() as directory,
        patch.dict(
            "os.environ",
            environment,
            clear=False,
        ),
    ):
        state_directory = Path(directory)
        state_store = CsvDailyStateStore(
            state_directory
        )

        state_store.commit_recursive_result(
            result=day_one_result,
            run_date=DAY_ONE,
        )

        snapshot = state_store.load_snapshot()
        selected = select_counterparties(
            snapshot.network.nodes
        )
        changed_network = snapshot.network

        for counterparty in selected.to_dict(
            orient="records"
        ):
            counterparty_row = pd.Series(
                counterparty
            )
            source_customer = (
                select_source_customer(
                    nodes=changed_network.nodes,
                    group_id=str(
                        counterparty_row[
                            "group_id"
                        ]
                    ),
                )
            )
            changed_network = (
                add_day_two_counterparty_evidence(
                    network=changed_network,
                    counterparty_node=(
                        counterparty_row
                    ),
                    source_customer=(
                        source_customer
                    ),
                )
            )

        state_store.save_network_state(
            network=changed_network,
            run_date=DAY_TWO,
        )

        adapter = SuppressingAdapter()

        with patch.object(
            OpenAIDecisionAdapter,
            "from_environment",
            return_value=adapter,
        ):
            result = run_controlled_daily_ai(
                state_directory=state_directory,
                run_date=DAY_TWO,
                settings=DailyAiSettings(
                    live_ai_enabled=True,
                    daily_call_limit=1,
                    run_call_limit=1,
                ),
            )

        assert result.calls_executed == 1
        assert len(adapter.calls) == 1

        runtime_store = (
            JsonProductionAiRuntimeStore(
                state_directory
            )
        )
        runtime = runtime_store.load()

        assert runtime.live_ai_enabled
        assert runtime.daily_call_limit == 1
        assert runtime.run_call_limit == 1
        assert runtime.model == (
            "synthetic-model-v1"
        )
        assert runtime.prompt_version == (
            "synthetic-prompt-v1"
        )
        assert runtime.timeout_seconds == 30
        assert runtime.max_output_tokens == 2500
        assert (
            PRODUCTION_AI_RUNTIME_FILENAME
            in RUN_STATE_ARTIFACT_FILENAMES
        )
        assert len(
            RUN_STATE_ARTIFACT_FILENAMES
        ) == 11

        runtime_text = (
            runtime_store.path.read_text(
                encoding="utf-8"
            )
        )
        assert "synthetic-integration-key" not in runtime_text
        assert "api_key" not in runtime_text

        ledger = CsvAiCallLedger(
            state_directory
        ).load()

        assert len(ledger) == 1
        assert (
            ledger.iloc[0]["call_status"]
            == "COMPLETED"
        )

        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "",
            },
            clear=False,
        ):
            try:
                run_controlled_daily_ai(
                    state_directory=state_directory,
                    run_date=DAY_TWO,
                    settings=DailyAiSettings(
                        live_ai_enabled=True,
                        daily_call_limit=2,
                        run_call_limit=1,
                    ),
                )
            except ProductionAiStartupError as exc:
                assert (
                    "Production live-AI startup failed"
                    in str(exc)
                )
            else:
                raise AssertionError(
                    "Missing API configuration did not "
                    "fail production startup."
                )

        startup_failure_store = (
            JsonProductionAiStartupFailureStore(
                state_directory
            )
        )
        startup_failure = (
            startup_failure_store.load()
        )

        assert (
            startup_failure.error_code
            == "PRODUCTION_AI_STARTUP_FAILED"
        )
        assert (
            startup_failure.error_type
            == "ProductionAiRuntimeError"
        )
        assert (
            PRODUCTION_AI_STARTUP_FAILURE_FILENAME
            in RUN_STATE_ARTIFACT_FILENAMES
        )
        assert len(
            RUN_STATE_ARTIFACT_FILENAMES
        ) == 11

        failure_text = (
            startup_failure_store.path.read_text(
                encoding="utf-8"
            )
        )
        assert "synthetic-integration-key" not in failure_text
        assert (
            len(
                CsvAiCallLedger(
                    state_directory
                ).load()
            )
            == 1
        )

        print(
            "Production AI runner integration "
            "smoke test passed."
        )
        print("Default production path selected: passed")
        print("Runtime preflight before adapter: passed")
        print("Runtime identity persisted: passed")
        print("Custom adapter received calls: 1")
        print("AI call ledger rows: 1")
        print("Startup failure audit: passed")
        print("Startup failure consumed calls: 0")
        print("API key persisted: 0")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
