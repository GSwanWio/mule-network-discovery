"""Validate the non-billable live acceptance preflight."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from network_mule_discovery.live_acceptance_preflight import (
    LiveAcceptancePreflightError,
    run_live_acceptance_preflight,
)


VALID_ENVIRONMENT = {
    "ALLOW_LIVE_SYNTHETIC_ACCEPTANCE": "1",
    "MULE_NETWORK_ENABLE_LIVE_AI": "1",
    "MULE_NETWORK_DAILY_AI_CALL_LIMIT": "15",
    "MULE_NETWORK_RUN_AI_CALL_LIMIT": "11",
    "OPENAI_API_KEY": "synthetic-preflight-key",
    "OPENAI_MODEL": "synthetic-preflight-model",
    "OPENAI_PROMPT_VERSION": (
        "synthetic-preflight-prompt-v1"
    ),
    "OPENAI_TIMEOUT_SECONDS": "45",
    "OPENAI_MAX_OUTPUT_TOKENS": "4000",
}


def _expect_failure(
    environment: dict[str, str],
    expected_text: str,
) -> None:
    with patch.dict(
        "os.environ",
        environment,
        clear=True,
    ):
        try:
            run_live_acceptance_preflight()
        except LiveAcceptancePreflightError as exc:
            assert expected_text in str(exc)
        else:
            raise AssertionError(
                "Unsafe live acceptance configuration "
                "was not rejected."
            )


def main() -> None:
    """Validate authorization, budgets, and runtime identity."""
    with patch.dict(
        "os.environ",
        VALID_ENVIRONMENT,
        clear=True,
    ):
        result = run_live_acceptance_preflight()

    assert result.scenario_ids == (
        "scenario_1",
        "scenario_2",
        "scenario_3",
        "scenario_4",
        "scenario_5",
    )
    assert result.scenario_count == 5
    assert result.maximum_initial_live_calls == 15
    assert result.maximum_scenario_live_calls == 11
    assert result.settings.daily_call_limit == 15
    assert result.settings.run_call_limit == 11
    assert result.runtime.model == (
        "synthetic-preflight-model"
    )
    assert result.runtime.prompt_version == (
        "synthetic-preflight-prompt-v1"
    )
    assert result.runtime.sdk_package == "openai"
    assert result.runtime.sdk_version
    assert (
        "synthetic-preflight-key"
        not in str(result)
    )

    unauthorized = dict(VALID_ENVIRONMENT)
    unauthorized[
        "ALLOW_LIVE_SYNTHETIC_ACCEPTANCE"
    ] = "0"

    low_daily_limit = dict(VALID_ENVIRONMENT)
    low_daily_limit[
        "MULE_NETWORK_DAILY_AI_CALL_LIMIT"
    ] = "14"

    low_run_limit = dict(VALID_ENVIRONMENT)
    low_run_limit[
        "MULE_NETWORK_RUN_AI_CALL_LIMIT"
    ] = "10"

    missing_api_key = dict(VALID_ENVIRONMENT)
    missing_api_key["OPENAI_API_KEY"] = ""

    _expect_failure(
        unauthorized,
        "not explicitly authorized",
    )
    _expect_failure(
        low_daily_limit,
        "daily limit is below",
    )
    _expect_failure(
        low_run_limit,
        "run limit is below",
    )
    _expect_failure(
        missing_api_key,
        "OPENAI_API_KEY is not configured",
    )

    print(
        "Live acceptance preflight smoke test passed."
    )
    print("Explicit authorization: passed")
    print("Selected scenarios: 5")
    print("Maximum initial calls: 15")
    print("Maximum scenario calls: 11")
    print("Runtime identity validated: passed")
    print("API key exposed: no")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
