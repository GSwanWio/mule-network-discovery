"""Non-billable preflight for full live synthetic acceptance."""

from __future__ import annotations

import os
from dataclasses import dataclass

from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
    load_daily_ai_settings,
)
from network_mule_discovery.live_acceptance_matrix import (
    LIVE_ACCEPTANCE_CASES,
    LiveAcceptanceCase,
    get_live_acceptance_case,
)
from network_mule_discovery.production_ai_runtime import (
    ProductionAiRuntime,
    build_production_ai_runtime,
)


LIVE_ACCEPTANCE_AUTHORIZATION_ENV_VAR = (
    "ALLOW_LIVE_SYNTHETIC_ACCEPTANCE"
)


class LiveAcceptancePreflightError(RuntimeError):
    """Live acceptance is not configured safely."""


@dataclass(frozen=True)
class LiveAcceptancePreflight:
    """Validated non-secret live acceptance identity."""

    scenario_ids: tuple[str, ...]
    scenario_count: int
    maximum_initial_live_calls: int
    maximum_scenario_live_calls: int
    settings: DailyAiSettings
    runtime: ProductionAiRuntime


def _resolve_cases(
    scenario_ids: tuple[str, ...] | None,
) -> tuple[LiveAcceptanceCase, ...]:
    """Resolve and validate the selected acceptance cases."""
    if scenario_ids is None:
        return LIVE_ACCEPTANCE_CASES

    normalized = tuple(
        str(scenario_id).strip().lower()
        for scenario_id in scenario_ids
    )

    if not normalized:
        raise LiveAcceptancePreflightError(
            "At least one acceptance scenario is required."
        )

    if any(not scenario_id for scenario_id in normalized):
        raise LiveAcceptancePreflightError(
            "Acceptance scenario IDs cannot be blank."
        )

    if len(set(normalized)) != len(normalized):
        raise LiveAcceptancePreflightError(
            "Acceptance scenario IDs cannot be duplicated."
        )

    try:
        return tuple(
            get_live_acceptance_case(scenario_id)
            for scenario_id in normalized
        )
    except ValueError as exc:
        raise LiveAcceptancePreflightError(
            str(exc)
        ) from exc


def run_live_acceptance_preflight(
    *,
    scenario_ids: tuple[str, ...] | None = None,
) -> LiveAcceptancePreflight:
    """Validate live execution without making an API call."""
    if (
        os.getenv(
            LIVE_ACCEPTANCE_AUTHORIZATION_ENV_VAR,
            "",
        ).strip()
        != "1"
    ):
        raise LiveAcceptancePreflightError(
            "Live synthetic acceptance is not explicitly "
            "authorized. Set "
            "ALLOW_LIVE_SYNTHETIC_ACCEPTANCE=1."
        )

    cases = _resolve_cases(scenario_ids)

    try:
        settings = load_daily_ai_settings()
    except Exception as exc:
        raise LiveAcceptancePreflightError(
            "Live-AI safety settings are invalid: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if not settings.live_ai_enabled:
        raise LiveAcceptancePreflightError(
            "MULE_NETWORK_ENABLE_LIVE_AI must equal 1."
        )

    maximum_initial_live_calls = sum(
        case.max_live_calls
        for case in cases
    )
    maximum_scenario_live_calls = max(
        (
            case.max_live_calls
            for case in cases
        ),
        default=0,
    )

    if (
        settings.daily_call_limit
        < maximum_initial_live_calls
    ):
        raise LiveAcceptancePreflightError(
            "The configured daily limit is below the "
            "selected acceptance-suite cap: "
            f"{settings.daily_call_limit} < "
            f"{maximum_initial_live_calls}."
        )

    if (
        settings.run_call_limit
        < maximum_scenario_live_calls
    ):
        raise LiveAcceptancePreflightError(
            "The configured run limit is below the "
            "largest scenario cap: "
            f"{settings.run_call_limit} < "
            f"{maximum_scenario_live_calls}."
        )

    try:
        runtime = build_production_ai_runtime(
            settings
        )
    except Exception as exc:
        raise LiveAcceptancePreflightError(
            "Production OpenAI configuration is invalid: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    return LiveAcceptancePreflight(
        scenario_ids=tuple(
            case.scenario_id
            for case in cases
        ),
        scenario_count=len(cases),
        maximum_initial_live_calls=(
            maximum_initial_live_calls
        ),
        maximum_scenario_live_calls=(
            maximum_scenario_live_calls
        ),
        settings=settings,
        runtime=runtime,
    )
