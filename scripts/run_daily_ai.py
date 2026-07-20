"""Run one controlled daily AI processing cycle."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.daily_ai_runner import (
    load_daily_ai_settings,
    run_controlled_daily_ai,
)
from network_mule_discovery.daily_state_preflight import (
    DailyStatePreflightError,
    validate_daily_state_preflight,
)


def parse_arguments() -> argparse.Namespace:
    """Parse daily runner arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate state and execute a bounded "
            "daily AI decision cycle."
        )
    )

    parser.add_argument(
        "--state-directory",
        required=True,
        type=Path,
    )

    parser.add_argument(
        "--run-date",
        required=True,
    )

    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Validate persisted network state without "
            "planning or AI execution."
        ),
    )

    parser.add_argument(
        "--execute-live-ai",
        action="store_true",
        help=(
            "Authorize live calls when the environment "
            "gate is also enabled."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Validate state, then plan or execute live AI."""
    arguments = parse_arguments()

    try:
        preflight = (
            validate_daily_state_preflight(
                arguments.state_directory
            )
        )

    except DailyStatePreflightError as exc:
        raise SystemExit(
            f"Daily state preflight failed: {exc}"
        ) from exc

    print("Daily state preflight passed.")
    print(
        f"State directory: "
        f"{preflight.state_directory}"
    )
    print(
        f"Persisted groups: "
        f"{preflight.group_count}"
    )
    print(
        f"Persisted nodes: "
        f"{preflight.node_count}"
    )
    print(
        f"Persisted edges: "
        f"{preflight.edge_count}"
    )

    if arguments.preflight_only:
        print("Planning executed: False")
        print("Live AI calls executed: 0")
        return

    settings = load_daily_ai_settings()

    if (
        arguments.execute_live_ai
        and not settings.live_ai_enabled
    ):
        raise SystemExit(
            "--execute-live-ai was supplied, but "
            "MULE_NETWORK_ENABLE_LIVE_AI is not 1."
        )

    if not arguments.execute_live_ai:
        settings = replace(
            settings,
            live_ai_enabled=False,
        )

    result = run_controlled_daily_ai(
        state_directory=(
            arguments.state_directory
        ),
        run_date=arguments.run_date,
        settings=settings,
    )

    print("Controlled daily AI run completed.")
    print(
        "Live AI enabled: "
        f"{result.live_ai_enabled}"
    )
    print(
        "AI calls before run: "
        f"{result.calls_before_run}"
    )
    print(
        "AI calls executed: "
        f"{result.calls_executed}"
    )
    print(
        "AI calls remaining today: "
        f"{result.calls_remaining_today}"
    )
    print(
        "Pending actionable AI items: "
        f"{result.final_plan.queued_ai_action_count}"
    )
    print(
        "Failed-closed items: "
        f"{result.final_plan.failed_closed_item_count}"
    )


if __name__ == "__main__":
    main()
