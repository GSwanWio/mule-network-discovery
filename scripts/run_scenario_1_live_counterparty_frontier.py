"""Run Scenario 1's first breadth-first live AI frontier."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_PAYLOAD_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    load_daily_ai_settings,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
    run_counterparty_ai_frontier,
    write_counterparty_frontier_outputs,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute only the first counterparty AI frontier "
            "for Scenario 1."
        )
    )
    parser.add_argument(
        "--runtime-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_1"
            / "runtime"
        ),
    )
    parser.add_argument(
        "--run-date",
        default=str(RUN_DATE),
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help=(
            "Delete the Scenario 1 live decision state before "
            "planning or execution."
        ),
    )
    parser.add_argument(
        "--execute-live-ai",
        action="store_true",
        help=(
            "Authorize live calls when the environment gate is "
            "also enabled."
        ),
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    first_layer_directory = (
        arguments.runtime_directory
        / "first_layer"
    )
    feature_directory = (
        arguments.runtime_directory
        / "features"
    )
    state_directory = (
        arguments.runtime_directory
        / "live_ai_state"
    )
    output_directory = (
        arguments.runtime_directory
        / "live_counterparty_frontier"
    )

    unified_result = load_unified_result(
        first_layer_directory
    )
    supplemental_payloads = (
        load_supplemental_subject_payloads(
            feature_directory
            / COUNTERPARTY_PAYLOAD_FILENAME
        )
    )

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

    result = run_counterparty_ai_frontier(
        unified_result=unified_result,
        supplemental_subject_payloads=(
            supplemental_payloads
        ),
        state_directory=state_directory,
        run_date=arguments.run_date,
        settings=settings,
        reset_state=arguments.reset_state,
    )

    write_counterparty_frontier_outputs(
        result=result,
        output_directory=output_directory,
    )

    initial_counterparty_queue = (
        result.controlled_run.initial_plan
        .actionable_queue
        .loc[
            lambda frame: frame["action_type"]
            .eq("RUN_COUNTERPARTY_AI")
        ]
    )

    final_counterparty_queue = (
        result.controlled_run.final_plan
        .actionable_queue
        .loc[
            lambda frame: frame["action_type"]
            .eq("RUN_COUNTERPARTY_AI")
        ]
    )

    final_customer_queue = (
        result.controlled_run.final_plan
        .actionable_queue
        .loc[
            lambda frame: frame["action_type"]
            .eq("RUN_CUSTOMER_AI")
        ]
    )

    counterparty_decisions = (
        result.decision_store.loc[
            result.decision_store[
                "subject_type"
            ].eq("COUNTERPARTY")
        ]
        .sort_values(
            by=["subject_key"],
            kind="stable",
        )
    )

    print(
        "Scenario 1 counterparty frontier completed."
    )
    print(
        f"Live AI enabled: "
        f"{result.controlled_run.live_ai_enabled}"
    )
    print(
        "Counterparty subjects at start: "
        f"{len(initial_counterparty_queue)}"
    )
    print(
        "AI calls executed: "
        f"{result.controlled_run.calls_executed}"
    )
    print(
        "Persisted counterparty decisions: "
        f"{len(counterparty_decisions)}"
    )
    print(
        "Counterparty actions remaining: "
        f"{len(final_counterparty_queue)}"
    )
    print(
        "Customer AI actions queued for next frontier: "
        f"{len(final_customer_queue)}"
    )
    print(
        f"State directory: {state_directory}"
    )
    print(
        f"Review outputs: {output_directory}"
    )

    if not counterparty_decisions.empty:
        print("\nPersisted counterparty decisions:")
        print(
            counterparty_decisions[
                [
                    "subject_key",
                    "decision",
                    "reason_code",
                    "decision_version",
                ]
            ].to_string(index=False)
        )

    audit = result.ai_call_ledger.loc[
        result.ai_call_ledger[
            "subject_type"
        ].eq("COUNTERPARTY")
    ]

    if not audit.empty:
        print("\nLive AI review audit:")
        print(
            audit[
                [
                    "subject_key",
                    "call_status",
                    "decision",
                    "confidence",
                    "rationale",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
