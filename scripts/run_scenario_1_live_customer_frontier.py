"""Run Scenario 1's customer-only live AI frontier."""

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
from network_mule_discovery.customer_behavioral_features import (
    CUSTOMER_PAYLOAD_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    load_daily_ai_settings,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
    run_customer_ai_frontier,
    write_customer_frontier_outputs,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute only the customer AI frontier for "
            "Scenario 1."
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
    counterparty_feature_directory = (
        arguments.runtime_directory
        / "features"
    )
    customer_feature_directory = (
        arguments.runtime_directory
        / "customer_features"
    )
    state_directory = (
        arguments.runtime_directory
        / "live_ai_state"
    )
    output_directory = (
        arguments.runtime_directory
        / "live_customer_frontier"
    )

    unified_result = load_unified_result(
        first_layer_directory
    )
    counterparty_payloads = (
        load_supplemental_subject_payloads(
            counterparty_feature_directory
            / COUNTERPARTY_PAYLOAD_FILENAME
        )
    )
    customer_payloads = (
        load_supplemental_subject_payloads(
            customer_feature_directory
            / CUSTOMER_PAYLOAD_FILENAME
        )
    )
    supplemental_payloads = pd.concat(
        [
            counterparty_payloads,
            customer_payloads,
        ],
        ignore_index=True,
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

    result = run_customer_ai_frontier(
        unified_result=unified_result,
        supplemental_subject_payloads=(
            supplemental_payloads
        ),
        state_directory=state_directory,
        run_date=arguments.run_date,
        settings=settings,
    )

    write_customer_frontier_outputs(
        result=result,
        output_directory=output_directory,
    )

    initial_customer_queue = (
        result.controlled_run.initial_plan
        .actionable_queue
        .loc[
            lambda frame: frame["action_type"]
            .eq("RUN_CUSTOMER_AI")
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
    recursive_queue = (
        result.controlled_run.final_plan
        .actionable_queue
        .loc[
            lambda frame: frame["action_type"]
            .eq("DISCOVER_CUSTOMER_RELATIONSHIPS")
        ]
    )

    customer_decision_history = (
        result.decision_store.loc[
            result.decision_store[
                "subject_type"
            ].eq("CUSTOMER")
        ]
        .sort_values(
            by=[
                "subject_key",
                "decided_at",
                "decision_id",
            ],
            kind="stable",
        )
    )

    current_customer_snapshots = (
        result.controlled_run.final_plan
        .projection.subject_snapshots.loc[
            lambda frame: frame[
                "subject_type"
            ].eq("CUSTOMER"),
            [
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
        ]
    )

    current_customer_decisions = (
        current_customer_snapshots.merge(
            customer_decision_history,
            how="inner",
            on=[
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
            validate="one_to_one",
        )
        .sort_values(
            by=["subject_key"],
            kind="stable",
        )
    )

    print(
        "Scenario 1 customer frontier completed."
    )
    print(
        f"Live AI enabled: "
        f"{result.controlled_run.live_ai_enabled}"
    )
    print(
        "Customer subjects at start: "
        f"{len(initial_customer_queue)}"
    )
    print(
        "AI calls executed: "
        f"{result.controlled_run.calls_executed}"
    )
    print(
        "Current customer decisions: "
        f"{len(current_customer_decisions)}"
    )
    print(
        "Customer decision history rows: "
        f"{len(customer_decision_history)}"
    )
    print(
        "Customer actions remaining: "
        f"{len(final_customer_queue)}"
    )
    print(
        "Recursive expansion sources queued: "
        f"{len(recursive_queue)}"
    )
    print(
        f"State directory: {state_directory}"
    )
    print(
        f"Review outputs: {output_directory}"
    )

    if not current_customer_decisions.empty:
        print("\nCurrent customer decisions:")
        print(
            current_customer_decisions[
                [
                    "subject_key",
                    "decision",
                    "reason_code",
                    "decision_version",
                ]
            ].to_string(index=False)
        )

    customer_audit = result.ai_call_ledger.loc[
        result.ai_call_ledger[
            "subject_type"
        ].eq("CUSTOMER")
    ]

    if not customer_audit.empty:
        print("\nLive customer AI review audit:")
        print(
            customer_audit[
                [
                    "subject_key",
                    "call_status",
                    "decision",
                    "confidence",
                    "rationale",
                ]
            ].to_string(index=False)
        )

    if not recursive_queue.empty:
        print("\nQueued recursive expansion sources:")
        print(
            recursive_queue[
                [
                    "subject_key",
                    "queue_reason",
                    "trigger_decision_id",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
