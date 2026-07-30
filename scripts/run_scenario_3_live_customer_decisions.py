"""Run Scenario 3's direct customer AI decision frontier."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
from network_mule_discovery.scenario_3_synthetic_data import (
    ADD_ONLY_CUSTOMER_ID,
    PAYMENT_BACKED_CUSTOMER_ID,
    RUN_DATE,
)


PAYMENT_BACKED_SUBJECT_KEY = (
    f"RETAIL|{PAYMENT_BACKED_CUSTOMER_ID}"
)
ADD_ONLY_SUBJECT_KEY = f"SME|{ADD_ONLY_CUSTOMER_ID}"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute Scenario 3's direct customer AI frontier "
            "after beneficiary-to-confirmed-mule discovery."
        )
    )
    parser.add_argument(
        "--runtime-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_3"
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
            "Delete only Scenario 3 customer-decision state "
            "before rebuilding the frontier."
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


def _current_customer_decisions(
    *,
    result,
) -> pd.DataFrame:
    decision_history = result.decision_store.loc[
        result.decision_store["subject_type"].eq("CUSTOMER")
    ].copy()

    if decision_history.empty:
        return decision_history

    snapshots = (
        result.controlled_run.final_plan
        .projection.subject_snapshots.loc[
            lambda frame: frame["subject_type"].eq("CUSTOMER"),
            [
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
        ]
    )

    return (
        snapshots.merge(
            decision_history,
            how="inner",
            on=[
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
            validate="one_to_one",
        )
        .sort_values(by=["subject_key"], kind="stable")
        .reset_index(drop=True)
    )


def main() -> None:
    arguments = parse_arguments()

    discovery_directory = (
        arguments.runtime_directory
        / "beneficiary_discovery"
    )
    feature_directory = discovery_directory / "features"
    state_directory = (
        arguments.runtime_directory
        / "live_customer_decision_state"
    )
    output_directory = (
        arguments.runtime_directory
        / "live_customer_decisions"
    )

    if arguments.reset_state:
        for directory in (
            state_directory,
            output_directory,
        ):
            if directory.exists():
                shutil.rmtree(directory)

    unified_result = load_unified_result(
        discovery_directory
    )
    customer_payloads = load_supplemental_subject_payloads(
        feature_directory / CUSTOMER_PAYLOAD_FILENAME
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
        supplemental_subject_payloads=customer_payloads,
        state_directory=state_directory,
        run_date=arguments.run_date,
        settings=settings,
    )

    write_customer_frontier_outputs(
        result=result,
        output_directory=output_directory,
    )

    initial_queue = (
        result.controlled_run.initial_plan
        .actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_CUSTOMER_AI"
            )
        ]
    )
    final_queue = (
        result.controlled_run.final_plan
        .actionable_queue
    )
    remaining_customer_queue = final_queue.loc[
        final_queue["action_type"].eq("RUN_CUSTOMER_AI")
    ]
    recursive_queue = final_queue.loc[
        final_queue["action_type"].eq(
            "DISCOVER_CUSTOMER_RELATIONSHIPS"
        )
    ]
    failed_frontier = (
        result.controlled_run.final_plan
        .frontier_queue.loc[
            lambda frame: frame["queue_status"].eq(
                "FAILED_CLOSED"
            )
        ]
    )

    current_decisions = _current_customer_decisions(
        result=result
    )
    decision_map = dict(
        zip(
            current_decisions.get(
                "subject_key",
                pd.Series(dtype="string"),
            ),
            current_decisions.get(
                "decision",
                pd.Series(dtype="string"),
            ),
        )
    )

    telemetry = {
        "run_date": str(arguments.run_date),
        "live_ai_enabled": bool(
            result.controlled_run.live_ai_enabled
        ),
        "calls_executed": int(
            result.controlled_run.calls_executed
        ),
        "customer_subjects_at_start": len(initial_queue),
        "current_customer_decision_count": len(
            current_decisions
        ),
        "customer_actions_remaining": len(
            remaining_customer_queue
        ),
        "recursive_sources_queued": len(recursive_queue),
        "failed_frontier_count": len(failed_frontier),
        "payment_backed_customer_decision": (
            decision_map.get(PAYMENT_BACKED_SUBJECT_KEY, "")
        ),
        "add_only_customer_decision": (
            decision_map.get(ADD_ONLY_SUBJECT_KEY, "")
        ),
        "expected_payment_backed_mule_like_match": (
            decision_map.get(PAYMENT_BACKED_SUBJECT_KEY)
            == "MULE_LIKE"
        ),
        "expected_add_only_insufficient_match": (
            decision_map.get(ADD_ONLY_SUBJECT_KEY)
            == "INSUFFICIENT_EVIDENCE"
        ),
        "observed_node_count": len(unified_result.nodes),
        "observed_edge_count": len(unified_result.edges),
    }

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    (
        output_directory
        / "live_customer_decision_telemetry.json"
    ).write_text(
        json.dumps(
            telemetry,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print("Scenario 3 live customer decisions completed.")
    print(
        "Live AI enabled: "
        f"{telemetry['live_ai_enabled']}"
    )
    print(
        "Customer subjects at start: "
        f"{telemetry['customer_subjects_at_start']}"
    )
    print(
        "AI calls executed: "
        f"{telemetry['calls_executed']}"
    )
    print(
        "Current customer decisions: "
        f"{telemetry['current_customer_decision_count']}"
    )
    print(
        "Payment-backed customer decision: "
        f"{telemetry['payment_backed_customer_decision'] or 'none'}"
    )
    print(
        "Add-only customer decision: "
        f"{telemetry['add_only_customer_decision'] or 'none'}"
    )
    print(
        "Expected R3002 MULE_LIKE: "
        f"{telemetry['expected_payment_backed_mule_like_match']}"
    )
    print(
        "Expected B3001 INSUFFICIENT_EVIDENCE: "
        f"{telemetry['expected_add_only_insufficient_match']}"
    )
    print(
        "Customer actions remaining: "
        f"{telemetry['customer_actions_remaining']}"
    )
    print(
        "Recursive sources queued: "
        f"{telemetry['recursive_sources_queued']}"
    )
    print(
        "Failed frontier count: "
        f"{telemetry['failed_frontier_count']}"
    )
    print(
        "Observed graph nodes/edges: "
        f"{telemetry['observed_node_count']}/"
        f"{telemetry['observed_edge_count']}"
    )
    print(f"State directory: {state_directory}")
    print(f"Review outputs: {output_directory}")

    customer_audit = result.ai_call_ledger.loc[
        result.ai_call_ledger["subject_type"].eq("CUSTOMER")
    ]

    if not customer_audit.empty:
        audit_columns = [
            column
            for column in [
                "subject_key",
                "call_status",
                "decision",
                "confidence",
                "reason_code",
                "rationale",
                "response_id",
                "input_tokens",
                "output_tokens",
                "error_code",
                "error_message",
            ]
            if column in customer_audit.columns
        ]
        print("\nCustomer AI review audit:")
        print(
            customer_audit[audit_columns].to_string(
                index=False
            )
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
