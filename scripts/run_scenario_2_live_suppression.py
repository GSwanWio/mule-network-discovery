"""Execute Scenario 2's controlled live counterparty suppression."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path


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
from network_mule_discovery.frontier_termination import (
    FrontierTerminationError,
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    COMMON_PUBLIC_COUNTERPARTY_ACCOUNT,
    RUN_DATE,
    SEED_CUSTOMER_ID,
)


SUPPRESSION_DECISIONS = frozenset({
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
})


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute Scenario 2's single controlled counterparty "
            "decision and terminate when suppression exhausts the "
            "frontier."
        )
    )
    parser.add_argument(
        "--runtime-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_2"
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
        help="Delete Scenario 2 live state before execution.",
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

    evidence_directory = (
        arguments.runtime_directory
        / "hub_discovery_evidence"
    )
    feature_directory = evidence_directory / "features"
    state_directory = (
        arguments.runtime_directory
        / "live_suppression_state"
    )
    output_directory = (
        arguments.runtime_directory
        / "live_suppression"
    )

    unified_result = load_unified_result(
        evidence_directory
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

    decision_rows = result.decision_store.loc[
        result.decision_store["subject_type"].eq(
            "COUNTERPARTY"
        )
        & result.decision_store["subject_key"].str.endswith(
            COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
        )
    ].copy()
    actionable_queue = (
        result.controlled_run.final_plan.actionable_queue
    )
    customer_queue_count = int(
        actionable_queue["action_type"]
        .eq("RUN_CUSTOMER_AI")
        .sum()
    )
    recursive_source_count = int(
        actionable_queue["action_type"]
        .eq("DISCOVER_CUSTOMER_RELATIONSHIPS")
        .sum()
    )
    failed_count = int(
        result.controlled_run.final_plan
        .failed_closed_item_count
    )

    termination = None
    applied_decision = ""

    if not decision_rows.empty:
        applied_decision = str(
            decision_rows.iloc[-1]["decision"]
        )

    if (
        applied_decision in SUPPRESSION_DECISIONS
        and actionable_queue.empty
        and failed_count == 0
    ):
        termination = (
            run_frontier_exhaustion_termination(
                state_directory=state_directory,
                run_date=arguments.run_date,
                supplemental_subject_payloads=(
                    supplemental_payloads
                ),
                group_ids=(
                    unified_result.groups["group_id"]
                    .astype("string")
                    .tolist()
                ),
                source_entity_key=(
                    f"RETAIL|{SEED_CUSTOMER_ID}"
                ),
            )
        )
        termination.termination_status.to_csv(
            output_directory / "termination_status.csv",
            index=False,
        )
        termination.guardrail_telemetry.to_csv(
            output_directory
            / "termination_guardrail_telemetry.csv",
            index=False,
        )

    projection = (
        termination.final_plan.projection
        if termination is not None
        else result.controlled_run.final_plan.projection
    )
    suppressed_relationship_count = int(
        (
            projection.edges["edge_type"]
            .eq("SHARED_EXTERNAL_COUNTERPARTY")
            & projection.edges["relationship_status"]
            .astype("string")
            .str.startswith("COUNTERPARTY_SUPPRESSED_")
        ).sum()
    )

    telemetry = {
        "run_date": str(arguments.run_date),
        "counterparty_key": (
            f"LOCAL_ACCOUNT|"
            f"{COMMON_PUBLIC_COUNTERPARTY_ACCOUNT}"
        ),
        "live_ai_enabled": (
            result.controlled_run.live_ai_enabled
        ),
        "calls_executed": (
            result.controlled_run.calls_executed
        ),
        "applied_decision": applied_decision,
        "expected_common_public_match": (
            applied_decision
            == "COMMON_PUBLIC_SUPPRESS"
        ),
        "suppressed_non_seed_customer_count": (
            suppressed_relationship_count
        ),
        "customer_ai_actions_queued": (
            customer_queue_count
        ),
        "recursive_sources_queued": (
            recursive_source_count
        ),
        "failed_frontier_count": failed_count,
        "termination_status": (
            "TERMINATED"
            if termination is not None
            else ""
        ),
        "termination_reason": (
            "FRONTIER_EXHAUSTED"
            if termination is not None
            else ""
        ),
        "observed_node_count": len(unified_result.nodes),
        "observed_edge_count": len(unified_result.edges),
    }

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    (output_directory / "live_suppression_telemetry.json").write_text(
        json.dumps(
            telemetry,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print("Scenario 2 live suppression completed.")
    print(
        "Live AI enabled: "
        f"{result.controlled_run.live_ai_enabled}"
    )
    print(
        "AI calls executed: "
        f"{result.controlled_run.calls_executed}"
    )
    print(
        "Applied counterparty decision: "
        f"{applied_decision or 'none'}"
    )
    print(
        "Expected COMMON_PUBLIC_SUPPRESS: "
        f"{telemetry['expected_common_public_match']}"
    )
    print(
        "Suppressed non-seed customer relationships: "
        f"{suppressed_relationship_count}"
    )
    print(
        "Customer AI actions queued: "
        f"{customer_queue_count}"
    )
    print(
        "Recursive sources queued: "
        f"{recursive_source_count}"
    )
    print(
        "Failed frontier count: "
        f"{failed_count}"
    )
    print(
        "Termination: "
        f"{telemetry['termination_status'] or 'not reached'}"
        f"/{telemetry['termination_reason'] or 'not reached'}"
    )
    print(
        "Observed graph nodes/edges: "
        f"{len(unified_result.nodes)}/"
        f"{len(unified_result.edges)}"
    )
    print(f"State directory: {state_directory}")
    print(f"Review outputs: {output_directory}")

    audit = result.ai_call_ledger.loc[
        result.ai_call_ledger["subject_type"].eq(
            "COUNTERPARTY"
        )
    ]

    if not audit.empty:
        print("\nAI review audit:")
        print(
            audit[
                [
                    "subject_key",
                    "call_status",
                    "decision",
                    "confidence",
                    "rationale",
                    "response_id",
                    "input_tokens",
                    "output_tokens",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    try:
        main()
    except FrontierTerminationError as exc:
        raise SystemExit(str(exc)) from exc
