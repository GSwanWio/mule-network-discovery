"""Execute Scenario 5's controlled live counterparty decision."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_PAYLOAD_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
    load_daily_ai_settings,
)
from network_mule_discovery.frontier_ai import (
    CounterpartyFrontierRunResult,
    load_supplemental_subject_payloads,
    load_unified_result,
    run_counterparty_ai_frontier,
    write_counterparty_frontier_outputs,
)
from network_mule_discovery.frontier_termination import (
    FrontierTerminationError,
    FrontierTerminationResult,
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    AMBIGUOUS_COUNTERPARTY_ACCOUNT,
    RUN_DATE,
    SEED_CUSTOMER_ID,
)


SUPPRESSION_DECISIONS = frozenset({
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
})


@dataclass(frozen=True)
class Scenario5LiveCounterpartyDecisionResult:
    """Persisted Scenario 5 decision, projection and termination state."""

    frontier_result: CounterpartyFrontierRunResult
    termination: FrontierTerminationResult | None
    telemetry: dict[str, object]
    state_directory: Path
    output_directory: Path


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute Scenario 5's single controlled counterparty "
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
            / "scenario_5"
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
        help="Delete Scenario 5 live state before execution.",
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


def execute_scenario_5_live_counterparty_decision(
    *,
    runtime_directory: Path | str,
    run_date: object = RUN_DATE,
    settings: DailyAiSettings,
    reset_state: bool = False,
    adapter_factory: Callable[[], object] | None = None,
) -> Scenario5LiveCounterpartyDecisionResult:
    """Execute one persisted counterparty frontier for Scenario 5."""
    resolved_runtime_directory = Path(runtime_directory)
    evidence_directory = (
        resolved_runtime_directory
        / "counterparty_evidence"
    )
    feature_directory = evidence_directory / "features"
    state_directory = (
        resolved_runtime_directory
        / "live_counterparty_decision_state"
    )
    output_directory = (
        resolved_runtime_directory
        / "live_counterparty_decision"
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

    frontier_result = run_counterparty_ai_frontier(
        unified_result=unified_result,
        supplemental_subject_payloads=(
            supplemental_payloads
        ),
        state_directory=state_directory,
        run_date=run_date,
        settings=settings,
        reset_state=reset_state,
        adapter_factory=adapter_factory,
    )

    write_counterparty_frontier_outputs(
        result=frontier_result,
        output_directory=output_directory,
    )

    base_projection = (
        frontier_result.controlled_run
        .final_plan.projection
    )

    counterparty_nodes = base_projection.nodes.loc[
        base_projection.nodes["node_type"].eq(
            "COUNTERPARTY"
        )
        & base_projection.nodes[
            "counterparty_key"
        ].astype("string").str.endswith(
            AMBIGUOUS_COUNTERPARTY_ACCOUNT
        )
    ]

    if len(counterparty_nodes) != 1:
        raise RuntimeError(
            "Expected exactly one Scenario 5 counterparty "
            f"node; found {len(counterparty_nodes)}."
        )

    actionable_queue = (
        frontier_result.controlled_run
        .final_plan.actionable_queue
    )
    counterparty_queue_count = int(
        actionable_queue["action_type"]
        .eq("RUN_COUNTERPARTY_AI")
        .sum()
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
        frontier_result.controlled_run
        .final_plan.failed_closed_item_count
    )

    termination = None
    raw_applied_decision = counterparty_nodes.iloc[0].get(
        "applied_decision",
        "",
    )
    applied_decision = str(
        raw_applied_decision
    ).strip()

    if applied_decision.lower() in {
        "",
        "<na>",
        "nan",
        "none",
    }:
        applied_decision = ""

    if (
        applied_decision in SUPPRESSION_DECISIONS
        and actionable_queue.empty
        and failed_count == 0
    ):
        termination = (
            run_frontier_exhaustion_termination(
                state_directory=state_directory,
                run_date=run_date,
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
            output_directory
            / "termination_status.csv",
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
        else base_projection
    )
    suppressed_relationship_count = int(
        (
            projection.edges["edge_type"]
            .eq("SHARED_EXTERNAL_COUNTERPARTY")
            & projection.edges[
                "relationship_status"
            ]
            .astype("string")
            .str.startswith(
                "COUNTERPARTY_SUPPRESSED_"
            )
        ).sum()
    )

    telemetry: dict[str, object] = {
        "run_date": str(run_date),
        "counterparty_key": (
            f"LOCAL_ACCOUNT|"
            f"{AMBIGUOUS_COUNTERPARTY_ACCOUNT}"
        ),
        "live_ai_enabled": (
            frontier_result.controlled_run
            .live_ai_enabled
        ),
        "calls_executed": (
            frontier_result.controlled_run
            .calls_executed
        ),
        "applied_decision": applied_decision,
        "expected_insufficient_evidence_match": (
            applied_decision
            == "INSUFFICIENT_EVIDENCE_SUPPRESS"
        ),
        "suppressed_non_seed_customer_count": (
            suppressed_relationship_count
        ),
        "counterparty_ai_actions_queued": (
            counterparty_queue_count
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
        "observed_node_count": len(
            unified_result.nodes
        ),
        "observed_edge_count": len(
            unified_result.edges
        ),
    }

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    (
        output_directory
        / "live_counterparty_decision_telemetry.json"
    ).write_text(
        json.dumps(
            telemetry,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return Scenario5LiveCounterpartyDecisionResult(
        frontier_result=frontier_result,
        termination=termination,
        telemetry=telemetry,
        state_directory=state_directory,
        output_directory=output_directory,
    )


def print_result(
    result: Scenario5LiveCounterpartyDecisionResult,
) -> None:
    """Print a compact operational review of one run."""
    telemetry = result.telemetry

    print("Scenario 5 live counterparty decision completed.")
    print(
        "Live AI enabled: "
        f"{telemetry['live_ai_enabled']}"
    )
    print(
        "AI calls executed: "
        f"{telemetry['calls_executed']}"
    )
    print(
        "Applied counterparty decision: "
        f"{telemetry['applied_decision'] or 'none'}"
    )
    print(
        "Expected INSUFFICIENT_EVIDENCE_SUPPRESS: "
        f"{telemetry['expected_insufficient_evidence_match']}"
    )
    print(
        "Suppressed non-seed customer relationships: "
        f"{telemetry['suppressed_non_seed_customer_count']}"
    )
    print(
        "Counterparty AI actions queued: "
        f"{telemetry['counterparty_ai_actions_queued']}"
    )
    print(
        "Customer AI actions queued: "
        f"{telemetry['customer_ai_actions_queued']}"
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
        "Termination: "
        f"{telemetry['termination_status'] or 'not reached'}"
        f"/{telemetry['termination_reason'] or 'not reached'}"
    )
    print(
        "Observed graph nodes/edges: "
        f"{telemetry['observed_node_count']}/"
        f"{telemetry['observed_edge_count']}"
    )
    print(f"State directory: {result.state_directory}")
    print(f"Review outputs: {result.output_directory}")

    audit = result.frontier_result.ai_call_ledger.loc[
        result.frontier_result.ai_call_ledger[
            "subject_type"
        ].eq("COUNTERPARTY")
    ]

    if not audit.empty:
        columns = [
            column
            for column in [
                "subject_key",
                "call_status",
                "decision",
                "confidence",
                "reason_code",
                "rationale",
                "response_id",
                "request_id",
                "input_tokens",
                "output_tokens",
                "error_code",
                "error_message",
            ]
            if column in audit.columns
        ]
        print("\nCounterparty AI review audit:")
        print(
            audit[columns].to_string(
                index=False,
            )
        )


def main() -> None:
    arguments = parse_arguments()
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

    result = execute_scenario_5_live_counterparty_decision(
        runtime_directory=arguments.runtime_directory,
        run_date=arguments.run_date,
        settings=settings,
        reset_state=arguments.reset_state,
    )
    print_result(result)


if __name__ == "__main__":
    try:
        main()
    except FrontierTerminationError as exc:
        raise SystemExit(str(exc)) from exc
