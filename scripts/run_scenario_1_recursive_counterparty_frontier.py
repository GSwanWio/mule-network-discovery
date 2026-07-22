"""Run Scenario 1's second-layer counterparty frontier."""

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
    COUNTERPARTY_CUSTOMER_PROFILE_FILENAME,
    COUNTERPARTY_PAYLOAD_FILENAME,
    COUNTERPARTY_PROFILE_FILENAME,
)
from network_mule_discovery.customer_behavioral_features import (
    CUSTOMER_PAYLOAD_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    load_daily_ai_settings,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    run_recursive_counterparty_frontier,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Discover and assess Scenario 1's second-layer "
            "counterparty frontier."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_1"
        ),
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
    state_directory = (
        arguments.runtime_directory / "live_ai_state"
    )
    counterparty_feature_directory = (
        arguments.runtime_directory / "features"
    )
    customer_feature_directory = (
        arguments.runtime_directory / "customer_features"
    )
    recursive_feature_directory = (
        arguments.runtime_directory
        / "recursive_counterparty_features"
    )
    output_directory = (
        arguments.runtime_directory
        / "live_recursive_counterparty_frontier"
    )
    output_directory.mkdir(parents=True, exist_ok=True)

    counterparty_payloads = load_supplemental_subject_payloads(
        counterparty_feature_directory
        / COUNTERPARTY_PAYLOAD_FILENAME
    )
    customer_payloads = load_supplemental_subject_payloads(
        customer_feature_directory
        / CUSTOMER_PAYLOAD_FILENAME
    )
    existing_payloads = pd.concat(
        [counterparty_payloads, customer_payloads],
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

    result = run_recursive_counterparty_frontier(
        source_directory=arguments.source_directory,
        state_directory=state_directory,
        run_date=arguments.run_date,
        supplemental_subject_payloads=(
            existing_payloads
        ),
        settings=settings,
    )
    recursive_feature_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    feature_outputs = {
        COUNTERPARTY_PROFILE_FILENAME: (
            result.new_features.counterparty_profiles
        ),
        COUNTERPARTY_CUSTOMER_PROFILE_FILENAME: (
            result.new_features.counterparty_customer_profiles
        ),
        COUNTERPARTY_PAYLOAD_FILENAME: (
            result.new_features.counterparty_payloads
        ),
    }

    for filename, frame in feature_outputs.items():
        frame.to_csv(
            recursive_feature_directory / filename,
            index=False,
        )

    outputs = {
        "recursive_relationships.csv": (
            result.discovery.relationships
        ),
        "recursive_counterparties.csv": (
            result.discovery.counterparty_summary
        ),
        "expanded_groups.csv": (
            result.expanded_network.groups
        ),
        "expanded_nodes.csv": (
            result.expanded_network.nodes
        ),
        "expanded_edges.csv": (
            result.expanded_network.edges
        ),
        "frontier_queue.csv": (
            result.controlled_run.final_plan.frontier_queue
        ),
        "decision_store.csv": result.decision_store,
        "ai_call_ledger.csv": result.ai_call_ledger,
        "guardrail_telemetry.csv": (
            result.guardrail_telemetry
        ),
    }

    for filename, frame in outputs.items():
        frame.to_csv(
            output_directory / filename,
            index=False,
        )

    initial_counterparty_queue = (
        result.controlled_run.initial_plan
        .actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_COUNTERPARTY_AI"
            )
        ]
    )
    final_counterparty_queue = (
        result.controlled_run.final_plan
        .actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_COUNTERPARTY_AI"
            )
        ]
    )
    next_customer_queue = (
        result.controlled_run.final_plan
        .actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_CUSTOMER_AI"
            )
        ]
    )
    new_decisions = result.decision_store.loc[
        result.decision_store["subject_type"].eq(
            "COUNTERPARTY"
        )
        & result.decision_store["subject_key"].isin(
            result.discovery.new_counterparty_keys
        )
    ]

    print(
        "Scenario 1 recursive counterparty frontier completed."
    )
    print(
        "Approved recursive source: "
        f"{result.discovery.source_entity_key}"
    )
    print(
        "New counterparties discovered: "
        f"{len(result.discovery.new_counterparty_keys)}"
    )
    print(
        "New linked customers observed: "
        f"{result.discovery.relationships['target_entity_key'].nunique()}"
    )
    print(
        "Already observed counterparties skipped: "
        f"{len(result.discovery.skipped_existing_counterparty_keys)}"
    )
    print(
        "Unshared source counterparties skipped: "
        f"{len(result.discovery.unshared_counterparty_keys)}"
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
        "New counterparty decisions: "
        f"{len(new_decisions)}"
    )
    print(
        "Counterparty actions remaining: "
        f"{len(final_counterparty_queue)}"
    )
    print(
        "Customer actions queued for next frontier: "
        f"{len(next_customer_queue)}"
    )

    telemetry = result.guardrail_telemetry.iloc[0]
    print(
        "Observed group depth: "
        f"{telemetry['max_observed_depth']}"
    )
    print(
        "Observed group nodes/edges: "
        f"{telemetry['total_node_count']}/"
        f"{telemetry['total_edge_count']}"
    )
    print(
        "Guardrail mode: "
        f"{telemetry['guardrail_status']}"
    )
    print(f"State directory: {state_directory}")
    print(f"Review outputs: {output_directory}")

    if not new_decisions.empty:
        print("\nNew counterparty decisions:")
        print(
            new_decisions[
                [
                    "subject_key",
                    "decision",
                    "reason_code",
                    "decision_version",
                ]
            ].to_string(index=False)
        )

    if not next_customer_queue.empty:
        print("\nQueued next customer frontier:")
        print(
            next_customer_queue[
                [
                    "subject_key",
                    "queue_reason",
                ]
            ]
            .sort_values(
                by="subject_key",
                kind="stable",
            )
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
