"""Run Scenario 1's second-layer customer AI frontier."""

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
    CUSTOMER_COUNTERPARTY_PROFILE_FILENAME,
    CUSTOMER_PAYLOAD_FILENAME,
    CUSTOMER_PROFILE_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    load_daily_ai_settings,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
)
from network_mule_discovery.recursive_customer_frontier import (
    run_recursive_customer_frontier,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Assess Scenario 1's second-layer customer frontier."
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


def _load_optional_payloads(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "subject_type",
                "subject_key",
                "feature_payload_json",
            ]
        )

    return load_supplemental_subject_payloads(path)


def main() -> None:
    arguments = parse_arguments()
    runtime_directory = arguments.runtime_directory
    state_directory = runtime_directory / "live_ai_state"
    recursive_customer_feature_directory = (
        runtime_directory / "recursive_customer_features"
    )
    output_directory = (
        runtime_directory
        / "live_recursive_customer_frontier"
    )
    output_directory.mkdir(parents=True, exist_ok=True)

    payload_paths = [
        runtime_directory
        / "features"
        / COUNTERPARTY_PAYLOAD_FILENAME,
        runtime_directory
        / "customer_features"
        / CUSTOMER_PAYLOAD_FILENAME,
        runtime_directory
        / "recursive_counterparty_features"
        / COUNTERPARTY_PAYLOAD_FILENAME,
    ]
    persisted_recursive_customer_path = (
        recursive_customer_feature_directory
        / CUSTOMER_PAYLOAD_FILENAME
    )
    payload_frames = [
        load_supplemental_subject_payloads(path)
        for path in payload_paths
    ]
    persisted_recursive_customer_payloads = (
        _load_optional_payloads(
            persisted_recursive_customer_path
        )
    )

    if not persisted_recursive_customer_payloads.empty:
        payload_frames.append(
            persisted_recursive_customer_payloads
        )
        persisted_customer_keys = tuple(
            sorted(
                persisted_recursive_customer_payloads[
                    "subject_key"
                ].astype("string").unique()
            )
        )
    else:
        persisted_customer_keys = None

    supplemental_payloads = pd.concat(
        payload_frames,
        ignore_index=True,
    )
    supplemental_payloads = (
        supplemental_payloads
        .drop_duplicates(
            subset=["subject_type", "subject_key"],
            keep="last",
        )
        .sort_values(
            by=["subject_type", "subject_key"],
            kind="stable",
        )
        .reset_index(drop=True)
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

    result = run_recursive_customer_frontier(
        source_directory=arguments.source_directory,
        state_directory=state_directory,
        run_date=arguments.run_date,
        supplemental_subject_payloads=(
            supplemental_payloads
        ),
        settings=settings,
        customer_keys=persisted_customer_keys,
    )
    recursive_customer_feature_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    feature_outputs = {
        CUSTOMER_PROFILE_FILENAME: (
            result.new_features.customer_profiles
        ),
        CUSTOMER_COUNTERPARTY_PROFILE_FILENAME: (
            result.new_features
            .customer_counterparty_profiles
        ),
        CUSTOMER_PAYLOAD_FILENAME: (
            result.new_features.customer_payloads
        ),
    }

    for filename, frame in feature_outputs.items():
        frame.to_csv(
            recursive_customer_feature_directory
            / filename,
            index=False,
        )

    controlled_run = result.customer_frontier.controlled_run
    outputs = {
        "decision_groups.csv": (
            controlled_run.final_plan.projection.groups
        ),
        "decision_group_nodes.csv": (
            controlled_run.final_plan.projection.nodes
        ),
        "decision_group_edges.csv": (
            controlled_run.final_plan.projection.edges
        ),
        "decision_subject_snapshots.csv": (
            controlled_run.final_plan.projection
            .subject_snapshots
        ),
        "frontier_queue.csv": (
            controlled_run.final_plan.frontier_queue
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

    initial_customer_queue = (
        controlled_run.initial_plan.actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_CUSTOMER_AI"
            )
        ]
    )
    final_customer_queue = (
        controlled_run.final_plan.actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_CUSTOMER_AI"
            )
        ]
    )
    recursive_queue = (
        controlled_run.final_plan.actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "DISCOVER_CUSTOMER_RELATIONSHIPS"
            )
        ]
    )
    target_snapshots = (
        controlled_run.final_plan.projection
        .subject_snapshots.loc[
            lambda frame: (
                frame["subject_type"].eq("CUSTOMER")
                & frame["subject_key"].isin(
                    result.customer_keys
                )
            ),
            [
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ],
        ]
    )
    customer_history = result.decision_store.loc[
        result.decision_store["subject_type"].eq(
            "CUSTOMER"
        )
        & result.decision_store["subject_key"].isin(
            result.customer_keys
        )
    ].copy()
    current_decisions = (
        target_snapshots.merge(
            customer_history,
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
    target_audit = result.ai_call_ledger.loc[
        result.ai_call_ledger["subject_type"].eq(
            "CUSTOMER"
        )
        & result.ai_call_ledger["subject_key"].isin(
            result.customer_keys
        )
    ].copy()

    print(
        "Scenario 1 recursive customer frontier completed."
    )
    print(
        "Customer subjects: "
        f"{', '.join(result.customer_keys)}"
    )
    print(
        f"Live AI enabled: {controlled_run.live_ai_enabled}"
    )
    print(
        "Customer subjects at start: "
        f"{len(initial_customer_queue)}"
    )
    print(
        "AI calls executed: "
        f"{controlled_run.calls_executed}"
    )
    print(
        "Current recursive customer decisions: "
        f"{len(current_decisions)}"
    )
    print(
        "Recursive customer decision history rows: "
        f"{len(customer_history)}"
    )
    print(
        "Customer actions remaining: "
        f"{len(final_customer_queue)}"
    )
    print(
        "Further recursive sources queued: "
        f"{len(recursive_queue)}"
    )
    telemetry_row = result.guardrail_telemetry.iloc[0]
    print(
        "Observed group depth: "
        f"{telemetry_row['max_observed_depth']}"
    )
    print(
        "Observed group nodes/edges: "
        f"{telemetry_row['total_node_count']}/"
        f"{telemetry_row['total_edge_count']}"
    )
    print(
        "Current frontier width: "
        f"{telemetry_row['current_frontier_width']}"
    )
    print(
        "Guardrail mode: "
        f"{telemetry_row['guardrail_status']}"
    )
    print(f"State directory: {state_directory}")
    print(f"Review outputs: {output_directory}")

    if not current_decisions.empty:
        print("\nCurrent recursive customer decisions:")
        print(
            current_decisions[
                [
                    "subject_key",
                    "decision",
                    "reason_code",
                    "decision_version",
                ]
            ].to_string(index=False)
        )

    if not target_audit.empty:
        print("\nLive recursive customer AI review audit:")
        columns = [
            column
            for column in [
                "subject_key",
                "call_status",
                "decision",
                "confidence",
                "rationale",
                "error_code",
            ]
            if column in target_audit.columns
        ]
        print(
            target_audit[columns].to_string(
                index=False
            )
        )

    if not recursive_queue.empty:
        print("\nQueued further recursive sources:")
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
