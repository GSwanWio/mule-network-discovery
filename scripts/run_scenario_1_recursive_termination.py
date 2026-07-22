"""Finalize Scenario 1 after the recursive frontier is exhausted."""

from __future__ import annotations

import argparse
import sys
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
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
)
from network_mule_discovery.recursive_termination import (
    run_recursive_termination,
)
from network_mule_discovery.scenario_1_synthetic_data import RUN_DATE


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consume Scenario 1's final recursive source and persist "
            "frontier exhaustion."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT / "data" / "synthetic" / "scenario_1"
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
    parser.add_argument("--run-date", default=str(RUN_DATE))
    return parser.parse_args()


def _load_payloads(runtime_directory: Path) -> pd.DataFrame:
    paths = [
        runtime_directory
        / "features"
        / COUNTERPARTY_PAYLOAD_FILENAME,
        runtime_directory
        / "customer_features"
        / CUSTOMER_PAYLOAD_FILENAME,
        runtime_directory
        / "recursive_counterparty_features"
        / COUNTERPARTY_PAYLOAD_FILENAME,
        runtime_directory
        / "recursive_customer_features"
        / CUSTOMER_PAYLOAD_FILENAME,
    ]
    missing = [str(path) for path in paths if not path.exists()]

    if missing:
        raise SystemExit(
            "Required persisted evidence payloads are missing: "
            f"{missing}"
        )

    combined = pd.concat(
        [load_supplemental_subject_payloads(path) for path in paths],
        ignore_index=True,
    )

    return (
        combined
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


def main() -> None:
    arguments = parse_arguments()
    runtime_directory = arguments.runtime_directory
    state_directory = runtime_directory / "live_ai_state"
    output_directory = runtime_directory / "live_recursive_termination"
    output_directory.mkdir(parents=True, exist_ok=True)
    supplemental_payloads = _load_payloads(runtime_directory)
    result = run_recursive_termination(
        source_directory=arguments.source_directory,
        state_directory=state_directory,
        run_date=arguments.run_date,
        supplemental_subject_payloads=supplemental_payloads,
    )
    outputs = {
        "termination_status.csv": result.termination_status,
        "guardrail_telemetry.csv": result.guardrail_telemetry,
        "expansion_ledger.csv": result.expansion_ledger,
        "frontier_queue.csv": result.final_plan.frontier_queue,
        "decision_groups.csv": result.final_plan.projection.groups,
        "decision_group_nodes.csv": result.final_plan.projection.nodes,
        "decision_group_edges.csv": result.final_plan.projection.edges,
    }

    for filename, frame in outputs.items():
        frame.to_csv(output_directory / filename, index=False)

    telemetry = result.guardrail_telemetry.iloc[0]
    status = result.termination_status.iloc[0]

    print("Scenario 1 recursive termination completed.")
    print(f"Final expansion source: {result.source_entity_key}")
    print(
        "Discovery performed this run: "
        f"{result.discovery_performed}"
    )
    print(
        "Expansion ledger row appended: "
        f"{result.expansion_ledger_appended}"
    )
    print(
        "New shared counterparties discovered: "
        f"{len(result.discovery.new_counterparty_keys)}"
    )
    print(
        "Already observed counterparties skipped: "
        f"{len(result.discovery.skipped_existing_counterparty_keys)}"
    )
    print(
        "Unshared counterparties skipped: "
        f"{len(result.discovery.unshared_counterparty_keys)}"
    )
    print(
        "Ready frontier count: "
        f"{status['ready_frontier_count']}"
    )
    print(
        "Failed frontier count: "
        f"{status['failed_frontier_count']}"
    )
    print(
        "Termination status/reason: "
        f"{status['termination_status']}/"
        f"{status['termination_reason']}"
    )
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


if __name__ == "__main__":
    main()
