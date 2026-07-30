"""Consume Scenario 3's mule-like source and terminate the frontier."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.customer_behavioral_features import (
    CUSTOMER_PAYLOAD_FILENAME,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
)
from network_mule_discovery.recursive_termination import (
    run_recursive_termination,
)
from network_mule_discovery.scenario_3_synthetic_data import (
    PAYMENT_BACKED_CUSTOMER_ID,
    RUN_DATE,
)


PAYMENT_BACKED_SUBJECT_KEY = f"RETAIL|{PAYMENT_BACKED_CUSTOMER_ID}"
BENEFICIARY_EDGE_TYPES = frozenset({
    "BENEFICIARY_ADDED_SEED_ACCOUNT",
    "BENEFICIARY_ADDED_MULE_ACCOUNT",
})


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consume Scenario 3's approved mule-like customer, "
            "record a zero-row recursive round, and persist "
            "frontier exhaustion."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=PROJECT_ROOT / "data" / "synthetic" / "scenario_3",
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
    parser.add_argument("--run-date", default=str(RUN_DATE))
    return parser.parse_args()


def _load_customer_payloads(runtime_directory: Path) -> pd.DataFrame:
    path = (
        runtime_directory
        / "beneficiary_discovery"
        / "features"
        / CUSTOMER_PAYLOAD_FILENAME
    )

    if not path.exists():
        raise SystemExit(
            "Scenario 3 customer evidence is unavailable. Run "
            "run_scenario_3_beneficiary_discovery.py first: "
            f"{path}"
        )

    return load_supplemental_subject_payloads(path)


def _represented_beneficiary_edges(edges: pd.DataFrame) -> pd.DataFrame:
    source_node_key = f"CUSTOMER|{PAYMENT_BACKED_SUBJECT_KEY}"

    return edges.loc[
        edges["edge_type"].isin(BENEFICIARY_EDGE_TYPES)
        & (
            edges["source_node_key"].eq(source_node_key)
            | edges["target_node_key"].eq(source_node_key)
        )
    ].copy()


def main() -> None:
    arguments = parse_arguments()
    state_directory = (
        arguments.runtime_directory / "live_customer_decision_state"
    )
    output_directory = arguments.runtime_directory / "recursive_termination"
    output_directory.mkdir(parents=True, exist_ok=True)

    payloads = _load_customer_payloads(arguments.runtime_directory)
    result = run_recursive_termination(
        source_directory=arguments.source_directory,
        state_directory=state_directory,
        run_date=arguments.run_date,
        supplemental_subject_payloads=payloads,
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

    represented_edges = _represented_beneficiary_edges(
        result.final_plan.projection.edges
    )
    status = result.termination_status.iloc[0]
    guardrail = result.guardrail_telemetry.iloc[0]
    telemetry = {
        "run_date": str(arguments.run_date),
        "source_entity_key": result.source_entity_key,
        "discovery_performed": result.discovery_performed,
        "expansion_ledger_appended": result.expansion_ledger_appended,
        "new_counterparty_count": len(
            result.discovery.new_counterparty_keys
        ),
        "existing_counterparty_skip_count": len(
            result.discovery.skipped_existing_counterparty_keys
        ),
        "unshared_counterparty_skip_count": len(
            result.discovery.unshared_counterparty_keys
        ),
        "represented_beneficiary_edge_count": len(represented_edges),
        "ready_frontier_count": int(status["ready_frontier_count"]),
        "failed_frontier_count": int(status["failed_frontier_count"]),
        "termination_status": str(status["termination_status"]),
        "termination_reason": str(status["termination_reason"]),
        "observed_node_count": int(guardrail["total_node_count"]),
        "observed_edge_count": int(guardrail["total_edge_count"]),
        "max_observed_depth": int(guardrail["max_observed_depth"]),
        "guardrail_status": str(guardrail["guardrail_status"]),
        "live_ai_calls": 0,
    }
    (
        output_directory / "recursive_termination_telemetry.json"
    ).write_text(
        json.dumps(telemetry, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print("Scenario 3 recursive termination completed.")
    print(f"Final expansion source: {telemetry['source_entity_key']}")
    print(
        "Discovery performed this run: "
        f"{telemetry['discovery_performed']}"
    )
    print(
        "Expansion ledger row appended: "
        f"{telemetry['expansion_ledger_appended']}"
    )
    print(
        "Known-mule beneficiary edges already represented: "
        f"{telemetry['represented_beneficiary_edge_count']}"
    )
    print(
        "New shared counterparties discovered: "
        f"{telemetry['new_counterparty_count']}"
    )
    print(
        "Unshared counterparties skipped: "
        f"{telemetry['unshared_counterparty_skip_count']}"
    )
    print(f"Ready frontier count: {telemetry['ready_frontier_count']}")
    print(f"Failed frontier count: {telemetry['failed_frontier_count']}")
    print(
        "Termination status/reason: "
        f"{telemetry['termination_status']}/"
        f"{telemetry['termination_reason']}"
    )
    print(
        "Observed graph nodes/edges: "
        f"{telemetry['observed_node_count']}/"
        f"{telemetry['observed_edge_count']}"
    )
    print(f"Maximum observed depth: {telemetry['max_observed_depth']}")
    print(f"Guardrail mode: {telemetry['guardrail_status']}")
    print("Live AI calls made: 0")
    print(f"State directory: {state_directory}")
    print(f"Review outputs: {output_directory}")


if __name__ == "__main__":
    main()
