"""Run Scenario 4 deterministic EID-only grouping and termination."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    CounterpartyDiscoveryResult,
    discover_counterparty_candidates,
)
from network_mule_discovery.daily_state import (
    NETWORK_GROUPS_FILENAME,
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.eid_discovery import (
    EidDiscoveryResult,
    discover_entities_by_seed_eids,
)
from network_mule_discovery.frontier_termination import (
    FrontierTerminationResult,
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_4_synthetic_data import (
    RUN_DATE,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
    build_unified_seed_groups,
)


SOURCE_ENTITY_KEY = "DETERMINISTIC_EID_ONLY"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Scenario 4 deterministic EID-only grouping, "
            "zero-AI planning, and frontier exhaustion."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_4"
        ),
    )
    parser.add_argument(
        "--runtime-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_4"
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
    )
    return parser.parse_args()


def build_scenario_4_network(
    *,
    source_directory: Path,
    canonical_directory: Path,
    discovery_output_directory: Path,
    run_date: str,
) -> tuple[
    EidDiscoveryResult,
    CounterpartyDiscoveryResult,
    UnifiedGroupResult,
]:
    """Build the deterministic Scenario 4 network."""
    paths = write_canonical_discovery_inputs(
        source_directory=source_directory,
        output_directory=canonical_directory,
        run_date=run_date,
    )
    data_source = CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=paths.seed_mule_pool_path,
        customer_identity_path=paths.customer_identity_path,
        seed_mule_events_path=paths.seed_mule_events_path,
        counterparty_events_path=paths.counterparty_events_path,
        output_directory=discovery_output_directory,
    )
    eid_result = discover_entities_by_seed_eids(
        data_source=data_source,
        run_date=run_date,
    )
    counterparty_result = discover_counterparty_candidates(
        data_source=data_source,
        run_date=run_date,
    )
    unified_result = build_unified_seed_groups(
        eid_discovery=eid_result,
        counterparty_discovery=counterparty_result,
        run_date=run_date,
        assess_eid_linked_customers=False,
    )

    return (
        eid_result,
        counterparty_result,
        unified_result,
    )


def terminate_scenario_4_groups(
    *,
    state_directory: Path,
    run_date: str,
) -> FrontierTerminationResult:
    """Terminate the EID-only groups after confirming zero work."""
    return run_frontier_exhaustion_termination(
        state_directory=state_directory,
        run_date=run_date,
        supplemental_subject_payloads=None,
        source_entity_key=SOURCE_ENTITY_KEY,
    )


def _group_sizes(groups: pd.DataFrame) -> list[int]:
    return sorted(
        int(value)
        for value in groups["customer_count"].tolist()
    )


def main() -> None:
    arguments = parse_arguments()
    canonical_directory = (
        arguments.runtime_directory / "canonical"
    )
    output_directory = (
        arguments.runtime_directory / "eid_only_groups"
    )
    state_directory = (
        arguments.runtime_directory / "eid_only_state"
    )

    if arguments.reset_state:
        shutil.rmtree(
            state_directory,
            ignore_errors=True,
        )

    (
        eid_result,
        counterparty_result,
        unified_result,
    ) = build_scenario_4_network(
        source_directory=arguments.source_directory,
        canonical_directory=canonical_directory,
        discovery_output_directory=output_directory,
        run_date=arguments.run_date,
    )

    state_store = CsvDailyStateStore(state_directory)
    state_path = state_directory / NETWORK_GROUPS_FILENAME
    state_initialized = not state_path.exists()

    if state_initialized:
        state_store.save_network_state(
            network=unified_result,
            run_date=arguments.run_date,
        )

    before_snapshot = state_store.load_snapshot()
    before_terminated_count = int(
        before_snapshot.network.groups.get(
            "termination_status",
            pd.Series(
                "",
                index=before_snapshot.network.groups.index,
            ),
        )
        .astype("string")
        .eq("TERMINATED")
        .sum()
    )
    initial_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=arguments.run_date,
    )

    if not initial_plan.actionable_queue.empty:
        remaining = initial_plan.actionable_queue[
            ["action_type", "subject_key"]
        ].to_dict("records")
        raise SystemExit(
            "Scenario 4 unexpectedly created a decision or "
            f"expansion frontier: {remaining}"
        )

    termination = terminate_scenario_4_groups(
        state_directory=state_directory,
        run_date=arguments.run_date,
    )
    final_snapshot = state_store.load_snapshot()
    groups = final_snapshot.network.groups
    nodes = final_snapshot.network.nodes
    edges = final_snapshot.network.edges
    terminated_count = int(
        groups["termination_status"]
        .astype("string")
        .eq("TERMINATED")
        .sum()
    )
    termination_changed = (
        terminated_count > before_terminated_count
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    eid_result.eid_links.to_csv(
        output_directory / "eid_links.csv",
        index=False,
    )
    groups.to_csv(
        output_directory / "terminated_groups.csv",
        index=False,
    )
    nodes.to_csv(
        output_directory / "terminated_nodes.csv",
        index=False,
    )
    edges.to_csv(
        output_directory / "terminated_edges.csv",
        index=False,
    )
    termination.termination_status.to_csv(
        output_directory / "termination_status.csv",
        index=False,
    )
    termination.guardrail_telemetry.to_csv(
        output_directory / "guardrail_telemetry.csv",
        index=False,
    )

    telemetry = {
        "run_date": str(arguments.run_date),
        "state_initialized": state_initialized,
        "termination_changed": termination_changed,
        "seed_count": len(
            eid_result.seed_resolution.seed_entities
        ),
        "group_count": len(groups),
        "group_sizes": _group_sizes(groups),
        "observed_node_count": len(nodes),
        "observed_edge_count": len(edges),
        "eid_link_count": len(eid_result.eid_links),
        "counterparty_candidate_count": len(
            counterparty_result.candidate_counterparties
        ),
        "beneficiary_seed_link_count": len(
            counterparty_result.beneficiary_seed_links
        ),
        "counterparty_ai_actions_queued": 0,
        "customer_ai_actions_queued": 0,
        "recursive_sources_queued": 0,
        "ready_frontier_count": len(
            termination.final_plan.actionable_queue
        ),
        "failed_frontier_count": (
            termination.final_plan.failed_closed_item_count
        ),
        "terminated_group_count": terminated_count,
        "termination_reason": "FRONTIER_EXHAUSTED",
        "live_ai_calls": 0,
    }
    (
        output_directory
        / "eid_only_telemetry.json"
    ).write_text(
        json.dumps(
            telemetry,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print("Scenario 4 EID-only grouping completed.")
    print(f"State initialized this run: {state_initialized}")
    print(f"Termination changed this run: {termination_changed}")
    print(f"Confirmed seeds: {telemetry['seed_count']}")
    print(f"Stable groups: {telemetry['group_count']}")
    print(f"Group sizes: {telemetry['group_sizes']}")
    print(f"EID links: {telemetry['eid_link_count']}")
    print(
        "Observed graph nodes/edges: "
        f"{telemetry['observed_node_count']}/"
        f"{telemetry['observed_edge_count']}"
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
        "Termination: TERMINATED/"
        f"{telemetry['termination_reason']}"
    )
    print("Live AI calls made: 0")
    print(f"State directory: {state_directory}")
    print(f"Review outputs: {output_directory}")


if __name__ == "__main__":
    main()
