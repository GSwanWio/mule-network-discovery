"""Run Scenario 3 deterministic beneficiary discovery and planning."""

from __future__ import annotations

import argparse
import json
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
    discover_counterparty_candidates,
)
from network_mule_discovery.customer_behavioral_features import (
    write_customer_behavioral_features,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_3_synthetic_data import (
    RUN_DATE,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Scenario 3 beneficiary-to-confirmed-mule "
            "discovery and direct customer-AI planning."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_3"
        ),
    )
    parser.add_argument(
        "--work-directory",
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
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    canonical_directory = (
        arguments.work_directory / "canonical"
    )
    output_directory = (
        arguments.work_directory
        / "beneficiary_discovery"
    )
    state_directory = output_directory / "state"
    feature_directory = output_directory / "features"

    paths = write_canonical_discovery_inputs(
        source_directory=arguments.source_directory,
        output_directory=canonical_directory,
        run_date=arguments.run_date,
    )
    data_source = CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=paths.seed_mule_pool_path,
        customer_identity_path=paths.customer_identity_path,
        seed_mule_events_path=paths.seed_mule_events_path,
        counterparty_events_path=paths.counterparty_events_path,
        output_directory=output_directory,
    )

    eid_result = discover_entities_by_seed_eids(
        data_source=data_source,
        run_date=arguments.run_date,
    )
    counterparty_result = discover_counterparty_candidates(
        data_source=data_source,
        run_date=arguments.run_date,
    )
    unified_result = build_unified_seed_groups(
        eid_discovery=eid_result,
        counterparty_discovery=counterparty_result,
        run_date=arguments.run_date,
    )

    state_store = CsvDailyStateStore(state_directory)
    state_store.save_network_state(
        network=unified_result,
        run_date=arguments.run_date,
    )
    graph_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=arguments.run_date,
    )
    customer_queue = graph_plan.actionable_queue.loc[
        graph_plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI")
    ]
    customer_keys = sorted(
        customer_queue["subject_key"].unique()
    )

    customer_features = write_customer_behavioral_features(
        source_directory=arguments.source_directory,
        customer_keys=customer_keys,
        projection=graph_plan.projection,
        output_directory=feature_directory,
        run_date=arguments.run_date,
    )
    final_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=arguments.run_date,
        supplemental_subject_payloads=(
            customer_features.customer_payloads
        ),
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )
    counterparty_result.beneficiary_seed_links.to_csv(
        output_directory / "beneficiary_seed_links.csv",
        index=False,
    )
    unified_result.groups.to_csv(
        output_directory / "unified_groups.csv",
        index=False,
    )
    unified_result.nodes.to_csv(
        output_directory / "unified_nodes.csv",
        index=False,
    )
    unified_result.edges.to_csv(
        output_directory / "unified_edges.csv",
        index=False,
    )
    final_plan.frontier_queue.to_csv(
        output_directory / "frontier_queue.csv",
        index=False,
    )

    evidence_counts = (
        counterparty_result.beneficiary_seed_links[
            "beneficiary_link_evidence_type"
        ]
        .value_counts()
        .to_dict()
    )
    telemetry = {
        "run_date": str(arguments.run_date),
        "eid_link_count": len(eid_result.eid_links),
        "counterparty_ai_actions_queued": int(
            final_plan.actionable_queue[
                "action_type"
            ].eq("RUN_COUNTERPARTY_AI").sum()
        ),
        "customer_ai_actions_queued": int(
            final_plan.actionable_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI").sum()
        ),
        "beneficiary_seed_link_count": len(
            counterparty_result.beneficiary_seed_links
        ),
        "payment_backed_link_count": int(
            evidence_counts.get("PAYMENT_BACKED", 0)
        ),
        "add_only_link_count": int(
            evidence_counts.get("ADD_ONLY", 0)
        ),
        "observed_node_count": len(unified_result.nodes),
        "observed_edge_count": len(unified_result.edges),
        "recursive_sources_queued": int(
            final_plan.actionable_queue[
                "action_type"
            ].eq("DISCOVER_CUSTOMER_RELATIONSHIPS").sum()
        ),
        "live_ai_calls": 0,
    }
    (
        output_directory
        / "beneficiary_discovery_telemetry.json"
    ).write_text(
        json.dumps(
            telemetry,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print("Scenario 3 beneficiary discovery completed.")
    print(f"Output directory: {output_directory}")
    print(f"EID links: {telemetry['eid_link_count']}")
    print(
        "Beneficiary-to-seed links: "
        f"{telemetry['beneficiary_seed_link_count']}"
    )
    print(
        "Payment-backed links: "
        f"{telemetry['payment_backed_link_count']}"
    )
    print(
        "Add-only links: "
        f"{telemetry['add_only_link_count']}"
    )
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
    print("Live AI calls made: 0")


if __name__ == "__main__":
    main()
