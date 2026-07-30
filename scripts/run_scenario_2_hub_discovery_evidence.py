"""Run Scenario 2 discovery and bounded evidence without live AI."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_CUSTOMER_PROFILE_FILENAME,
    COUNTERPARTY_PAYLOAD_FILENAME,
    COUNTERPARTY_PROFILE_FILENAME,
    write_behavioral_features,
)
from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    discover_counterparty_candidates,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.decision_engine import (
    build_subject_snapshots,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    COMMON_PUBLIC_COUNTERPARTY_ACCOUNT,
    RUN_DATE,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Scenario 2 canonical conversion, high-degree hub "
            "discovery, bounded evidence, and planning-only frontier."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_2"
        ),
    )
    parser.add_argument(
        "--work-directory",
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
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    canonical_directory = (
        arguments.work_directory / "canonical"
    )
    output_directory = (
        arguments.work_directory
        / "hub_discovery_evidence"
    )
    feature_directory = output_directory / "features"
    state_directory = output_directory / "state"

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
    counterparty_result = (
        discover_counterparty_candidates(
            data_source=data_source,
            run_date=arguments.run_date,
        )
    )
    unified_result = build_unified_seed_groups(
        eid_discovery=eid_result,
        counterparty_discovery=counterparty_result,
        run_date=arguments.run_date,
    )

    counterparty_keys = sorted(
        counterparty_result.candidate_counterparties[
            "counterparty_key"
        ]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    features = write_behavioral_features(
        source_directory=arguments.source_directory,
        counterparty_keys=counterparty_keys,
        output_directory=feature_directory,
        run_date=arguments.run_date,
    )

    snapshots = build_subject_snapshots(
        nodes=unified_result.nodes,
        edges=unified_result.edges,
        supplemental_subject_payloads=(
            features.counterparty_payloads
        ),
    )

    state_store = CsvDailyStateStore(state_directory)
    state_store.save_network_state(
        network=unified_result,
        run_date=arguments.run_date,
    )
    plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=arguments.run_date,
        supplemental_subject_payloads=(
            features.counterparty_payloads
        ),
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    eid_result.eid_links.to_csv(
        output_directory / "eid_links.csv",
        index=False,
    )
    counterparty_result.seed_counterparties.to_csv(
        output_directory / "seed_counterparties.csv",
        index=False,
    )
    counterparty_result.candidate_customer_links.to_csv(
        output_directory / "candidate_customer_links.csv",
        index=False,
    )
    counterparty_result.candidate_counterparties.to_csv(
        output_directory / "candidate_counterparties.csv",
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
    snapshots.to_csv(
        output_directory / "subject_snapshots.csv",
        index=False,
    )
    plan.frontier_queue.to_csv(
        output_directory / "frontier_queue.csv",
        index=False,
    )

    counterparty_snapshot = snapshots.loc[
        snapshots["subject_type"].eq("COUNTERPARTY")
        & snapshots["subject_key"].str.endswith(
            COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
        )
    ].iloc[0]

    payload = json.loads(
        counterparty_snapshot["feature_payload_json"]
    )
    graph_summary = payload[
        "relationship_evidence_summary"
    ]
    behavioral_sampling = payload[
        "behavioral_evidence"
    ]["linked_customer_sampling"]

    telemetry = {
        "run_date": str(arguments.run_date),
        "counterparty_key": counterparty_snapshot[
            "subject_key"
        ],
        "candidate_customer_count": len(
            counterparty_result.candidate_customer_links
        ),
        "graph_relationship_count": graph_summary[
            "relationship_count"
        ],
        "graph_sampled_relationship_count": graph_summary[
            "sampled_relationship_count"
        ],
        "behavior_customer_population_count": (
            behavioral_sampling[
                "population_customer_count"
            ]
        ),
        "behavior_sampled_customer_count": (
            behavioral_sampling[
                "sampled_customer_count"
            ]
        ),
        "payload_bytes": len(
            counterparty_snapshot[
                "feature_payload_json"
            ].encode("utf-8")
        ),
        "counterparty_ai_actions_queued": int(
            plan.actionable_queue["action_type"]
            .eq("RUN_COUNTERPARTY_AI")
            .sum()
        ),
        "customer_ai_actions_queued": int(
            plan.actionable_queue["action_type"]
            .eq("RUN_CUSTOMER_AI")
            .sum()
        ),
        "live_ai_calls": 0,
    }

    (output_directory / "hub_breadth_telemetry.json").write_text(
        json.dumps(
            telemetry,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(
        "Scenario 2 hub discovery and evidence completed."
    )
    print(f"Output directory: {output_directory}")
    print(f"EID links: {len(eid_result.eid_links)}")
    print(
        "Candidate counterparties: "
        f"{len(counterparty_result.candidate_counterparties)}"
    )
    print(
        "Candidate customer links: "
        f"{len(counterparty_result.candidate_customer_links)}"
    )
    print(f"Unified nodes: {len(unified_result.nodes)}")
    print(f"Unified edges: {len(unified_result.edges)}")
    print(
        "Full graph relationships: "
        f"{graph_summary['relationship_count']}"
    )
    print(
        "Graph relationships in AI payload: "
        f"{graph_summary['sampled_relationship_count']}"
    )
    print(
        "Full behavioral customer population: "
        f"{behavioral_sampling['population_customer_count']}"
    )
    print(
        "Behavioral customer sample in AI payload: "
        f"{behavioral_sampling['sampled_customer_count']}"
    )
    print(f"AI payload bytes: {telemetry['payload_bytes']}")
    print(
        "Counterparty AI actions queued: "
        f"{telemetry['counterparty_ai_actions_queued']}"
    )
    print(
        "Customer AI actions queued: "
        f"{telemetry['customer_ai_actions_queued']}"
    )
    print("Live AI calls made: 0")
    print(
        "Feature files: "
        f"{COUNTERPARTY_PROFILE_FILENAME}, "
        f"{COUNTERPARTY_CUSTOMER_PROFILE_FILENAME}, "
        f"{COUNTERPARTY_PAYLOAD_FILENAME}"
    )


if __name__ == "__main__":
    main()
