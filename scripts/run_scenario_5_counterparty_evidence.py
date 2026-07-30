"""Run Scenario 5 discovery and planning without live AI."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.behavioral_features import write_behavioral_features
from network_mule_discovery.counterparty_data_sources import CsvCounterpartyNetworkDataSource
from network_mule_discovery.counterparty_discovery import discover_counterparty_candidates
from network_mule_discovery.daily_state import CsvDailyStateStore, build_incremental_daily_plan
from network_mule_discovery.decision_engine import build_subject_snapshots
from network_mule_discovery.eid_discovery import discover_entities_by_seed_eids
from network_mule_discovery.raw_source_adapter import write_canonical_discovery_inputs
from network_mule_discovery.scenario_5_synthetic_data import (
    AMBIGUOUS_COUNTERPARTY_ACCOUNT,
    RUN_DATE,
)
from network_mule_discovery.unified_group_builder import build_unified_seed_groups


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=PROJECT_ROOT / "data" / "synthetic" / "scenario_5",
    )
    parser.add_argument(
        "--work-directory",
        type=Path,
        default=PROJECT_ROOT / "data" / "synthetic" / "scenario_5" / "runtime",
    )
    parser.add_argument("--run-date", default=str(RUN_DATE))
    args = parser.parse_args()

    canonical_directory = args.work_directory / "canonical"
    output_directory = args.work_directory / "counterparty_evidence"
    feature_directory = output_directory / "features"
    state_directory = output_directory / "state"

    paths = write_canonical_discovery_inputs(
        source_directory=args.source_directory,
        output_directory=canonical_directory,
        run_date=args.run_date,
    )
    data_source = CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=paths.seed_mule_pool_path,
        customer_identity_path=paths.customer_identity_path,
        seed_mule_events_path=paths.seed_mule_events_path,
        counterparty_events_path=paths.counterparty_events_path,
        output_directory=output_directory,
    )
    eid_result = discover_entities_by_seed_eids(data_source=data_source, run_date=args.run_date)
    counterparty_result = discover_counterparty_candidates(data_source=data_source, run_date=args.run_date)
    unified_result = build_unified_seed_groups(
        eid_discovery=eid_result,
        counterparty_discovery=counterparty_result,
        run_date=args.run_date,
    )
    counterparty_keys = sorted(
        counterparty_result.candidate_counterparties["counterparty_key"]
        .dropna().drop_duplicates().tolist()
    )
    features = write_behavioral_features(
        source_directory=args.source_directory,
        counterparty_keys=counterparty_keys,
        output_directory=feature_directory,
        run_date=args.run_date,
    )
    snapshots = build_subject_snapshots(
        nodes=unified_result.nodes,
        edges=unified_result.edges,
        supplemental_subject_payloads=features.counterparty_payloads,
    )
    state_store = CsvDailyStateStore(state_directory)
    state_store.save_network_state(network=unified_result, run_date=args.run_date)
    plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=args.run_date,
        supplemental_subject_payloads=features.counterparty_payloads,
    )

    output_directory.mkdir(parents=True, exist_ok=True)
    unified_result.groups.to_csv(output_directory / "unified_groups.csv", index=False)
    unified_result.nodes.to_csv(output_directory / "unified_nodes.csv", index=False)
    unified_result.edges.to_csv(output_directory / "unified_edges.csv", index=False)
    snapshots.to_csv(output_directory / "subject_snapshots.csv", index=False)
    plan.frontier_queue.to_csv(output_directory / "frontier_queue.csv", index=False)

    target = snapshots.loc[
        snapshots["subject_type"].eq("COUNTERPARTY")
        & snapshots["subject_key"].str.endswith(AMBIGUOUS_COUNTERPARTY_ACCOUNT)
    ].iloc[0]
    payload = json.loads(target["feature_payload_json"])
    behavior = payload["behavioral_evidence"]
    telemetry = {
        "run_date": str(args.run_date),
        "counterparty_key": target["subject_key"],
        "candidate_customer_count": len(counterparty_result.candidate_customer_links),
        "observed_node_count": len(unified_result.nodes),
        "observed_edge_count": len(unified_result.edges),
        "counterparty_ai_actions_queued": int(
            plan.actionable_queue["action_type"].eq("RUN_COUNTERPARTY_AI").sum()
        ),
        "customer_ai_actions_queued": int(
            plan.actionable_queue["action_type"].eq("RUN_CUSTOMER_AI").sum()
        ),
        "transfer_event_count": int(behavior["aggregate_behavior"]["transfer_event_count"]),
        "distinct_customer_count": int(behavior["aggregate_behavior"]["distinct_customer_count"]),
        "feature_snapshot_hash": target["feature_snapshot_hash"],
        "live_ai_calls": 0,
    }
    (output_directory / "counterparty_evidence_telemetry.json").write_text(
        json.dumps(telemetry, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print("Scenario 5 counterparty evidence completed.")
    print(f"EID links: {len(eid_result.eid_links)}")
    print(f"Candidate counterparties: {len(counterparty_result.candidate_counterparties)}")
    print(f"Candidate customer links: {len(counterparty_result.candidate_customer_links)}")
    print(f"Observed graph nodes/edges: {len(unified_result.nodes)}/{len(unified_result.edges)}")
    print(f"Counterparty AI actions queued: {telemetry['counterparty_ai_actions_queued']}")
    print(f"Customer AI actions queued: {telemetry['customer_ai_actions_queued']}")
    print("Live AI calls made: 0")


if __name__ == "__main__":
    main()
