"""Validate Scenario 2 high-degree discovery and bounded evidence."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT,
    build_behavioral_features,
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
    COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT,
    build_subject_snapshots,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    NON_SEED_CUSTOMER_COUNT,
    RUN_DATE,
    TOTAL_CUSTOMER_COUNT,
    TOTAL_RECURRING_PAYMENT_COUNT,
    generate_scenario_2_source_data,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        canonical_directory = root / "canonical"
        discovery_directory = root / "discovery"
        state_directory = root / "state"

        generate_scenario_2_source_data(
            source_directory
        )
        paths = write_canonical_discovery_inputs(
            source_directory=source_directory,
            output_directory=canonical_directory,
            run_date=RUN_DATE,
        )

        data_source = CsvCounterpartyNetworkDataSource(
            seed_mule_pool_path=paths.seed_mule_pool_path,
            customer_identity_path=paths.customer_identity_path,
            seed_mule_events_path=paths.seed_mule_events_path,
            counterparty_events_path=paths.counterparty_events_path,
            output_directory=discovery_directory,
        )

        eid_result = discover_entities_by_seed_eids(
            data_source=data_source,
            run_date=RUN_DATE,
        )
        counterparty_result = (
            discover_counterparty_candidates(
                data_source=data_source,
                run_date=RUN_DATE,
            )
        )
        unified_result = build_unified_seed_groups(
            eid_discovery=eid_result,
            counterparty_discovery=(
                counterparty_result
            ),
            run_date=RUN_DATE,
        )

        assert eid_result.eid_links.empty
        assert len(
            counterparty_result.candidate_counterparties
        ) == 1
        assert len(
            counterparty_result.candidate_customer_links
        ) == NON_SEED_CUSTOMER_COUNT
        assert (
            counterparty_result.candidate_customer_links[
                "candidate_entity_key"
            ].nunique()
            == NON_SEED_CUSTOMER_COUNT
        )

        candidate_counterparty = (
            counterparty_result.candidate_counterparties
            .iloc[0]
        )
        counterparty_key = candidate_counterparty[
            "counterparty_key"
        ]

        assert int(
            candidate_counterparty[
                "candidate_customer_count"
            ]
        ) == NON_SEED_CUSTOMER_COUNT
        assert int(
            candidate_counterparty[
                "candidate_event_count"
            ]
        ) == (
            NON_SEED_CUSTOMER_COUNT * 5
        )
        assert int(
            candidate_counterparty[
                "seed_event_count"
            ]
        ) == 5

        assert len(unified_result.groups) == 1
        assert len(unified_result.nodes) == 502
        assert len(unified_result.edges) == 501

        group = unified_result.groups.iloc[0]
        assert int(group["customer_count"]) == (
            TOTAL_CUSTOMER_COUNT
        )
        assert int(group["counterparty_count"]) == 1
        assert int(group["eid_link_count"]) == 0
        assert int(
            group[
                "shared_counterparty_customer_count"
            ]
        ) == NON_SEED_CUSTOMER_COUNT
        assert int(
            group["customer_assessment_pending_count"]
        ) == 0
        assert int(
            group["counterparty_ai_pending_count"]
        ) == 1

        blocked_customers = unified_result.nodes.loc[
            unified_result.nodes["node_type"].eq(
                "CUSTOMER"
            )
            & unified_result.nodes[
                "customer_assessment_status"
            ].eq("BLOCKED_PENDING_COUNTERPARTY_AI")
        ]
        assert len(blocked_customers) == (
            NON_SEED_CUSTOMER_COUNT
        )

        features = build_behavioral_features(
            source_directory=source_directory,
            counterparty_keys=[counterparty_key],
            run_date=RUN_DATE,
        )

        assert len(features.counterparty_profiles) == 1
        assert len(
            features.counterparty_customer_profiles
        ) == TOTAL_CUSTOMER_COUNT

        profile = features.counterparty_profiles.iloc[0]
        assert int(profile["transfer_event_count"]) == (
            TOTAL_RECURRING_PAYMENT_COUNT
        )
        assert int(profile["distinct_customer_count"]) == (
            TOTAL_CUSTOMER_COUNT
        )
        assert int(profile["seed_customer_count"]) == 1
        assert int(profile["repeat_customer_count"]) == (
            TOTAL_CUSTOMER_COUNT
        )
        assert float(
            profile["top_customer_amount_share"]
        ) < 0.01
        assert float(
            profile["top_3_customer_amount_share"]
        ) < 0.03
        assert float(
            profile["beneficiary_created_last_30d_share"]
        ) == 0.0

        snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=unified_result.edges,
            supplemental_subject_payloads=(
                features.counterparty_payloads
            ),
        )
        repeated_snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=unified_result.edges,
            supplemental_subject_payloads=(
                features.counterparty_payloads
            ),
        )

        counterparty_snapshot = snapshots.loc[
            snapshots["subject_type"].eq(
                "COUNTERPARTY"
            )
            & snapshots["subject_key"].eq(
                counterparty_key
            )
        ].iloc[0]
        repeated_counterparty_snapshot = (
            repeated_snapshots.loc[
                repeated_snapshots[
                    "subject_type"
                ].eq("COUNTERPARTY")
                & repeated_snapshots[
                    "subject_key"
                ].eq(counterparty_key)
            ].iloc[0]
        )

        assert (
            counterparty_snapshot[
                "feature_snapshot_hash"
            ]
            == repeated_counterparty_snapshot[
                "feature_snapshot_hash"
            ]
        )
        assert int(
            counterparty_snapshot["relationship_count"]
        ) == 501

        payload_json = counterparty_snapshot[
            "feature_payload_json"
        ]
        payload = json.loads(payload_json)

        graph_summary = payload[
            "relationship_evidence_summary"
        ]
        assert graph_summary["payload_mode"] == (
            "BOUNDED_SAMPLE"
        )
        assert graph_summary[
            "relationship_count"
        ] == 501
        assert graph_summary[
            "sample_limit"
        ] == COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT
        assert graph_summary[
            "sampled_relationship_count"
        ] == COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT
        assert graph_summary[
            "omitted_relationship_count"
        ] == (
            501
            - COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT
        )
        assert graph_summary[
            "connected_subject_count"
        ] == TOTAL_CUSTOMER_COUNT
        assert graph_summary["edge_type_counts"] == {
            "SEED_COUNTERPARTY_EVIDENCE": 1,
            "SHARED_EXTERNAL_COUNTERPARTY": (
                NON_SEED_CUSTOMER_COUNT
            ),
        }
        assert len(
            graph_summary[
                "full_relationship_digest"
            ]
        ) == 64
        assert len(payload["relationships"]) == (
            COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT
        )
        assert sum(
            relationship["edge_type"]
            == "SEED_COUNTERPARTY_EVIDENCE"
            for relationship in payload[
                "relationships"
            ]
        ) == 1

        behavioral_payload = payload[
            "behavioral_evidence"
        ]
        behavioral_sampling = behavioral_payload[
            "linked_customer_sampling"
        ]
        assert behavioral_sampling[
            "population_customer_count"
        ] == TOTAL_CUSTOMER_COUNT
        assert behavioral_sampling[
            "sample_limit"
        ] == COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT
        assert behavioral_sampling[
            "sampled_customer_count"
        ] == COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT
        assert behavioral_sampling[
            "omitted_customer_count"
        ] == (
            TOTAL_CUSTOMER_COUNT
            - COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT
        )
        assert len(
            behavioral_sampling[
                "full_population_behavior_digest"
            ]
        ) == 64
        assert len(
            behavioral_payload[
                "highest_value_linked_customers"
            ]
        ) == COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT

        sampled_graph_customer_keys = {
            endpoint
            for relationship in payload[
                "relationships"
            ]
            for endpoint in (
                relationship["source_node_key"],
                relationship["target_node_key"],
            )
            if str(endpoint).startswith("CUSTOMER|")
        }
        sampled_behavior_customer_ids = {
            row["customer_id"]
            for row in behavioral_payload[
                "highest_value_linked_customers"
            ]
        }
        all_candidate_customer_keys = {
            f"CUSTOMER|{entity_key}"
            for entity_key in (
                counterparty_result
                .candidate_customer_links[
                    "candidate_entity_key"
                ]
            )
        }
        all_candidate_customer_ids = set(
            counterparty_result
            .candidate_customer_links[
                "candidate_customer_id"
            ]
        )

        omitted_node_keys = (
            all_candidate_customer_keys
            - sampled_graph_customer_keys
        )
        omitted_customer_ids = (
            all_candidate_customer_ids
            - sampled_behavior_customer_ids
        )
        assert omitted_node_keys
        assert omitted_customer_ids

        fully_omitted_customer_ids = {
            node_key.rsplit("|", 1)[-1]
            for node_key in omitted_node_keys
        } & omitted_customer_ids
        assert fully_omitted_customer_ids

        omitted_customer_id = sorted(
            fully_omitted_customer_ids
        )[0]
        assert omitted_customer_id not in payload_json
        assert len(payload_json.encode("utf-8")) < 100000

        mutated_edges = unified_result.edges.copy()
        omitted_edge_mask = (
            mutated_edges["edge_type"].eq(
                "SHARED_EXTERNAL_COUNTERPARTY"
            )
            & mutated_edges["target_node_key"].eq(
                next(iter(omitted_node_keys))
            )
        )
        assert omitted_edge_mask.sum() == 1
        mutated_edges.loc[
            omitted_edge_mask,
            "evidence_summary",
        ] = "CHANGED_UNSAMPLED_RELATIONSHIP_EVIDENCE"

        changed_snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=mutated_edges,
            supplemental_subject_payloads=(
                features.counterparty_payloads
            ),
        )
        changed_counterparty_snapshot = (
            changed_snapshots.loc[
                changed_snapshots[
                    "subject_type"
                ].eq("COUNTERPARTY")
                & changed_snapshots[
                    "subject_key"
                ].eq(counterparty_key)
            ].iloc[0]
        )
        assert (
            changed_counterparty_snapshot[
                "feature_snapshot_hash"
            ]
            != counterparty_snapshot[
                "feature_snapshot_hash"
            ]
        )

        state_store = CsvDailyStateStore(
            state_directory
        )
        state_store.save_network_state(
            network=unified_result,
            run_date=RUN_DATE,
        )
        plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
            supplemental_subject_payloads=(
                features.counterparty_payloads
            ),
        )

        counterparty_queue = (
            plan.actionable_queue.loc[
                plan.actionable_queue[
                    "action_type"
                ].eq("RUN_COUNTERPARTY_AI")
            ]
        )
        customer_queue = (
            plan.actionable_queue.loc[
                plan.actionable_queue[
                    "action_type"
                ].eq("RUN_CUSTOMER_AI")
            ]
        )

        assert len(counterparty_queue) == 1
        assert len(customer_queue) == 0
        assert counterparty_queue.iloc[0][
            "subject_key"
        ] == counterparty_key

    print(
        "Scenario 2 hub discovery and evidence smoke test passed."
    )
    print("Normalized EID links: 0")
    print("Candidate counterparties: 1")
    print(
        "Non-seed linked customers observed: "
        f"{NON_SEED_CUSTOMER_COUNT}"
    )
    print("Unified graph nodes/edges: 502/501")
    print("Full counterparty relationships: 501")
    print(
        "Graph relationships in AI payload: "
        f"{COUNTERPARTY_GRAPH_RELATIONSHIP_SAMPLE_LIMIT}"
    )
    print(
        "Behavioral customer population: "
        f"{TOTAL_CUSTOMER_COUNT}"
    )
    print(
        "Behavioral customers in AI payload: "
        f"{COUNTERPARTY_LINKED_CUSTOMER_SAMPLE_LIMIT}"
    )
    print("Unsampled evidence hash protection: passed")
    print("Counterparty AI actions queued: 1")
    print("Customer AI actions queued: 0")
    print("Planning-only AI calls: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
