"""Validate Scenario 3 beneficiary discovery without live AI."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    BENEFICIARY_LINK_EVIDENCE_ADD_ONLY,
    BENEFICIARY_LINK_EVIDENCE_PAYMENT_BACKED,
    discover_counterparty_candidates,
)
from network_mule_discovery.customer_behavioral_features import (
    CUSTOMER_ASSESSMENT_POLICY_VERSION,
    build_customer_behavioral_features,
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
    ADD_ONLY_CUSTOMER_ID,
    PAYMENT_BACKED_CUSTOMER_ID,
    PAYMENT_PAIR_COUNT,
    RUN_DATE,
    generate_scenario_3_source_data,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        canonical_directory = root / "canonical"
        output_directory = root / "output"
        state_directory = root / "state"

        generate_scenario_3_source_data(
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
            output_directory=output_directory,
        )

        eid_result = discover_entities_by_seed_eids(
            data_source=data_source,
            run_date=RUN_DATE,
        )
        counterparty_result = discover_counterparty_candidates(
            data_source=data_source,
            run_date=RUN_DATE,
        )
        unified_result = build_unified_seed_groups(
            eid_discovery=eid_result,
            counterparty_discovery=counterparty_result,
            run_date=RUN_DATE,
        )

        assert len(eid_result.eid_links) == 0
        assert counterparty_result.seed_counterparties.empty
        assert counterparty_result.candidate_customer_links.empty
        assert counterparty_result.candidate_counterparties.empty

        links = (
            counterparty_result.beneficiary_seed_links
            .set_index("candidate_customer_id")
        )
        assert set(links.index) == {
            PAYMENT_BACKED_CUSTOMER_ID,
            ADD_ONLY_CUSTOMER_ID,
        }
        assert links.loc[
            PAYMENT_BACKED_CUSTOMER_ID,
            "beneficiary_link_evidence_type",
        ] == BENEFICIARY_LINK_EVIDENCE_PAYMENT_BACKED
        assert bool(
            links.loc[
                PAYMENT_BACKED_CUSTOMER_ID,
                "beneficiary_payment_backed_flag",
            ]
        )
        assert int(
            links.loc[
                PAYMENT_BACKED_CUSTOMER_ID,
                "beneficiary_payment_event_count",
            ]
        ) == PAYMENT_PAIR_COUNT
        assert links.loc[
            ADD_ONLY_CUSTOMER_ID,
            "beneficiary_link_evidence_type",
        ] == BENEFICIARY_LINK_EVIDENCE_ADD_ONLY
        assert not bool(
            links.loc[
                ADD_ONLY_CUSTOMER_ID,
                "beneficiary_payment_backed_flag",
            ]
        )
        assert int(
            links.loc[
                ADD_ONLY_CUSTOMER_ID,
                "beneficiary_payment_event_count",
            ]
        ) == 0
        assert set(links["seed_account_match_type"]) == {
            "ACCOUNT"
        }

        assert len(unified_result.groups) == 1
        assert len(unified_result.nodes) == 3
        assert len(unified_result.edges) == 2
        assert not unified_result.nodes[
            "node_type"
        ].eq("COUNTERPARTY").any()
        assert unified_result.edges[
            "edge_type"
        ].eq("BENEFICIARY_ADDED_SEED_ACCOUNT").all()

        beneficiary_edges = unified_result.edges.loc[
            unified_result.edges[
                "edge_type"
            ].eq("BENEFICIARY_ADDED_SEED_ACCOUNT")
        ]
        assert beneficiary_edges[
            "candidate_event_count"
        ].astype("int64").eq(1).all()
        assert beneficiary_edges[
            "evidence_summary"
        ].eq(
            "Customer added a known seed mule account "
            "as beneficiary"
        ).all()

        state_store = CsvDailyStateStore(
            state_directory
        )
        state_store.save_network_state(
            network=unified_result,
            run_date=RUN_DATE,
        )
        graph_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
        )

        assert not graph_plan.actionable_queue[
            "action_type"
        ].eq("RUN_COUNTERPARTY_AI").any()
        graph_customer_queue = graph_plan.actionable_queue.loc[
            graph_plan.actionable_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI")
        ]
        customer_keys = sorted(
            graph_customer_queue[
                "subject_key"
            ].unique()
        )
        assert customer_keys == [
            "RETAIL|R3002",
            "SME|B3001",
        ]

        customer_features = build_customer_behavioral_features(
            source_directory=source_directory,
            customer_keys=customer_keys,
            projection=graph_plan.projection,
            run_date=RUN_DATE,
        )
        profiles = customer_features.customer_profiles.set_index(
            "subject_key"
        )
        assert profiles.loc[
            "RETAIL|R3002",
            "rapid_outward_share_2h_30d",
        ] == 1.0
        assert profiles.loc[
            "RETAIL|R3002",
            "flow_through_ratio_30d",
        ] == 0.9
        assert profiles.loc[
            "RETAIL|R3002",
            "distinct_inward_source_count_30d",
        ] == PAYMENT_PAIR_COUNT
        assert profiles.loc[
            "RETAIL|R3002",
            "deterministic_relationship_count",
        ] == 1
        assert profiles.loc[
            "SME|B3001",
            "all_time_inward_event_count",
        ] == 0
        assert profiles.loc[
            "SME|B3001",
            "all_time_outward_event_count",
        ] == 0
        assert profiles.loc[
            "SME|B3001",
            "deterministic_relationship_count",
        ] == 1

        for payload_json in customer_features.customer_payloads[
            "feature_payload_json"
        ]:
            payload = json.loads(payload_json)
            assert payload[
                "assessment_policy_version"
            ] == CUSTOMER_ASSESSMENT_POLICY_VERSION
            assert payload[
                "assessment_context"
            ]["deterministic_relationship_count"] == 1

        payload_text = "\n".join(
            customer_features.customer_payloads[
                "feature_payload_json"
            ].tolist()
        )
        for forbidden in (
            "MULE_LIKE",
            "EXPOSED_VULNERABLE",
            "LOW_CONCERN",
            "INSUFFICIENT_EVIDENCE",
        ):
            assert forbidden not in payload_text

        final_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
            supplemental_subject_payloads=(
                customer_features.customer_payloads
            ),
        )
        assert not final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_COUNTERPARTY_AI").any()
        final_customer_queue = final_plan.actionable_queue.loc[
            final_plan.actionable_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI")
        ]
        assert sorted(
            final_customer_queue[
                "subject_key"
            ].unique()
        ) == customer_keys
        assert not final_plan.actionable_queue[
            "action_type"
        ].eq("DISCOVER_CUSTOMER_RELATIONSHIPS").any()

    print("Scenario 3 beneficiary discovery smoke test passed.")
    print("Normalized EID links: 0")
    print("Shared-counterparty candidates: 0")
    print("Beneficiary-to-seed links: 2")
    print("Payment-backed links: 1")
    print("Add-only links: 1")
    print(f"Payment evidence events: {PAYMENT_PAIR_COUNT}")
    print("Unified graph nodes/edges: 3/2")
    print("Counterparty AI actions queued: 0")
    print("Customer AI actions queued: 2")
    print("Recursive sources queued before decisions: 0")
    print("Rapid-drain customer evidence: passed")
    print("Sparse add-only customer evidence: passed")
    print("Scenario labels in runtime payloads: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
