"""Validate Scenario 4 deterministic EID-only grouping offline."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.scenario_4_synthetic_data import (
    GROUP_A_SEED_ID,
    GROUP_A_SME_IDS,
    GROUP_B_RETAIL_ID,
    GROUP_B_SEED_ID,
    RUN_DATE,
    generate_scenario_4_source_data,
)
from run_scenario_4_eid_only_groups import (
    build_scenario_4_network,
    terminate_scenario_4_groups,
)


def _group_members(
    nodes: pd.DataFrame,
    seed_entity_key: str,
) -> set[str]:
    seed_rows = nodes.loc[
        nodes["entity_key"].eq(seed_entity_key)
    ]
    assert len(seed_rows) == 1
    group_id = seed_rows.iloc[0]["group_id"]

    return set(
        nodes.loc[
            nodes["group_id"].eq(group_id),
            "entity_key",
        ]
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        runtime_directory = root / "runtime"
        canonical_directory = runtime_directory / "canonical"
        output_directory = runtime_directory / "output"
        state_directory = runtime_directory / "state"

        generate_scenario_4_source_data(
            source_directory
        )
        (
            eid_result,
            counterparty_result,
            unified_result,
        ) = build_scenario_4_network(
            source_directory=source_directory,
            canonical_directory=canonical_directory,
            discovery_output_directory=output_directory,
            run_date=str(RUN_DATE),
        )

        assert len(
            eid_result.seed_resolution.seed_entities
        ) == 2
        assert len(eid_result.discovered_entities) == 3
        assert len(eid_result.eid_links) == 3
        assert counterparty_result.candidate_counterparties.empty
        assert counterparty_result.beneficiary_seed_links.empty
        assert len(unified_result.groups) == 2
        assert len(unified_result.nodes) == 5
        assert len(unified_result.edges) == 3
        assert sorted(
            unified_result.groups["customer_count"].tolist()
        ) == [2, 3]
        assert unified_result.edges[
            "edge_type"
        ].eq("SAME_EMIRATES_ID").all()
        assert not unified_result.edges[
            "customer_discovery_allowed_flag"
        ].any()

        group_a_members = _group_members(
            unified_result.nodes,
            f"RETAIL|{GROUP_A_SEED_ID}",
        )
        assert group_a_members == {
            f"RETAIL|{GROUP_A_SEED_ID}",
            *(f"SME|{customer_id}" for customer_id in GROUP_A_SME_IDS),
        }
        group_b_members = _group_members(
            unified_result.nodes,
            f"SME|{GROUP_B_SEED_ID}",
        )
        assert group_b_members == {
            f"SME|{GROUP_B_SEED_ID}",
            f"RETAIL|{GROUP_B_RETAIL_ID}",
        }

        non_seed_nodes = unified_result.nodes.loc[
            ~unified_result.nodes["node_roles"]
            .astype("string")
            .str.contains("SEED_MULE")
        ]
        assert len(non_seed_nodes) == 3
        assert non_seed_nodes[
            "customer_assessment_status"
        ].eq("NOT_APPLICABLE").all()
        assert not non_seed_nodes[
            "customer_discovery_allowed_flag"
        ].any()
        assert not non_seed_nodes[
            "expansion_source_flag"
        ].any()

        state_store = CsvDailyStateStore(
            state_directory
        )
        state_store.save_network_state(
            network=unified_result,
            run_date=RUN_DATE,
        )
        initial_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
        )
        assert initial_plan.actionable_queue.empty
        assert initial_plan.queued_ai_action_count == 0
        assert initial_plan.queued_expansion_action_count == 0
        assert initial_plan.failed_closed_item_count == 0

        first = terminate_scenario_4_groups(
            state_directory=state_directory,
            run_date=str(RUN_DATE),
        )
        assert first.final_plan.actionable_queue.empty
        assert first.final_plan.failed_closed_item_count == 0
        assert len(first.termination_status) == 2
        assert first.termination_status[
            "termination_status"
        ].eq("TERMINATED").all()
        assert first.termination_status[
            "termination_reason"
        ].eq("FRONTIER_EXHAUSTED").all()
        assert set(
            first.termination_status["total_node_count"]
        ) == {2, 3}
        assert set(
            first.termination_status["total_edge_count"]
        ) == {1, 2}

        first_snapshot = state_store.load_snapshot()
        first_group_ids = tuple(
            sorted(
                first_snapshot.network.groups[
                    "group_id"
                ].tolist()
            )
        )
        assert first_snapshot.decision_store.empty
        assert first_snapshot.expansion_ledger.empty
        assert first_snapshot.frontier_queue.empty

        second = terminate_scenario_4_groups(
            state_directory=state_directory,
            run_date=str(RUN_DATE),
        )
        second_snapshot = state_store.load_snapshot()
        assert second.final_plan.actionable_queue.empty
        assert second.final_plan.failed_closed_item_count == 0
        assert tuple(
            sorted(
                second_snapshot.network.groups[
                    "group_id"
                ].tolist()
            )
        ) == first_group_ids
        assert len(second_snapshot.network.nodes) == 5
        assert len(second_snapshot.network.edges) == 3
        assert second_snapshot.decision_store.empty
        assert second_snapshot.expansion_ledger.empty
        assert second_snapshot.frontier_queue.empty

        repeated_eid, repeated_cp, repeated_network = (
            build_scenario_4_network(
                source_directory=source_directory,
                canonical_directory=(
                    runtime_directory / "canonical_repeat"
                ),
                discovery_output_directory=(
                    runtime_directory / "output_repeat"
                ),
                run_date=str(RUN_DATE),
            )
        )
        assert len(repeated_eid.eid_links) == 3
        assert repeated_cp.candidate_counterparties.empty
        assert tuple(
            sorted(repeated_network.groups["group_id"])
        ) == first_group_ids

    print("Scenario 4 EID-only group smoke test passed.")
    print("Confirmed seeds: 2")
    print("Stable groups: 2")
    print("Group sizes: 3 and 2")
    print("EID links: 3")
    print("Observed graph nodes/edges: 5/3")
    print("Counterparty AI actions queued: 0")
    print("Customer AI actions queued: 0")
    print("Recursive sources queued: 0")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Stable deterministic rerun: passed")
    print("Repeated AI calls: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
