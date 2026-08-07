"""Validate Scenario 1's recursive customer frontier offline."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (PROJECT_ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    run_recursive_counterparty_frontier,
)
from network_mule_discovery.recursive_customer_frontier import (
    run_recursive_customer_frontier,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)
from smoke_test_scenario_1_recursive_counterparty_frontier import (
    RecursiveCounterpartyAdapter,
    build_initial_state,
    decision_record,
)


class RecursiveCustomerAdapter:
    """Return deterministic second-layer customer decisions."""

    decisions = {
        "RETAIL|R1005": (
            "MULE_LIKE",
            "SYNTHETIC_REPEATED_RAPID_DRAIN",
        ),
        "RETAIL|R1006": (
            "EXPOSED_VULNERABLE",
            "SYNTHETIC_SALARY_BASELINE_EXPOSURE",
        ),
        "RETAIL|R1007": (
            "INSUFFICIENT_EVIDENCE",
            "SYNTHETIC_SPARSE_ONE_OFF_FLOW",
        ),
    }

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.last_call_metadata: dict[str, object] = {}

    def decide(
        self,
        *,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        feature_payload_json: str,
        run_date: date,
        round_number: int,
        sequence_number: int,
    ) -> dict[str, str]:
        assert subject_type == "CUSTOMER"
        payload = json.loads(feature_payload_json)
        assert payload["subject_key"] == subject_key
        assert payload[
            "behavioral_evidence"
        ]["assessment_policy_version"] == (
            "customer-assessment-policy-v2"
        )

        self.calls.append(subject_key)
        decision, reason_code = self.decisions[subject_key]
        self.last_call_metadata = {
            "assessment": {
                "confidence": "HIGH",
                "rationale": (
                    "Production-shaped recursive customer evidence "
                    "was assessed in an isolated test."
                ),
                "key_evidence": [
                    "Bounded customer evidence payload present."
                ],
            },
            "model": "test-model",
            "prompt_version": "test-v3",
        }

        return decision_record(
            subject_type=subject_type,
            subject_key=subject_key,
            feature_snapshot_hash=feature_snapshot_hash,
            decision=decision,
            reason_code=reason_code,
        )


def forbidden_factory() -> object:
    raise AssertionError(
        "An unchanged or planning-only frontier created an adapter."
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        (
            source_directory,
            state_directory,
            initial_network,
            existing_payloads,
        ) = build_initial_state(root)
        recursive_counterparty_result = (
            run_recursive_counterparty_frontier(
                source_directory=source_directory,
                state_directory=state_directory,
                run_date=RUN_DATE,
                supplemental_subject_payloads=(
                    existing_payloads
                ),
                settings=DailyAiSettings(
                    live_ai_enabled=True,
                    daily_call_limit=1,
                    run_call_limit=1,
                ),
                adapter_factory=(
                    RecursiveCounterpartyAdapter
                ),
            )
        )
        recursive_counterparty_payloads = (
            recursive_counterparty_result
            .new_features.counterparty_payloads
        )
        combined_payloads = pd.concat(
            [
                existing_payloads,
                recursive_counterparty_payloads,
            ],
            ignore_index=True,
        )
        state_store = CsvDailyStateStore(state_directory)
        customer_ready_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
            supplemental_subject_payloads=combined_payloads,
        )
        customer_queue = (
            customer_ready_plan.actionable_queue.loc[
                customer_ready_plan.actionable_queue[
                    "action_type"
                ].eq("RUN_CUSTOMER_AI")
            ]
        )
        expected_customer_keys = (
            "RETAIL|R1005",
            "RETAIL|R1006",
            "RETAIL|R1007",
        )

        assert tuple(
            sorted(customer_queue["subject_key"])
        ) == expected_customer_keys
        assert not customer_ready_plan.actionable_queue[
            "action_type"
        ].eq("RUN_COUNTERPARTY_AI").any()

        planning_result = run_recursive_customer_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=combined_payloads,
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=3,
                run_call_limit=3,
            ),
            adapter_factory=forbidden_factory,
        )
        assert planning_result.customer_keys == (
            expected_customer_keys
        )
        assert (
            planning_result.customer_frontier
            .controlled_run.calls_executed
            == 0
        )
        profiles = planning_result.new_features.customer_profiles
        profile_lookup = profiles.set_index("subject_key")

        assert int(
            profile_lookup.loc[
                "RETAIL|R1005",
                "distinct_inward_source_count_30d",
            ]
        ) == 8
        assert float(
            profile_lookup.loc[
                "RETAIL|R1005",
                "rapid_outward_share_2h_30d",
            ]
        ) == 1.0
        assert int(
            profile_lookup.loc[
                "RETAIL|R1005",
                "outward_event_count_30d",
            ]
        ) == 5
        assert int(
            profile_lookup.loc[
                "RETAIL|R1006",
                "salary_month_count_365d",
            ]
        ) == 11
        assert float(
            profile_lookup.loc[
                "RETAIL|R1006",
                "salary_regular_amount_share_365d",
            ]
        ) == 1.0
        assert int(
            profile_lookup.loc[
                "RETAIL|R1007",
                "inward_event_count_30d",
            ]
        ) == 1
        assert int(
            profile_lookup.loc[
                "RETAIL|R1007",
                "outward_event_count_30d",
            ]
        ) == 1
        assert set(
            profiles[
                "approved_suspicious_counterparty_count"
            ]
        ) == {1}
        payload_text = "\n".join(
            planning_result.new_features.customer_payloads[
                "feature_payload_json"
            ]
        ).lower()

        for forbidden_text in [
            "expected decision",
            "expected outcome",
            "fraud_flag",
            "legitimate_flag",
        ]:
            assert forbidden_text not in payload_text

        live_result = run_recursive_customer_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=combined_payloads,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=4,
                run_call_limit=3,
            ),
            customer_keys=expected_customer_keys,
            adapter_factory=RecursiveCustomerAdapter,
        )
        controlled_run = (
            live_result.customer_frontier.controlled_run
        )
        assert controlled_run.calls_executed == 3
        current_decisions = live_result.decision_store.loc[
            live_result.decision_store["subject_type"].eq(
                "CUSTOMER"
            )
            & live_result.decision_store["subject_key"].isin(
                expected_customer_keys
            )
        ]
        decision_lookup = dict(
            zip(
                current_decisions["subject_key"],
                current_decisions["decision"],
            )
        )
        assert decision_lookup == {
            "RETAIL|R1005": "MULE_LIKE",
            "RETAIL|R1006": "EXPOSED_VULNERABLE",
            "RETAIL|R1007": "INSUFFICIENT_EVIDENCE",
        }
        recursive_queue = (
            controlled_run.final_plan.actionable_queue.loc[
                controlled_run.final_plan.actionable_queue[
                    "action_type"
                ].eq("DISCOVER_CUSTOMER_RELATIONSHIPS")
            ]
        )
        assert sorted(
            recursive_queue["subject_key"]
            .astype("string")
            .tolist()
        ) == [
            "RETAIL|R1005",
            "SME|B2001",
        ]
        assert not controlled_run.final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI").any()
        assert not controlled_run.final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_COUNTERPARTY_AI").any()
        telemetry = live_result.guardrail_telemetry.iloc[0]
        assert int(telemetry["max_observed_depth"]) == 4
        assert int(telemetry["total_node_count"]) == 108
        assert int(telemetry["total_edge_count"]) == 109
        assert int(telemetry["current_frontier_width"]) == 2
        assert int(telemetry["expansion_source_count"]) == 4
        assert int(telemetry["new_node_count"]) == 0
        assert int(telemetry["new_edge_count"]) == 0
        assert telemetry["guardrail_status"] == (
            "TELEMETRY_ONLY"
        )

        all_payloads = pd.concat(
            [
                combined_payloads,
                live_result.new_features.customer_payloads,
            ],
            ignore_index=True,
        )
        repeated_result = run_recursive_customer_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=all_payloads,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=4,
                run_call_limit=3,
            ),
            customer_keys=expected_customer_keys,
            adapter_factory=forbidden_factory,
        )
        assert (
            repeated_result.customer_frontier
            .controlled_run.calls_executed
            == 0
        )
        repeated_recursive_queue = (
            repeated_result.customer_frontier
            .controlled_run.final_plan.actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                )
            ]
        )
        assert sorted(
            repeated_recursive_queue["subject_key"]
            .astype("string")
            .tolist()
        ) == [
            "RETAIL|R1005",
            "SME|B2001",
        ]
        snapshot = state_store.load_snapshot()
        assert len(snapshot.network.nodes) == (
            len(initial_network.nodes) + 4
        )
        assert len(snapshot.network.edges) == (
            len(initial_network.edges) + 4
        )

        print(
            "Scenario 1 recursive customer frontier smoke test passed."
        )
        print("Recursive customer behavioral profiles: 3")
        print("Repeated rapid-drain evidence: passed")
        print("Stable-salary exposure evidence: passed")
        print("Sparse one-off evidence: passed")
        print("Counterparty phase barrier: passed")
        print("Customer AI actions executed in test: 3")
        print("Further recursive sources queued: 2")
        print("Graph nodes/edges unchanged: passed")
        print("Current frontier width: 2")
        print("Maximum observed depth: 4")
        print("Guardrail mode: telemetry only")
        print("Unchanged repeated customer calls: 0")
        print("Scenario labels in runtime payloads: 0")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
