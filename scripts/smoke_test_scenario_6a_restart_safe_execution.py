"""Validate Scenario 6A restart-safe exact-once AI execution."""

from __future__ import annotations

import hashlib
import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

try:
    from openai import OpenAI as _OpenAI  # noqa: F401
except (ImportError, AttributeError):
    import types

    openai_stub = types.ModuleType("openai")

    class _OfflineOpenAIStub:
        pass

    openai_stub.OpenAI = _OfflineOpenAIStub
    sys.modules["openai"] = openai_stub


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
    run_controlled_daily_ai,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_termination import (
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.operational_resilience import (
    validate_persisted_operational_state,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


RUN_DATE = date(2026, 7, 21)
GROUP_ID = "G-SCENARIO-6A"
SEED_ENTITY_KEY = "RETAIL|R6001"
COUNTERPARTY_KEYS = (
    "LOCAL_ACCOUNT|760000000001",
    "LOCAL_ACCOUNT|760000000002",
)


class RestartSafeSuppressingAdapter:
    """Return deterministic suppression decisions for queued subjects."""

    def __init__(self, adapter_name: str) -> None:
        self.adapter_name = adapter_name
        self.calls: list[str] = []

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
        assert subject_type == "COUNTERPARTY"
        assert subject_key in COUNTERPARTY_KEYS
        assert feature_payload_json
        assert run_date == RUN_DATE
        assert round_number == 1
        assert sequence_number == 1

        self.calls.append(subject_key)

        digest = hashlib.sha256(
            "|".join(
                [
                    subject_type,
                    subject_key,
                    feature_snapshot_hash,
                ]
            ).encode("utf-8")
        ).hexdigest()[:16]

        return {
            "decision_id": f"S6A{digest}",
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": feature_snapshot_hash,
            "decision": "LEGITIMATE_SUPPRESS",
            "reason_code": "SCENARIO_6A_RESTART_SAFE_SUPPRESSION",
            "decision_version": "scenario-6a-test-v1",
            "decided_at": "2026-07-21 12:00:00",
            "source": self.adapter_name,
        }


def forbidden_factory() -> object:
    raise AssertionError(
        "An exhausted restart must not instantiate the adapter."
    )


def build_restart_safe_network() -> UnifiedGroupResult:
    """Build one seed group with two independent AI counterparties."""
    run_id = "scenario_6a_20260721"
    run_date = str(RUN_DATE)
    seed_node_key = f"CUSTOMER|{SEED_ENTITY_KEY}"

    nodes = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_date": run_date,
                "group_id": GROUP_ID,
                "node_id": "N-S6A-SEED",
                "node_key": seed_node_key,
                "node_type": "CUSTOMER",
                "entity_type": "RETAIL",
                "entity_id": "R6001",
                "entity_key": SEED_ENTITY_KEY,
                "counterparty_key": "",
                "display_label": SEED_ENTITY_KEY,
                "node_roles": "SEED_MULE",
                "node_status": "SEED_EXPANSION_SOURCE",
                "customer_assessment_status": "SEED_CONFIRMED",
                "customer_discovery_allowed_flag": True,
                "expansion_source_flag": True,
                "first_seen_date": run_date,
                "last_seen_date": run_date,
            },
            *[
                {
                    "run_id": run_id,
                    "run_date": run_date,
                    "group_id": GROUP_ID,
                    "node_id": f"N-S6A-CP-{index}",
                    "node_key": f"COUNTERPARTY|{counterparty_key}",
                    "node_type": "COUNTERPARTY",
                    "entity_type": "",
                    "entity_id": "",
                    "entity_key": "",
                    "counterparty_key": counterparty_key,
                    "display_label": f"Scenario 6A Counterparty {index}",
                    "node_roles": "EXTERNAL_COUNTERPARTY_CANDIDATE",
                    "node_status": "OBSERVED_PENDING_COUNTERPARTY_AI",
                    "customer_assessment_status": "NOT_APPLICABLE",
                    "customer_discovery_allowed_flag": False,
                    "expansion_source_flag": False,
                    "first_seen_date": run_date,
                    "last_seen_date": run_date,
                }
                for index, counterparty_key in enumerate(
                    COUNTERPARTY_KEYS,
                    start=1,
                )
            ],
        ]
    )

    edges = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_date": run_date,
                "group_id": GROUP_ID,
                "edge_id": f"E-S6A-{index}",
                "source_node_id": "N-S6A-SEED",
                "target_node_id": f"N-S6A-CP-{index}",
                "source_node_key": seed_node_key,
                "target_node_key": f"COUNTERPARTY|{counterparty_key}",
                "edge_type": "SEED_COUNTERPARTY_EVIDENCE",
                "relationship_status": "COUNTERPARTY_CANDIDATE",
                "customer_discovery_allowed_flag": False,
                "recursive_expansion_allowed_flag": False,
                "evidence_key": f"S6A-EVIDENCE-{index}",
                "evidence_summary": "Synthetic restart-safe seed payment",
                "source_event_count": 1,
                "candidate_event_count": 1,
                "first_seen_date": run_date,
                "last_seen_date": run_date,
            }
            for index, counterparty_key in enumerate(
                COUNTERPARTY_KEYS,
                start=1,
            )
        ]
    )

    groups = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_date": run_date,
                "group_id": GROUP_ID,
                "group_anchor_seed_entity_key": SEED_ENTITY_KEY,
                "group_status": "ACTIVE",
                "seed_entity_count": 1,
                "customer_count": 1,
                "counterparty_count": 2,
                "eid_link_count": 0,
                "counterparty_candidate_count": 2,
                "shared_counterparty_customer_count": 0,
                "beneficiary_seed_link_count": 0,
                "customer_assessment_pending_count": 0,
                "counterparty_ai_pending_count": 2,
                "recursive_expansion_source_count": 1,
                "total_node_count": 3,
                "total_edge_count": 2,
                "first_seen_date": run_date,
                "last_seen_date": run_date,
            }
        ]
    )

    return UnifiedGroupResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
    )


def subject_hashes(plan) -> tuple[tuple[str, str, str], ...]:
    """Return deterministic feature identities for comparison."""
    rows = plan.projection.subject_snapshots[
        [
            "subject_type",
            "subject_key",
            "feature_snapshot_hash",
        ]
    ].astype("string")

    return tuple(
        sorted(
            tuple(row)
            for row in rows.itertuples(
                index=False,
                name=None,
            )
        )
    )


def main() -> None:
    """Prove bounded partial execution resumes exactly once from disk."""
    with TemporaryDirectory() as directory:
        state_directory = Path(directory)
        state_store = CsvDailyStateStore(state_directory)
        state_store.save_network_state(
            network=build_restart_safe_network(),
            run_date=RUN_DATE,
        )

        initial_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
        )
        assert initial_plan.queued_ai_action_count == 2
        assert initial_plan.queued_expansion_action_count == 0

        initial_node_ids = tuple(
            sorted(
                state_store.load_snapshot()
                .network.nodes["node_id"]
                .astype("string")
            )
        )
        initial_edge_ids = tuple(
            sorted(
                state_store.load_snapshot()
                .network.edges["edge_id"]
                .astype("string")
            )
        )
        initial_subject_hashes = subject_hashes(initial_plan)

        first_adapter = RestartSafeSuppressingAdapter(
            "SCENARIO_6A_FIRST_PROCESS"
        )
        first = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=1,
            ),
            adapter_factory=lambda: first_adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert first.calls_before_run == 0
        assert first.calls_executed == 1
        assert len(first_adapter.calls) == 1
        assert first.final_plan.queued_ai_action_count == 1

        first_snapshot = CsvDailyStateStore(
            state_directory
        ).load_snapshot()
        first_ledger = CsvAiCallLedger(
            state_directory
        ).load()
        first_integrity = validate_persisted_operational_state(
            snapshot=first_snapshot,
            ai_call_ledger=first_ledger,
        )
        assert first_integrity.decision_count == 1
        assert first_integrity.ai_call_count == 1
        assert first_integrity.completed_ai_outcome_count == 1

        second_adapter = RestartSafeSuppressingAdapter(
            "SCENARIO_6A_RESTARTED_PROCESS"
        )
        second = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=1,
            ),
            adapter_factory=lambda: second_adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert second.calls_before_run == 1
        assert second.calls_executed == 1
        assert len(second_adapter.calls) == 1
        assert second_adapter.calls[0] != first_adapter.calls[0]
        assert set(first_adapter.calls + second_adapter.calls) == set(
            COUNTERPARTY_KEYS
        )
        assert second.final_plan.actionable_queue.empty

        second_store = CsvDailyStateStore(state_directory)
        second_snapshot = second_store.load_snapshot()
        second_ledger = CsvAiCallLedger(
            state_directory
        ).load()
        second_integrity = validate_persisted_operational_state(
            snapshot=second_snapshot,
            ai_call_ledger=second_ledger,
        )
        assert second_integrity.decision_count == 2
        assert second_integrity.ai_call_count == 2
        assert second_integrity.completed_ai_outcome_count == 2
        assert second_integrity.expansion_ledger_count == 0
        assert second_integrity.frontier_queue_count == 0

        final_plan_before_termination = (
            build_incremental_daily_plan(
                state_store=second_store,
                run_date=RUN_DATE,
            )
        )
        assert subject_hashes(
            final_plan_before_termination
        ) == initial_subject_hashes
        assert tuple(
            sorted(
                second_snapshot.network.nodes["node_id"]
                .astype("string")
            )
        ) == initial_node_ids
        assert tuple(
            sorted(
                second_snapshot.network.edges["edge_id"]
                .astype("string")
            )
        ) == initial_edge_ids

        termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=None,
            group_ids=[GROUP_ID],
            source_entity_key=SEED_ENTITY_KEY,
        )
        assert termination.final_plan.actionable_queue.empty
        assert termination.final_plan.failed_closed_item_count == 0
        termination_row = termination.termination_status.iloc[0]
        assert termination_row["termination_status"] == "TERMINATED"
        assert termination_row["termination_reason"] == "FRONTIER_EXHAUSTED"

        third = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=3,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert third.calls_before_run == 2
        assert third.calls_executed == 0
        assert third.final_plan.actionable_queue.empty

        repeated_termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=None,
            group_ids=[GROUP_ID],
            source_entity_key=SEED_ENTITY_KEY,
        )
        assert repeated_termination.final_plan.actionable_queue.empty

        final_snapshot = CsvDailyStateStore(
            state_directory
        ).load_snapshot()
        final_ledger = CsvAiCallLedger(
            state_directory
        ).load()
        final_integrity = validate_persisted_operational_state(
            snapshot=final_snapshot,
            ai_call_ledger=final_ledger,
        )
        assert final_integrity.decision_count == 2
        assert final_integrity.ai_call_count == 2
        assert final_integrity.completed_ai_outcome_count == 2
        assert final_integrity.expansion_ledger_count == 0
        assert final_integrity.frontier_queue_count == 0
        assert tuple(
            sorted(
                final_snapshot.network.nodes["node_id"]
                .astype("string")
            )
        ) == initial_node_ids
        assert tuple(
            sorted(
                final_snapshot.network.edges["edge_id"]
                .astype("string")
            )
        ) == initial_edge_ids
        assert subject_hashes(
            third.final_plan
        ) == initial_subject_hashes

    print("Scenario 6A restart-safe execution smoke test passed.")
    print("Initial queued AI actions: 2")
    print("First bounded run completed AI actions: 1")
    print("Restart repeated completed AI actions: 0")
    print("Restart completed remaining AI actions: 1")
    print("Completed decision rows: 2 unique")
    print("AI call ledger rows: 2 unique")
    print("Duplicate node/edge/queue identifiers: 0")
    print("Stable node and edge IDs: passed")
    print("Stable feature snapshot hashes: passed")
    print("Third unchanged run AI calls: 0")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Repeated termination remained idempotent: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
