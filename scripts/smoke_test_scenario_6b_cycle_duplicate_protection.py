"""Validate Scenario 6B cycle and duplicate protection."""

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
    DailyStateSnapshot,
)
from network_mule_discovery.frontier_termination import (
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.operational_resilience import (
    OperationalStateIntegrityError,
    validate_persisted_operational_state,
)
from network_mule_discovery.recursive_expansion import (
    merge_expansion_relationships,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


RUN_DATE = date(2026, 7, 22)
GROUP_ID = "G-SCENARIO-6B"
ENTITY_A = "RETAIL|R6101"
ENTITY_B = "SME|B6101"
COUNTERPARTY_1 = "LOCAL_ACCOUNT|761000000001"
COUNTERPARTY_2 = "LOCAL_ACCOUNT|761000000002"


class CycleSuppressingAdapter:
    """Return deterministic suppression for both cycle counterparties."""

    def __init__(self) -> None:
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
        assert subject_key in {
            COUNTERPARTY_1,
            COUNTERPARTY_2,
        }
        assert feature_payload_json
        assert run_date == RUN_DATE
        assert round_number == 1
        assert sequence_number in {1, 2}

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
            "decision_id": f"S6B{digest}",
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": feature_snapshot_hash,
            "decision": "LEGITIMATE_SUPPRESS",
            "reason_code": "SCENARIO_6B_CYCLE_SUPPRESSION",
            "decision_version": "scenario-6b-test-v1",
            "decided_at": "2026-07-22 12:00:00",
            "source": "SCENARIO_6B_OFFLINE_ADAPTER",
        }


def forbidden_factory() -> object:
    raise AssertionError(
        "An unchanged cycle must not instantiate the adapter."
    )


def build_initial_graph() -> UnifiedGroupResult:
    """Build one seed-only group before cyclic relationships arrive."""
    run_id = "scenario_6b_20260722"
    run_date = str(RUN_DATE)
    node_key = f"CUSTOMER|{ENTITY_A}"

    groups = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_date": run_date,
                "group_id": GROUP_ID,
                "group_anchor_seed_entity_key": ENTITY_A,
                "group_status": "ACTIVE",
                "seed_entity_count": 1,
                "customer_count": 1,
                "counterparty_count": 0,
                "eid_link_count": 0,
                "counterparty_candidate_count": 0,
                "shared_counterparty_customer_count": 0,
                "beneficiary_seed_link_count": 0,
                "customer_assessment_pending_count": 0,
                "counterparty_ai_pending_count": 0,
                "recursive_expansion_source_count": 1,
                "total_node_count": 1,
                "total_edge_count": 0,
                "first_seen_date": run_date,
                "last_seen_date": run_date,
            }
        ]
    )

    nodes = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_date": run_date,
                "group_id": GROUP_ID,
                "node_id": "N-S6B-SEED-A",
                "node_key": node_key,
                "node_type": "CUSTOMER",
                "entity_type": "RETAIL",
                "entity_id": "R6101",
                "entity_key": ENTITY_A,
                "counterparty_key": "",
                "display_label": ENTITY_A,
                "node_roles": "SEED_MULE",
                "node_status": "SEED_EXPANSION_SOURCE",
                "customer_assessment_status": "SEED_CONFIRMED",
                "customer_discovery_allowed_flag": True,
                "expansion_source_flag": True,
                "first_seen_date": run_date,
                "last_seen_date": run_date,
            }
        ]
    )

    edges = pd.DataFrame(
        columns=[
            "run_id",
            "run_date",
            "group_id",
            "edge_id",
            "source_node_id",
            "target_node_id",
            "source_node_key",
            "target_node_key",
            "edge_type",
            "relationship_status",
            "customer_discovery_allowed_flag",
            "recursive_expansion_allowed_flag",
            "evidence_key",
            "evidence_summary",
            "source_event_count",
            "candidate_event_count",
            "first_seen_date",
            "last_seen_date",
        ]
    )

    return UnifiedGroupResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
    )


def relationship_rows(
    *,
    source_entity_key: str,
    target_entity_type: str,
    target_entity_id: str,
    target_entity_key: str,
    counterparty_key: str,
    evidence_prefix: str,
) -> pd.DataFrame:
    """Return duplicate and multi-provenance rows for one logical path."""
    base = {
        "snapshot_date": str(RUN_DATE),
        "source_entity_key": source_entity_key,
        "relationship_type": "SHARED_EXTERNAL_COUNTERPARTY",
        "counterparty_key": counterparty_key,
        "counterparty_name": f"Scenario 6B {counterparty_key}",
        "target_entity_type": target_entity_type,
        "target_entity_id": target_entity_id,
        "target_entity_key": target_entity_key,
        "evidence_summary": (
            f"{source_entity_key} and {target_entity_key} share "
            f"{counterparty_key}."
        ),
        "source_event_count": 2,
        "candidate_event_count": 3,
        "total_candidate_event_count": 3,
    }

    first = {
        **base,
        "evidence_key": f"{evidence_prefix}-EXTRACT-1",
    }
    second = {
        **base,
        "evidence_key": f"{evidence_prefix}-EXTRACT-2",
        "evidence_summary": (
            f"Independent duplicate-safe provenance for "
            f"{counterparty_key}."
        ),
    }

    return pd.DataFrame(
        [
            first,
            first.copy(),
            second,
        ]
    )


def logical_edge_keys(
    graph: UnifiedGroupResult,
) -> tuple[tuple[str, str, str, str], ...]:
    """Return sorted logical edge identities."""
    rows = graph.edges[
        [
            "group_id",
            "edge_type",
            "source_node_key",
            "target_node_key",
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
    """Prove cyclic and duplicate evidence remains bounded."""
    first_relationships = relationship_rows(
        source_entity_key=ENTITY_A,
        target_entity_type="SME",
        target_entity_id="B6101",
        target_entity_key=ENTITY_B,
        counterparty_key=COUNTERPARTY_1,
        evidence_prefix="S6B-A-CP1",
    )
    second_relationships = relationship_rows(
        source_entity_key=ENTITY_B,
        target_entity_type="RETAIL",
        target_entity_id="R6101",
        target_entity_key=ENTITY_A,
        counterparty_key=COUNTERPARTY_2,
        evidence_prefix="S6B-B-CP2",
    )

    graph = build_initial_graph()
    first_graph = merge_expansion_relationships(
        graph=graph,
        relationships=first_relationships,
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    assert len(first_graph.nodes) == 3
    assert len(first_graph.edges) == 2

    first_node_ids = tuple(
        sorted(first_graph.nodes["node_id"].astype("string"))
    )
    first_edge_ids = tuple(
        sorted(first_graph.edges["edge_id"].astype("string"))
    )

    first_repeated = merge_expansion_relationships(
        graph=first_graph,
        relationships=first_relationships,
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    assert len(first_repeated.nodes) == 3
    assert len(first_repeated.edges) == 2
    assert tuple(
        sorted(first_repeated.nodes["node_id"].astype("string"))
    ) == first_node_ids
    assert tuple(
        sorted(first_repeated.edges["edge_id"].astype("string"))
    ) == first_edge_ids

    cycle_graph = merge_expansion_relationships(
        graph=first_repeated,
        relationships=second_relationships,
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    assert len(cycle_graph.nodes) == 4
    assert len(cycle_graph.edges) == 4

    expected_cycle = {
        (
            f"CUSTOMER|{ENTITY_A}",
            f"COUNTERPARTY|{COUNTERPARTY_1}",
        ),
        (
            f"COUNTERPARTY|{COUNTERPARTY_1}",
            f"CUSTOMER|{ENTITY_B}",
        ),
        (
            f"CUSTOMER|{ENTITY_B}",
            f"COUNTERPARTY|{COUNTERPARTY_2}",
        ),
        (
            f"COUNTERPARTY|{COUNTERPARTY_2}",
            f"CUSTOMER|{ENTITY_A}",
        ),
    }
    observed_cycle = set(
        cycle_graph.edges[
            ["source_node_key", "target_node_key"]
        ].itertuples(index=False, name=None)
    )
    assert observed_cycle == expected_cycle

    final_node_ids = tuple(
        sorted(cycle_graph.nodes["node_id"].astype("string"))
    )
    final_edge_ids = tuple(
        sorted(cycle_graph.edges["edge_id"].astype("string"))
    )
    final_logical_edges = logical_edge_keys(cycle_graph)

    all_relationships = pd.concat(
        [first_relationships, second_relationships],
        ignore_index=True,
    )
    unchanged_graph = merge_expansion_relationships(
        graph=cycle_graph,
        relationships=all_relationships,
        group_ids=[GROUP_ID],
        run_date=RUN_DATE,
    )
    assert len(unchanged_graph.nodes) == 4
    assert len(unchanged_graph.edges) == 4
    assert tuple(
        sorted(unchanged_graph.nodes["node_id"].astype("string"))
    ) == final_node_ids
    assert tuple(
        sorted(unchanged_graph.edges["edge_id"].astype("string"))
    ) == final_edge_ids
    assert logical_edge_keys(unchanged_graph) == final_logical_edges

    for edge in unchanged_graph.edges.itertuples(index=False):
        evidence_tokens = {
            token
            for token in str(edge.evidence_key).split("||")
            if token
        }
        assert len(evidence_tokens) == 2
        assert int(edge.source_event_count) == 2
        assert int(edge.candidate_event_count) == 3

    with TemporaryDirectory() as directory:
        state_directory = Path(directory)
        state_store = CsvDailyStateStore(state_directory)
        state_store.save_network_state(
            network=unchanged_graph,
            run_date=RUN_DATE,
        )

        expansion_rows = pd.DataFrame(
            [
                {
                    "run_date": str(RUN_DATE),
                    "round_number": "1",
                    "queue_item_id": "Q-S6B-A",
                    "source_entity_key": ENTITY_A,
                    "group_ids": GROUP_ID,
                    "relationship_rows_found": "3",
                    "expansion_status": "COMPLETED",
                },
                {
                    "run_date": str(RUN_DATE),
                    "round_number": "2",
                    "queue_item_id": "Q-S6B-B",
                    "source_entity_key": ENTITY_B,
                    "group_ids": GROUP_ID,
                    "relationship_rows_found": "3",
                    "expansion_status": "COMPLETED",
                },
            ]
        )
        state_store.append_expansion_ledger(expansion_rows)
        state_store.append_expansion_ledger(expansion_rows)
        assert len(state_store.load_expansion_ledger()) == 2

        adapter = CycleSuppressingAdapter()
        ai_run = run_controlled_daily_ai(
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=2,
            ),
            adapter_factory=lambda: adapter,
            allowed_action_types={"RUN_COUNTERPARTY_AI"},
        )
        assert ai_run.calls_executed == 2
        assert set(adapter.calls) == {
            COUNTERPARTY_1,
            COUNTERPARTY_2,
        }
        assert ai_run.final_plan.actionable_queue.empty
        assert ai_run.final_plan.queued_ai_action_count == 0
        assert ai_run.final_plan.queued_expansion_action_count == 0

        termination = run_frontier_exhaustion_termination(
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=None,
            group_ids=[GROUP_ID],
            source_entity_key=ENTITY_A,
        )
        termination_row = termination.termination_status.iloc[0]
        assert termination_row["termination_status"] == "TERMINATED"
        assert termination_row["termination_reason"] == "FRONTIER_EXHAUSTED"

        repeated = run_controlled_daily_ai(
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
        assert repeated.calls_before_run == 2
        assert repeated.calls_executed == 0
        assert repeated.final_plan.actionable_queue.empty

        snapshot = state_store.load_snapshot()
        ledger = CsvAiCallLedger(state_directory).load()
        integrity = validate_persisted_operational_state(
            snapshot=snapshot,
            ai_call_ledger=ledger,
        )
        assert integrity.node_count == 4
        assert integrity.edge_count == 4
        assert integrity.decision_count == 2
        assert integrity.expansion_ledger_count == 2
        assert integrity.frontier_queue_count == 0
        assert integrity.ai_call_count == 2
        assert integrity.completed_ai_outcome_count == 2

        duplicate_edge = snapshot.network.edges.iloc[0].copy()
        duplicate_edge["edge_id"] = "E-S6B-TAMPERED-DUPLICATE"
        tampered_edges = pd.concat(
            [
                snapshot.network.edges,
                pd.DataFrame([duplicate_edge]),
            ],
            ignore_index=True,
        )
        tampered_snapshot = DailyStateSnapshot(
            network=UnifiedGroupResult(
                groups=snapshot.network.groups,
                nodes=snapshot.network.nodes,
                edges=tampered_edges,
            ),
            decision_store=snapshot.decision_store,
            expansion_ledger=snapshot.expansion_ledger,
            frontier_queue=snapshot.frontier_queue,
        )

        try:
            validate_persisted_operational_state(
                snapshot=tampered_snapshot,
                ai_call_ledger=ledger,
            )
        except OperationalStateIntegrityError as exc:
            assert "logical network edges" in str(exc)
        else:
            raise AssertionError(
                "A duplicate logical edge was not rejected."
            )

    print("Scenario 6B cycle and duplicate protection smoke test passed.")
    print("Duplicate relationship rows supplied: 6")
    print("Logical cycle nodes/edges: 4/4")
    print("Duplicate logical nodes/edges persisted: 0/0")
    print("Distinct provenance tokens retained per edge: 2")
    print("Duplicate event-count inflation: 0")
    print("Repeated cycle merge new nodes/edges: 0/0")
    print("Counterparty AI actions completed: 2")
    print("Repeated completed AI actions: 0")
    print("Expansion ledger rows: 2 unique")
    print("Termination reason: FRONTIER_EXHAUSTED")
    print("Logical duplicate integrity rejection: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
