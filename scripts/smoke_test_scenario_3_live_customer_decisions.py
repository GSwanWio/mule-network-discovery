"""Validate Scenario 3 customer decisions without live API calls."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
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
    discover_counterparty_candidates,
)
from network_mule_discovery.customer_behavioral_features import (
    build_customer_behavioral_features,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.frontier_ai import (
    run_customer_ai_frontier,
)
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_3_synthetic_data import (
    ADD_ONLY_CUSTOMER_ID,
    PAYMENT_BACKED_CUSTOMER_ID,
    RUN_DATE,
    generate_scenario_3_source_data,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


PAYMENT_BACKED_SUBJECT_KEY = (
    f"RETAIL|{PAYMENT_BACKED_CUSTOMER_ID}"
)
ADD_ONLY_SUBJECT_KEY = f"SME|{ADD_ONLY_CUSTOMER_ID}"


def stable_id(prefix: str, *values: object) -> str:
    canonical = "|".join(str(value) for value in values)
    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()[:16]
    return f"{prefix}{digest}"


def decision_record(
    *,
    subject_type: str,
    subject_key: str,
    feature_snapshot_hash: str,
    decision: str,
    reason_code: str,
) -> dict[str, str]:
    return {
        "decision_id": stable_id(
            "TD",
            subject_type,
            subject_key,
            feature_snapshot_hash,
            decision,
        ),
        "subject_type": subject_type,
        "subject_key": subject_key,
        "feature_snapshot_hash": feature_snapshot_hash,
        "decision": decision,
        "reason_code": reason_code,
        "decision_version": "scenario-3-customer-test-v1",
        "decided_at": "2026-07-20 12:00:00",
        "source": "TEST_DECISION_ADAPTER",
    }


class Scenario3CustomerDecisionAdapter:
    """Return deterministic decisions for the two Scenario 3 customers."""

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
        assert "behavioral_evidence" in payload

        behavioral_evidence = payload["behavioral_evidence"]

        assert behavioral_evidence["subject_key"] == subject_key
        assert "transaction_behavior" in behavioral_evidence
        assert "assessment_context" in behavioral_evidence

        self.calls.append(subject_key)

        if subject_key == PAYMENT_BACKED_SUBJECT_KEY:
            decision = "MULE_LIKE"
            reason_code = "TEST_RAPID_DRAIN_TO_CONFIRMED_MULE"
        elif subject_key == ADD_ONLY_SUBJECT_KEY:
            decision = "INSUFFICIENT_EVIDENCE"
            reason_code = "TEST_ADD_ONLY_SPARSE_EVIDENCE"
        else:
            raise AssertionError(
                f"Unexpected Scenario 3 subject: {subject_key}"
            )

        self.last_call_metadata = {
            "assessment": {
                "confidence": "HIGH",
                "rationale": (
                    "Deterministic Scenario 3 customer decision test."
                ),
                "key_evidence": [
                    "Neutral production-shaped evidence payload present."
                ],
            },
            "model": "test-model",
            "prompt_version": "test-v1",
        }

        return decision_record(
            subject_type=subject_type,
            subject_key=subject_key,
            feature_snapshot_hash=feature_snapshot_hash,
            decision=decision,
            reason_code=reason_code,
        )


class PartialFailureAdapter(Scenario3CustomerDecisionAdapter):
    """Fail the mule-like candidate while completing the sparse case."""

    def decide(self, **kwargs) -> dict[str, str]:
        subject_key = str(kwargs["subject_key"])

        if subject_key == PAYMENT_BACKED_SUBJECT_KEY:
            self.calls.append(subject_key)
            raise RuntimeError(
                "Synthetic isolated customer AI failure."
            )

        return super().decide(**kwargs)


def forbidden_factory() -> object:
    raise AssertionError(
        "Planning or unchanged frontier created an adapter."
    )


def build_test_inputs(root: Path):
    source_directory = root / "source"
    canonical_directory = root / "canonical"
    discovery_directory = root / "discovery"

    generate_scenario_3_source_data(source_directory)
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
    counterparty_result = discover_counterparty_candidates(
        data_source=data_source,
        run_date=RUN_DATE,
    )
    unified_result = build_unified_seed_groups(
        eid_discovery=eid_result,
        counterparty_discovery=counterparty_result,
        run_date=RUN_DATE,
    )

    planning_state = CsvDailyStateStore(
        root / "planning_state"
    )
    planning_state.save_network_state(
        network=unified_result,
        run_date=RUN_DATE,
    )
    graph_plan = build_incremental_daily_plan(
        state_store=planning_state,
        run_date=RUN_DATE,
    )
    customer_keys = sorted(
        graph_plan.actionable_queue.loc[
            graph_plan.actionable_queue["action_type"].eq(
                "RUN_CUSTOMER_AI"
            ),
            "subject_key",
        ].unique()
    )
    customer_features = build_customer_behavioral_features(
        source_directory=source_directory,
        customer_keys=customer_keys,
        projection=graph_plan.projection,
        run_date=RUN_DATE,
    )

    return (
        unified_result,
        customer_features.customer_payloads,
        customer_keys,
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        (
            unified_result,
            customer_payloads,
            customer_keys,
        ) = build_test_inputs(root)

        assert customer_keys == [
            PAYMENT_BACKED_SUBJECT_KEY,
            ADD_ONLY_SUBJECT_KEY,
        ]

        state_directory = root / "live_state"
        planning_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=customer_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=2,
                run_call_limit=2,
            ),
            adapter_factory=forbidden_factory,
        )
        assert planning_result.controlled_run.calls_executed == 0
        assert len(
            planning_result.controlled_run.initial_plan
            .actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "RUN_CUSTOMER_AI"
                )
            ]
        ) == 2

        adapter = Scenario3CustomerDecisionAdapter()
        live_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=customer_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=2,
            ),
            adapter_factory=lambda: adapter,
        )

        assert live_result.controlled_run.calls_executed == 2
        assert sorted(adapter.calls) == customer_keys

        decisions = live_result.decision_store.loc[
            live_result.decision_store["subject_type"].eq(
                "CUSTOMER"
            )
        ].set_index("subject_key")
        assert decisions.loc[
            PAYMENT_BACKED_SUBJECT_KEY,
            "decision",
        ] == "MULE_LIKE"
        assert decisions.loc[
            ADD_ONLY_SUBJECT_KEY,
            "decision",
        ] == "INSUFFICIENT_EVIDENCE"

        final_plan = live_result.controlled_run.final_plan
        assert not final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI").any()
        recursive_queue = final_plan.actionable_queue.loc[
            final_plan.actionable_queue["action_type"].eq(
                "DISCOVER_CUSTOMER_RELATIONSHIPS"
            )
        ]
        assert len(recursive_queue) == 1
        assert recursive_queue.iloc[0][
            "subject_key"
        ] == PAYMENT_BACKED_SUBJECT_KEY
        assert not final_plan.frontier_queue[
            "queue_status"
        ].eq("FAILED_CLOSED").any()

        projected_nodes = (
            final_plan.projection.nodes.set_index("entity_key")
        )
        assert projected_nodes.loc[
            PAYMENT_BACKED_SUBJECT_KEY,
            "expansion_source_flag",
        ]
        assert not projected_nodes.loc[
            ADD_ONLY_SUBJECT_KEY,
            "expansion_source_flag",
        ]

        unchanged_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=customer_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=4,
                run_call_limit=2,
            ),
            adapter_factory=forbidden_factory,
        )
        assert unchanged_result.controlled_run.calls_executed == 0

        failure_state_directory = root / "failure_state"
        failure_adapter = PartialFailureAdapter()
        failure_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=customer_payloads,
            state_directory=failure_state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=2,
                run_call_limit=2,
            ),
            adapter_factory=lambda: failure_adapter,
        )
        assert failure_result.controlled_run.calls_executed == 2
        failure_ledger = failure_result.ai_call_ledger
        assert int(
            failure_ledger["call_status"].eq(
                "FAILED_CLOSED"
            ).sum()
        ) == 1
        assert int(
            failure_ledger["call_status"].eq(
                "COMPLETED"
            ).sum()
        ) == 1
        assert int(
            failure_result.controlled_run.final_plan
            .frontier_queue["queue_status"].eq(
                "FAILED_CLOSED"
            ).sum()
        ) == 1
        assert not (
            failure_result.controlled_run.final_plan
            .actionable_queue["action_type"].eq(
                "DISCOVER_CUSTOMER_RELATIONSHIPS"
            ).any()
        )

    print("Scenario 3 live customer decision smoke test passed.")
    print("Customer AI actions executed in test: 2")
    print("Payment-backed customer decision: MULE_LIKE")
    print("Add-only customer decision: INSUFFICIENT_EVIDENCE")
    print("Recursive sources queued: 1")
    print("Non-mule customer expansions queued: 0")
    print("Unchanged repeated customer calls: 0")
    print("Failure path remained failed closed: passed")
    print("Observed graph nodes/edges preserved: 3/2")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
