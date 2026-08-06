"""Validate Scenario 1's recursive counterparty frontier offline."""

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

from network_mule_discovery.behavioral_features import (
    build_behavioral_features,
)
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
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    run_recursive_counterparty_frontier,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
    generate_scenario_1_source_data,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


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
        "decision_version": "recursive-frontier-test-v1",
        "decided_at": "2026-07-20 12:00:00",
        "source": "TEST_DECISION_ADAPTER",
    }


class RecursiveCounterpartyAdapter:
    """Return one deterministic decision only in this smoke test."""

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
        assert subject_type == "COUNTERPARTY"
        assert subject_key == "LOCAL_ACCOUNT|990200000001"
        payload = json.loads(feature_payload_json)
        assert payload["subject_key"] == subject_key
        assert "behavioral_evidence" in payload
        self.calls.append(subject_key)
        self.last_call_metadata = {
            "assessment": {
                "confidence": "HIGH",
                "rationale": (
                    "Offline recursive counterparty frontier test."
                ),
                "key_evidence": [
                    "Production-shaped synthetic evidence present."
                ],
            },
            "model": "test-model",
            "prompt_version": "test-v1",
        }
        return decision_record(
            subject_type=subject_type,
            subject_key=subject_key,
            feature_snapshot_hash=feature_snapshot_hash,
            decision="SUSPICIOUS_EXPAND",
            reason_code="SYNTHETIC_RECURSIVE_SHARED_COUNTERPARTY",
        )


def forbidden_factory() -> object:
    raise AssertionError(
        "An unchanged or planning-only frontier created an adapter."
    )


def build_initial_state(root: Path):
    source_directory = root / "source"
    canonical_directory = root / "canonical"
    discovery_directory = root / "discovery"
    state_directory = root / "state"

    generate_scenario_1_source_data(source_directory)
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
    counterparty_features = build_behavioral_features(
        source_directory=source_directory,
        counterparty_keys=sorted(
            counterparty_result.seed_counterparties[
                "counterparty_key"
            ].unique()
        ),
        run_date=RUN_DATE,
    )
    state_store = CsvDailyStateStore(state_directory)
    state_store.save_network_state(
        unified_result,
        RUN_DATE,
    )
    first_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=RUN_DATE,
        supplemental_subject_payloads=(
            counterparty_features.counterparty_payloads
        ),
    )
    counterparty_hashes = dict(
        zip(
            first_plan.projection.subject_snapshots.loc[
                lambda frame: frame["subject_type"].eq(
                    "COUNTERPARTY"
                ),
                "subject_key",
            ],
            first_plan.projection.subject_snapshots.loc[
                lambda frame: frame["subject_type"].eq(
                    "COUNTERPARTY"
                ),
                "feature_snapshot_hash",
            ],
        )
    )
    state_store.append_decisions(
        pd.DataFrame(
            [
                decision_record(
                    subject_type="COUNTERPARTY",
                    subject_key="LOCAL_ACCOUNT|880100000001",
                    feature_snapshot_hash=counterparty_hashes[
                        "LOCAL_ACCOUNT|880100000001"
                    ],
                    decision="COMMON_PUBLIC_SUPPRESS",
                    reason_code="SYNTHETIC_COMMON_PUBLIC",
                ),
                decision_record(
                    subject_type="COUNTERPARTY",
                    subject_key="LOCAL_ACCOUNT|990100000001",
                    feature_snapshot_hash=counterparty_hashes[
                        "LOCAL_ACCOUNT|990100000001"
                    ],
                    decision="SUSPICIOUS_EXPAND",
                    reason_code="SYNTHETIC_SUSPICIOUS",
                ),
            ]
        )
    )
    customer_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=RUN_DATE,
        supplemental_subject_payloads=(
            counterparty_features.counterparty_payloads
        ),
    )
    customer_keys = sorted(
        customer_plan.actionable_queue.loc[
            customer_plan.actionable_queue["action_type"].eq(
                "RUN_CUSTOMER_AI"
            ),
            "subject_key",
        ]
    )
    customer_features = build_customer_behavioral_features(
        source_directory=source_directory,
        customer_keys=customer_keys,
        projection=customer_plan.projection,
        run_date=RUN_DATE,
    )
    combined_payloads = pd.concat(
        [
            counterparty_features.counterparty_payloads,
            customer_features.customer_payloads,
        ],
        ignore_index=True,
    )
    full_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=RUN_DATE,
        supplemental_subject_payloads=combined_payloads,
    )
    customer_hashes = dict(
        zip(
            full_plan.projection.subject_snapshots.loc[
                lambda frame: frame["subject_type"].eq(
                    "CUSTOMER"
                ),
                "subject_key",
            ],
            full_plan.projection.subject_snapshots.loc[
                lambda frame: frame["subject_type"].eq(
                    "CUSTOMER"
                ),
                "feature_snapshot_hash",
            ],
        )
    )
    customer_outcomes = {
        "RETAIL|R1002": "MULE_LIKE",
        "RETAIL|R1003": "EXPOSED_VULNERABLE",
        "RETAIL|R1004": "EXPOSED_VULNERABLE",
        "SME|B2001": "INSUFFICIENT_EVIDENCE",
        "SME|B2002": "EXPOSED_VULNERABLE",
    }
    state_store.append_decisions(
        pd.DataFrame(
            [
                decision_record(
                    subject_type="CUSTOMER",
                    subject_key=subject_key,
                    feature_snapshot_hash=customer_hashes[
                        subject_key
                    ],
                    decision=decision,
                    reason_code="SYNTHETIC_CUSTOMER_DECISION",
                )
                for subject_key, decision in sorted(
                    customer_outcomes.items()
                )
            ]
        )
    )

    return (
        source_directory,
        state_directory,
        unified_result,
        combined_payloads,
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
        state_store = CsvDailyStateStore(state_directory)
        ready_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
            supplemental_subject_payloads=existing_payloads,
        )
        discovery_queue = ready_plan.actionable_queue.loc[
            ready_plan.actionable_queue["action_type"].eq(
                "DISCOVER_CUSTOMER_RELATIONSHIPS"
            )
        ]
        assert sorted(
            discovery_queue["subject_key"]
            .astype("string")
            .tolist()
        ) == [
            "RETAIL|R1002",
            "SME|B2001",
        ]
        assert not initial_network.nodes["counterparty_key"].eq(
            "LOCAL_ACCOUNT|990200000001"
        ).any()

        planning_result = run_recursive_counterparty_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=existing_payloads,
            selected_source_entity_key="RETAIL|R1002",
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=1,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
        )
        assert planning_result.controlled_run.calls_executed == 0
        assert planning_result.discovery.new_counterparty_keys == (
            "LOCAL_ACCOUNT|990200000001",
        )
        assert len(planning_result.discovery.relationships) == 3
        assert set(
            planning_result.discovery.relationships[
                "target_entity_key"
            ]
        ) == {
            "RETAIL|R1005",
            "RETAIL|R1006",
            "RETAIL|R1007",
        }
        assert planning_result.discovery.skipped_existing_counterparty_keys == (
            "LOCAL_ACCOUNT|990100000001",
        )
        assert len(
            planning_result.discovery.unshared_counterparty_keys
        ) == 4
        assert len(planning_result.expanded_network.nodes) == (
            len(initial_network.nodes) + 4
        )
        assert len(planning_result.expanded_network.edges) == (
            len(initial_network.edges) + 4
        )
        assert planning_result.guardrail_telemetry.iloc[0][
            "max_observed_depth"
        ] == 4
        assert planning_result.guardrail_telemetry.iloc[0][
            "guardrail_status"
        ] == "TELEMETRY_ONLY"
        assert planning_result.guardrail_telemetry.iloc[0][
            "expansion_source_count"
        ] == 2
        assert not bool(
            planning_result.guardrail_telemetry.iloc[0][
                "breadth_cap_enforced_flag"
            ]
        )
        initial_cp_queue = (
            planning_result.controlled_run.initial_plan
            .actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "RUN_COUNTERPARTY_AI"
                )
            ]
        )
        assert list(initial_cp_queue["subject_key"]) == [
            "LOCAL_ACCOUNT|990200000001"
        ]
        planning_discovery = (
            planning_result.controlled_run
            .final_plan.actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                )
            ]
        )
        assert list(
            planning_discovery["subject_key"]
        ) == ["SME|B2001"]
        assert not planning_result.controlled_run.final_plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI").any()

        payload_text = "\n".join(
            planning_result.new_features.counterparty_payloads[
                "feature_payload_json"
            ]
        )
        for forbidden in (
            "SUSPICIOUS_EXPAND",
            "LEGITIMATE_SUPPRESS",
            "COMMON_PUBLIC_SUPPRESS",
            "INSUFFICIENT_EVIDENCE_SUPPRESS",
        ):
            assert forbidden not in payload_text

        adapter = RecursiveCounterpartyAdapter()
        live_result = run_recursive_counterparty_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=existing_payloads,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=1,
                run_call_limit=1,
            ),
            adapter_factory=lambda: adapter,
        )
        assert live_result.controlled_run.calls_executed == 1
        assert adapter.calls == [
            "LOCAL_ACCOUNT|990200000001"
        ]
        next_customers = live_result.controlled_run.final_plan.actionable_queue.loc[
            lambda frame: frame["action_type"].eq(
                "RUN_CUSTOMER_AI"
            ),
            "subject_key",
        ].sort_values().tolist()
        assert next_customers == [
            "RETAIL|R1005",
            "RETAIL|R1006",
            "RETAIL|R1007",
        ]
        assert live_result.guardrail_telemetry.iloc[0][
            "current_frontier_width"
        ] == 4
        remaining_discovery = (
            live_result.controlled_run
            .final_plan.actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "DISCOVER_CUSTOMER_RELATIONSHIPS"
                )
            ]
        )
        assert list(
            remaining_discovery["subject_key"]
        ) == ["SME|B2001"]
        ledger = state_store.load_expansion_ledger()
        assert len(ledger) == 1
        assert ledger.iloc[0]["expansion_status"] == "COMPLETED"

        all_counterparty_payloads = pd.concat(
            [
                existing_payloads,
                live_result.new_features.counterparty_payloads,
            ],
            ignore_index=True,
        )
        b2001_result = run_recursive_counterparty_frontier(
            source_directory=source_directory,
            state_directory=state_directory,
            run_date=RUN_DATE,
            supplemental_subject_payloads=(
                all_counterparty_payloads
            ),
            selected_source_entity_key="SME|B2001",
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=2,
                run_call_limit=1,
            ),
            adapter_factory=forbidden_factory,
        )
        assert b2001_result.controlled_run.calls_executed == 0
        assert (
            b2001_result.discovery.source_entity_key
            == "SME|B2001"
        )
        assert (
            b2001_result.discovery.new_counterparty_keys
            == tuple()
        )
        assert b2001_result.discovery.relationships.empty
        assert not (
            b2001_result.controlled_run
            .final_plan.actionable_queue[
                "action_type"
            ]
            .eq("DISCOVER_CUSTOMER_RELATIONSHIPS")
            .any()
        )
        ledger = state_store.load_expansion_ledger()
        assert len(ledger) == 2
        assert list(ledger["source_entity_key"]) == [
            "RETAIL|R1002",
            "SME|B2001",
        ]

    print(
        "Scenario 1 recursive counterparty frontier smoke test passed."
    )
    print("Approved recursive sources consumed: 2")
    print("New shared counterparties discovered: 1")
    print("New linked customers observed: 3")
    print("Already observed counterparties skipped: 1")
    print("Unshared counterparties skipped: 4")
    print("New graph nodes: 4")
    print("New graph edges: 4")
    print("Maximum observed depth: 4")
    print("Guardrail mode: telemetry only")
    print("Counterparty AI actions queued: 1")
    print("Next customer frontier queued: 3")
    print("Repeated discovery actions: 0")
    print("Repeated AI calls: 0")
    print("Scenario labels in runtime payloads: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
