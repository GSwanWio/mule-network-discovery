"""Validate Scenario 1's customer frontier without live calls."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

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
    CUSTOMER_ASSESSMENT_POLICY_VERSION,
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
        "decision_version": "scenario-1-customer-test-v1",
        "decided_at": "2026-07-20 12:00:00",
        "source": "TEST_DECISION_ADAPTER",
    }


class CustomerDecisionAdapter:
    """Return deterministic customer decisions in an isolated test."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.last_call_metadata: dict[str, object] = {}
        self.decisions = {
            "RETAIL|R1002": (
                "MULE_LIKE",
                "SYNTHETIC_RAPID_DRAIN",
            ),
            "RETAIL|R1003": (
                "EXPOSED_VULNERABLE",
                "SYNTHETIC_STABLE_SALARY_EXPOSURE",
            ),
            "RETAIL|R1004": (
                "INSUFFICIENT_EVIDENCE",
                "SYNTHETIC_SPARSE_HISTORY",
            ),
            "SME|B2001": (
                "LOW_CONCERN",
                "SYNTHETIC_DETERMINISTIC_ID_LINK",
            ),
            "SME|B2002": (
                "LOW_CONCERN",
                "SYNTHETIC_ESTABLISHED_BUSINESS",
            ),
        }

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

        self.calls.append(subject_key)
        decision, reason_code = self.decisions[subject_key]
        self.last_call_metadata = {
            "assessment": {
                "confidence": "HIGH",
                "rationale": "Synthetic isolated customer frontier test.",
                "key_evidence": [
                    "Production-derived synthetic evidence payload present."
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


def forbidden_factory() -> object:
    raise AssertionError(
        "Planning or unchanged frontier created an adapter."
    )


def build_test_inputs(root: Path):
    source_directory = root / "source"
    canonical_directory = root / "canonical"
    discovery_directory = root / "discovery"

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

    return (
        source_directory,
        unified_result,
        counterparty_features.counterparty_payloads,
    )


def append_counterparty_decisions(
    *,
    state_store: CsvDailyStateStore,
    counterparty_payloads: pd.DataFrame,
) -> None:
    plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=RUN_DATE,
        supplemental_subject_payloads=counterparty_payloads,
    )
    snapshots = plan.projection.subject_snapshots.loc[
        plan.projection.subject_snapshots[
            "subject_type"
        ].eq("COUNTERPARTY")
    ]
    hash_map = dict(
        zip(
            snapshots["subject_key"],
            snapshots["feature_snapshot_hash"],
        )
    )
    decisions = pd.DataFrame([
        decision_record(
            subject_type="COUNTERPARTY",
            subject_key="LOCAL_ACCOUNT|880100000001",
            feature_snapshot_hash=hash_map[
                "LOCAL_ACCOUNT|880100000001"
            ],
            decision="COMMON_PUBLIC_SUPPRESS",
            reason_code="SYNTHETIC_COMMON_PUBLIC",
        ),
        decision_record(
            subject_type="COUNTERPARTY",
            subject_key="LOCAL_ACCOUNT|990100000001",
            feature_snapshot_hash=hash_map[
                "LOCAL_ACCOUNT|990100000001"
            ],
            decision="SUSPICIOUS_EXPAND",
            reason_code="SYNTHETIC_SUSPICIOUS",
        ),
    ])
    state_store.append_decisions(decisions)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        state_directory = root / "state"
        (
            source_directory,
            unified_result,
            counterparty_payloads,
        ) = build_test_inputs(root)

        state_store = CsvDailyStateStore(state_directory)
        state_store.save_network_state(
            network=unified_result,
            run_date=RUN_DATE,
        )

        try:
            run_customer_ai_frontier(
                unified_result=unified_result,
                supplemental_subject_payloads=(
                    counterparty_payloads
                ),
                state_directory=state_directory,
                run_date=RUN_DATE,
                settings=DailyAiSettings(
                    live_ai_enabled=False,
                    daily_call_limit=5,
                    run_call_limit=5,
                ),
                adapter_factory=forbidden_factory,
            )
        except RuntimeError as exc:
            assert (
                "counterparty decisions remain unresolved"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Customer phase started before the "
                "counterparty frontier closed."
            )

        append_counterparty_decisions(
            state_store=state_store,
            counterparty_payloads=counterparty_payloads,
        )

        counterparty_plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=RUN_DATE,
            supplemental_subject_payloads=counterparty_payloads,
        )
        customer_queue = counterparty_plan.actionable_queue.loc[
            counterparty_plan.actionable_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI")
        ]
        customer_keys = sorted(
            customer_queue["subject_key"].unique()
        )

        assert customer_keys == [
            "RETAIL|R1002",
            "RETAIL|R1003",
            "RETAIL|R1004",
            "SME|B2001",
            "SME|B2002",
        ]

        customer_features = build_customer_behavioral_features(
            source_directory=source_directory,
            customer_keys=customer_keys,
            projection=counterparty_plan.projection,
            run_date=RUN_DATE,
        )
        profiles = customer_features.customer_profiles.set_index(
            "subject_key"
        )

        assert profiles.loc[
            "RETAIL|R1002",
            "rapid_outward_share_2h_30d",
        ] > 0.6
        assert profiles.loc[
            "RETAIL|R1002",
            "distinct_inward_source_count_30d",
        ] == 11
        assert profiles.loc[
            "RETAIL|R1003",
            "salary_month_count_365d",
        ] >= 10
        assert profiles.loc[
            "RETAIL|R1003",
            "salary_regular_amount_share_365d",
        ] == 1.0
        assert profiles.loc[
            "RETAIL|R1003",
            "suppressed_counterparty_count",
        ] >= 1
        assert profiles.loc[
            "SME|B2002",
            "account_tenure_days",
        ] > 900
        assert profiles.loc[
            "SME|B2002",
            "outward_event_count_30d",
        ] == 0
        assert profiles.loc[
            "RETAIL|R1004",
            "all_time_outward_event_count",
        ] == 1
        assert profiles.loc[
            "SME|B2001",
            "deterministic_relationship_count",
        ] == 1

        payload_text = "\n".join(
            customer_features.customer_payloads[
                "feature_payload_json"
            ].tolist()
        )

        for payload_json in (
            customer_features.customer_payloads[
                "feature_payload_json"
            ]
        ):
            payload = json.loads(payload_json)
            assert (
                payload["assessment_policy_version"]
                == CUSTOMER_ASSESSMENT_POLICY_VERSION
            )

        for forbidden in (
            "MULE_LIKE",
            "EXPOSED_VULNERABLE",
            "LOW_CONCERN",
            "INSUFFICIENT_EVIDENCE",
        ):
            assert forbidden not in payload_text

        combined_payloads = pd.concat(
            [
                counterparty_payloads,
                customer_features.customer_payloads,
            ],
            ignore_index=True,
        )

        planning_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=combined_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=5,
                run_call_limit=5,
            ),
            adapter_factory=forbidden_factory,
        )
        assert planning_result.controlled_run.calls_executed == 0
        assert len(
            planning_result.controlled_run.initial_plan.actionable_queue.loc[
                lambda frame: frame["action_type"].eq(
                    "RUN_CUSTOMER_AI"
                )
            ]
        ) == 5

        adapter = CustomerDecisionAdapter()
        live_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=combined_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=5,
                run_call_limit=5,
            ),
            adapter_factory=lambda: adapter,
        )

        assert live_result.controlled_run.calls_executed == 5
        assert sorted(adapter.calls) == customer_keys
        customer_decisions = live_result.decision_store.loc[
            live_result.decision_store[
                "subject_type"
            ].eq("CUSTOMER")
        ]
        assert len(customer_decisions) == 5

        final_queue = live_result.controlled_run.final_plan.actionable_queue
        assert not final_queue["action_type"].eq(
            "RUN_CUSTOMER_AI"
        ).any()
        recursive_queue = final_queue.loc[
            final_queue["action_type"].eq(
                "DISCOVER_CUSTOMER_RELATIONSHIPS"
            )
        ]
        assert len(recursive_queue) == 1
        assert recursive_queue.iloc[0][
            "subject_key"
        ] == "RETAIL|R1002"

        assert not unified_result.nodes[
            "counterparty_key"
        ].eq("LOCAL_ACCOUNT|990200000001").any()

        unchanged_result = run_customer_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=combined_payloads,
            state_directory=state_directory,
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=10,
                run_call_limit=5,
            ),
            adapter_factory=forbidden_factory,
        )
        assert unchanged_result.controlled_run.calls_executed == 0

    print(
        "Scenario 1 customer frontier smoke test passed."
    )
    print("Customer behavioral profiles: 5")
    print("Customer supplemental payloads: 5")
    print("Rapid-drain evidence: passed")
    print("Stable-salary evidence: passed")
    print("Established-business evidence: passed")
    print("Sparse-history evidence: passed")
    print("Deterministic EID context: passed")
    print("Counterparty phase barrier: passed")
    print("Customer AI actions executed in test: 5")
    print("Recursive sources queued: 1")
    print("Second-layer counterparty exposed early: 0")
    print("Unchanged repeated customer calls: 0")
    print("Scenario labels in runtime payloads: 0")
    print(
        "Customer assessment policy version: "
        f"{CUSTOMER_ASSESSMENT_POLICY_VERSION}"
    )
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
