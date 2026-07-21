"""Validate Scenario 1's counterparty frontier without live calls."""

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
    build_behavioral_features,
)
from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    discover_counterparty_candidates,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.decision_engine import (
    build_subject_snapshots,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.frontier_ai import (
    run_counterparty_ai_frontier,
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


def forbidden_factory() -> object:
    raise AssertionError(
        "Planning-only frontier created a live adapter."
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        canonical_directory = root / "canonical"
        discovery_directory = root / "discovery"
        state_directory = root / "state"

        generate_scenario_1_source_data(
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

        counterparty_keys = sorted(
            counterparty_result.seed_counterparties[
                "counterparty_key"
            ].drop_duplicates()
        )
        features = build_behavioral_features(
            source_directory=source_directory,
            counterparty_keys=counterparty_keys,
            run_date=RUN_DATE,
        )

        supplemental_payloads = (
            features.counterparty_payloads
        )

        graph_only_snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=unified_result.edges,
        )
        enriched_snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=unified_result.edges,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
        )

        enriched_counterparties = (
            enriched_snapshots.loc[
                enriched_snapshots[
                    "subject_type"
                ].eq("COUNTERPARTY")
            ]
        )
        graph_only_counterparties = (
            graph_only_snapshots.loc[
                graph_only_snapshots[
                    "subject_type"
                ].eq("COUNTERPARTY")
            ]
        )

        assert len(enriched_counterparties) == 2
        assert enriched_counterparties[
            "supplemental_evidence_included"
        ].all()

        merged_hashes = enriched_counterparties.merge(
            graph_only_counterparties[
                [
                    "subject_key",
                    "feature_snapshot_hash",
                ]
            ],
            on="subject_key",
            suffixes=("_enriched", "_graph_only"),
            validate="one_to_one",
        )

        assert merged_hashes[
            "feature_snapshot_hash_enriched"
        ].ne(
            merged_hashes[
                "feature_snapshot_hash_graph_only"
            ]
        ).all()

        for row in enriched_counterparties.itertuples(
            index=False
        ):
            payload = json.loads(
                row.feature_payload_json
            )
            assert (
                payload["subject_type"]
                == "COUNTERPARTY"
            )

            assert (
                payload["subject_key"]
                == row.subject_key
            )

            assert isinstance(
                payload["nodes"],
                list,
            )

            assert payload["nodes"]

            assert isinstance(
                payload["relationships"],
                list,
            )

            assert payload["relationships"]

            assert (
                "behavioral_evidence"
                in payload
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
                supplemental_payloads
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

        assert len(counterparty_queue) == 2
        assert len(customer_queue) == 1

        planning_result = (
            run_counterparty_ai_frontier(
                unified_result=unified_result,
                supplemental_subject_payloads=(
                    supplemental_payloads
                ),
                state_directory=state_directory,
                run_date=RUN_DATE,
                settings=DailyAiSettings(
                    live_ai_enabled=False,
                    daily_call_limit=2,
                    run_call_limit=2,
                ),
                adapter_factory=forbidden_factory,
            )
        )

        assert (
            planning_result
            .controlled_run
            .calls_executed
            == 0
        )
        assert planning_result.decision_store.empty
        assert planning_result.ai_call_ledger.empty

    print(
        "Scenario 1 counterparty frontier smoke test passed."
    )
    print("Enriched counterparty snapshots: 2")
    print("Graph evidence included: passed")
    print("Behavioral evidence included: passed")
    print("Evidence hashes changed by behavior: passed")
    print("Counterparty AI actions queued: 2")
    print("Deterministic customer AI actions deferred: 1")
    print("Counterparty-only phase barrier: passed")
    print("Planning-only AI calls: 0")
    print("Supplied AI decisions: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
