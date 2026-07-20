"""Smoke test for selective Day-2 changed-evidence processing."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)
from smoke_test_daily_incremental_state import (
    DAY_ONE,
    DAY_TWO,
    run_day_one,
)


def stable_id(
    prefix: str,
    *values: object,
) -> str:
    """Create a deterministic test identifier."""
    canonical_value = "|".join(
        str(value)
        for value in values
    )

    digest = hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()[:16]

    return f"{prefix}{digest}"


def select_suppressed_counterparty(
    nodes: pd.DataFrame,
) -> pd.Series:
    """
    Select one counterparty with a reusable suppression decision.

    Using a suppressed counterparty prevents the test from opening a
    new downstream customer branch before the changed counterparty has
    been reassessed.
    """
    candidates = nodes.loc[
        nodes["node_type"].eq("COUNTERPARTY")
        & nodes["node_status"]
        .astype("string")
        .str.startswith(
            "COUNTERPARTY_SUPPRESSED"
        )
        & nodes["applied_decision_id"]
        .astype("string")
        .fillna("")
        .str.strip()
        .ne("")
    ].copy()

    if candidates.empty:
        raise AssertionError(
            "No suppressed counterparty with an applied "
            "decision was found in the Day-1 network."
        )

    return (
        candidates
        .sort_values(
            by=[
                "counterparty_key",
                "group_id",
            ],
            kind="stable",
        )
        .iloc[0]
    )


def select_source_customer(
    nodes: pd.DataFrame,
    group_id: str,
) -> pd.Series:
    """Select one existing customer in the counterparty's group."""
    candidates = nodes.loc[
        nodes["group_id"].eq(group_id)
        & nodes["node_type"].eq("CUSTOMER")
    ].copy()

    if candidates.empty:
        raise AssertionError(
            f"No customer was found in group {group_id}."
        )

    candidates["seed_rank"] = (
        ~candidates[
            "customer_assessment_status"
        ].eq("SEED_CONFIRMED")
    ).astype(int)

    return (
        candidates
        .sort_values(
            by=[
                "seed_rank",
                "entity_key",
            ],
            kind="stable",
        )
        .iloc[0]
    )


def add_day_two_counterparty_evidence(
    network: UnifiedGroupResult,
    counterparty_node: pd.Series,
    source_customer: pd.Series,
) -> UnifiedGroupResult:
    """
    Add one new transfer-evidence edge.

    CUSTOMER_COUNTERPARTY_EVIDENCE contributes to the counterparty
    feature hash, but it is not customer-entry evidence. Therefore the
    counterparty should be the only subject whose hash changes.
    """
    groups = network.groups.copy()
    nodes = network.nodes.copy()
    edges = network.edges.copy()

    group_id = str(
        counterparty_node["group_id"]
    )

    counterparty_key = str(
        counterparty_node["counterparty_key"]
    )

    evidence_key = (
        f"DAY2_NEW_TRANSFER|{counterparty_key}"
    )

    edge_id = stable_id(
        "E",
        group_id,
        "CUSTOMER_COUNTERPARTY_EVIDENCE",
        source_customer["node_key"],
        counterparty_node["node_key"],
        evidence_key,
    )

    if edges["edge_id"].eq(edge_id).any():
        raise AssertionError(
            "The Day-2 test edge already exists."
        )

    if edges.empty:
        raise AssertionError(
            "The Day-1 network has no edge schema "
            "available for the test."
        )

    new_edge = edges.iloc[0].to_dict()

    new_edge.update(
        {
            "run_id": source_customer["run_id"],
            "run_date": DAY_TWO,
            "group_id": group_id,
            "edge_id": edge_id,
            "source_node_id": (
                source_customer["node_id"]
            ),
            "source_node_key": (
                source_customer["node_key"]
            ),
            "target_node_id": (
                counterparty_node["node_id"]
            ),
            "target_node_key": (
                counterparty_node["node_key"]
            ),
            "edge_type": (
                "CUSTOMER_COUNTERPARTY_EVIDENCE"
            ),
            "relationship_status": (
                "COUNTERPARTY_CANDIDATE"
            ),
            "customer_discovery_allowed_flag": False,
            "recursive_expansion_allowed_flag": False,
            "evidence_key": evidence_key,
            "evidence_summary": (
                "New Day-2 transfer evidence"
            ),
            "source_event_count": 1,
            "candidate_event_count": 0,
            "first_seen_date": DAY_TWO,
            "last_seen_date": DAY_TWO,
        }
    )

    edges = pd.concat(
        [
            edges,
            pd.DataFrame([new_edge]),
        ],
        ignore_index=True,
    )

    group_mask = groups["group_id"].eq(
        group_id
    )

    groups.loc[
        group_mask,
        "total_edge_count",
    ] = (
        pd.to_numeric(
            groups.loc[
                group_mask,
                "total_edge_count",
            ],
            errors="raise",
        )
        + 1
    )

    return UnifiedGroupResult(
        groups=groups,
        nodes=nodes,
        edges=edges,
    )


def snapshot_hashes(
    snapshots: pd.DataFrame,
) -> pd.DataFrame:
    """Return the comparable subject-hash fields."""
    return (
        snapshots[
            [
                "subject_type",
                "subject_key",
                "feature_snapshot_hash",
            ]
        ]
        .sort_values(
            by=[
                "subject_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def main() -> None:
    """Validate selective processing after one evidence change."""
    day_one_result = run_day_one()

    assert (
        day_one_result.termination_reason
        == "FRONTIER_EMPTY"
    )

    with TemporaryDirectory() as directory:
        state_store = CsvDailyStateStore(
            Path(directory)
        )

        state_store.commit_recursive_result(
            result=day_one_result,
            run_date=DAY_ONE,
        )

        unchanged_plan = (
            build_incremental_daily_plan(
                state_store=state_store,
                run_date=DAY_TWO,
            )
        )

        assert unchanged_plan.actionable_queue.empty
        assert (
            unchanged_plan.queued_ai_action_count
            == 0
        )
        assert (
            unchanged_plan
            .queued_expansion_action_count
            == 0
        )

        persisted_snapshot = (
            state_store.load_snapshot()
        )

        counterparty_node = (
            select_suppressed_counterparty(
                persisted_snapshot.network.nodes
            )
        )

        source_customer = (
            select_source_customer(
                nodes=(
                    persisted_snapshot.network.nodes
                ),
                group_id=str(
                    counterparty_node["group_id"]
                ),
            )
        )

        changed_counterparty_key = str(
            counterparty_node[
                "counterparty_key"
            ]
        )

        previously_applied_decision_id = str(
            counterparty_node[
                "applied_decision_id"
            ]
        )

        changed_network = (
            add_day_two_counterparty_evidence(
                network=(
                    persisted_snapshot.network
                ),
                counterparty_node=(
                    counterparty_node
                ),
                source_customer=source_customer,
            )
        )

        state_store.save_network_state(
            network=changed_network,
            run_date=DAY_TWO,
        )

        changed_plan = (
            build_incremental_daily_plan(
                state_store=state_store,
                run_date=DAY_TWO,
            )
        )

        assert len(
            changed_plan.actionable_queue
        ) == 1

        queued_action = (
            changed_plan.actionable_queue.iloc[0]
        )

        assert (
            queued_action["action_type"]
            == "RUN_COUNTERPARTY_AI"
        )

        assert (
            queued_action["subject_type"]
            == "COUNTERPARTY"
        )

        assert (
            queued_action["subject_key"]
            == changed_counterparty_key
        )

        assert (
            changed_plan.queued_ai_action_count
            == 1
        )

        assert (
            changed_plan
            .queued_expansion_action_count
            == 0
        )

        baseline_hashes = snapshot_hashes(
            unchanged_plan
            .projection
            .subject_snapshots
        )

        changed_hashes = snapshot_hashes(
            changed_plan
            .projection
            .subject_snapshots
        )

        comparison = baseline_hashes.merge(
            changed_hashes,
            how="outer",
            on=[
                "subject_type",
                "subject_key",
            ],
            suffixes=(
                "_before",
                "_after",
            ),
            validate="one_to_one",
        )

        changed_subjects = comparison.loc[
            comparison[
                "feature_snapshot_hash_before"
            ].ne(
                comparison[
                    "feature_snapshot_hash_after"
                ]
            )
        ].reset_index(drop=True)

        assert len(changed_subjects) == 1

        assert (
            changed_subjects.iloc[0][
                "subject_type"
            ]
            == "COUNTERPARTY"
        )

        assert (
            changed_subjects.iloc[0][
                "subject_key"
            ]
            == changed_counterparty_key
        )

        ignored_decision_ids = set(
            changed_plan
            .projection
            .ignored_decisions[
                "decision_id"
            ]
        )

        assert (
            previously_applied_decision_id
            in ignored_decision_ids
        )

        repeated_plan = (
            build_incremental_daily_plan(
                state_store=state_store,
                run_date=DAY_TWO,
            )
        )

        assert_frame_equal(
            changed_plan.actionable_queue,
            repeated_plan.actionable_queue,
            check_dtype=True,
        )

        assert set(
            persisted_snapshot
            .network
            .nodes["node_id"]
        ) == set(
            changed_network.nodes["node_id"]
        )

        assert (
            len(changed_network.edges)
            == len(
                persisted_snapshot.network.edges
            )
            + 1
        )

    print(
        "Daily changed-evidence smoke test passed."
    )

    print(
        "Changed subjects: 1 counterparty"
    )

    print(
        "Queued counterparty AI actions: 1"
    )

    print(
        "Queued customer AI actions: 0"
    )

    print(
        "Repeated customer expansions: 0"
    )

    print(
        "Unrelated subject hashes changed: 0"
    )

    print(
        "Stable repeated Day-2 plan: passed"
    )


if __name__ == "__main__":
    main()
