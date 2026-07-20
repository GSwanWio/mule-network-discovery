"""Smoke test for processing one changed Day-2 branch."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
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
from network_mule_discovery.incremental_processor import (
    execute_incremental_ai_actions,
)
from smoke_test_daily_changed_evidence import (
    add_day_two_counterparty_evidence,
    select_source_customer,
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


class ApproveChangedCounterpartyAdapter:
    """Approve only the selected changed counterparty."""

    def __init__(
        self,
        expected_counterparty_key: str,
    ) -> None:
        self.expected_counterparty_key = (
            expected_counterparty_key
        )

        self.calls: list[
            tuple[str, str, str]
        ] = []

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
        """Return a suspicious approval for the changed subject."""
        assert subject_type == "COUNTERPARTY"

        assert (
            subject_key
            == self.expected_counterparty_key
        )

        feature_payload = json.loads(
            feature_payload_json
        )

        assert (
            feature_payload["subject_type"]
            == subject_type
        )

        assert (
            feature_payload["subject_key"]
            == subject_key
        )

        assert feature_payload[
            "relationships"
        ]

        self.calls.append(
            (
                subject_type,
                subject_key,
                feature_snapshot_hash,
            )
        )

        decided_at = (
            pd.Timestamp(run_date)
            + pd.Timedelta(hours=21)
            + pd.Timedelta(
                seconds=sequence_number
            )
        )

        return {
            "decision_id": stable_id(
                "TD",
                subject_type,
                subject_key,
                feature_snapshot_hash,
                "SUSPICIOUS_EXPAND",
                "changed-branch-test-v1",
            ),
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "decision": "SUSPICIOUS_EXPAND",
            "reason_code": (
                "NEW_DAY2_EVIDENCE_REQUIRES_EXPANSION"
            ),
            "decision_version": (
                "changed-branch-test-v1"
            ),
            "decided_at": str(decided_at),
            "source": "TEST_DECISION_ADAPTER",
        }


def select_suppressed_branch(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
) -> tuple[pd.Series, set[str]]:
    """
    Select a suppressed counterparty with blocked linked customers.

    This guarantees that approving the changed counterparty exposes
    real customer-assessment work.
    """
    counterparty_candidates = (
        nodes.loc[
            nodes["node_type"].eq(
                "COUNTERPARTY"
            )
            & nodes["node_status"]
            .astype("string")
            .str.startswith(
                "COUNTERPARTY_SUPPRESSED"
            )
        ]
        .sort_values(
            by=[
                "counterparty_key",
                "group_id",
            ],
            kind="stable",
        )
    )

    for counterparty in (
        counterparty_candidates.itertuples()
    ):
        counterparty_node_key = (
            counterparty.node_key
        )

        branch_edges = edges.loc[
            edges["group_id"].eq(
                counterparty.group_id
            )
            & edges["edge_type"].eq(
                "SHARED_EXTERNAL_COUNTERPARTY"
            )
            & (
                edges["source_node_key"].eq(
                    counterparty_node_key
                )
                | edges[
                    "target_node_key"
                ].eq(counterparty_node_key)
            )
        ]

        linked_node_keys: set[str] = set()

        for edge in branch_edges.itertuples(
            index=False
        ):
            if (
                edge.source_node_key
                == counterparty_node_key
            ):
                linked_node_keys.add(
                    edge.target_node_key
                )
            else:
                linked_node_keys.add(
                    edge.source_node_key
                )

        linked_customers = nodes.loc[
            nodes["group_id"].eq(
                counterparty.group_id
            )
            & nodes["node_type"].eq(
                "CUSTOMER"
            )
            & nodes["node_key"].isin(
                linked_node_keys
            )
            & nodes[
                "customer_assessment_status"
            ].eq(
                "BLOCKED_PENDING_COUNTERPARTY_AI"
            )
        ].copy()

        if linked_customers.empty:
            continue

        linked_customer_keys = set(
            linked_customers[
                "entity_key"
            ].astype("string")
        )

        return (
            nodes.loc[
                counterparty.Index
            ],
            linked_customer_keys,
        )

    raise AssertionError(
        "No suppressed counterparty branch with "
        "blocked linked customers was found."
    )


def normalized_node_state(
    nodes: pd.DataFrame,
    excluded_node_keys: set[str],
) -> pd.DataFrame:
    """Return comparable workflow fields for unrelated nodes."""
    columns = [
        "group_id",
        "node_key",
        "node_status",
        "customer_assessment_status",
        "customer_discovery_allowed_flag",
        "expansion_source_flag",
        "applied_decision_id",
        "applied_decision",
        "decision_reason_code",
    ]

    return (
        nodes.loc[
            ~nodes["node_key"].isin(
                excluded_node_keys
            ),
            columns,
        ]
        .astype("string")
        .fillna("")
        .sort_values(
            by=[
                "group_id",
                "node_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def subject_hashes(
    snapshots: pd.DataFrame,
) -> pd.DataFrame:
    """Return comparable evidence hashes."""
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
    """Approve one changed counterparty and expose only its branch."""
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

        persisted_state = (
            state_store.load_snapshot()
        )

        (
            counterparty_node,
            affected_customer_keys,
        ) = select_suppressed_branch(
            nodes=(
                persisted_state.network.nodes
            ),
            edges=(
                persisted_state.network.edges
            ),
        )

        changed_counterparty_key = str(
            counterparty_node[
                "counterparty_key"
            ]
        )

        group_id = str(
            counterparty_node["group_id"]
        )

        source_customer = (
            select_source_customer(
                nodes=(
                    persisted_state.network.nodes
                ),
                group_id=group_id,
            )
        )

        changed_network = (
            add_day_two_counterparty_evidence(
                network=(
                    persisted_state.network
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

        assert (
            changed_plan
            .actionable_queue
            .iloc[0]["subject_key"]
            == changed_counterparty_key
        )

        before_nodes = (
            changed_plan.projection.nodes.copy()
        )

        before_hashes = subject_hashes(
            changed_plan
            .projection
            .subject_snapshots
        )

        adapter = (
            ApproveChangedCounterpartyAdapter(
                expected_counterparty_key=(
                    changed_counterparty_key
                )
            )
        )

        execution_result = (
            execute_incremental_ai_actions(
                state_store=state_store,
                decision_adapter=adapter,
                run_date=DAY_TWO,
                max_ai_calls=1,
            )
        )

        assert len(adapter.calls) == 1

        assert len(
            execution_result.executed_actions
        ) == 1

        assert len(
            execution_result.generated_decisions
        ) == 1

        generated_decision = (
            execution_result
            .generated_decisions
            .iloc[0]
        )

        assert (
            generated_decision["decision"]
            == "SUSPICIOUS_EXPAND"
        )

        refreshed_queue = (
            execution_result
            .refreshed_plan
            .actionable_queue
        )

        repeated_counterparty_ai = (
            refreshed_queue.loc[
                refreshed_queue[
                    "action_type"
                ].eq("RUN_COUNTERPARTY_AI")
                & refreshed_queue[
                    "subject_key"
                ].eq(
                    changed_counterparty_key
                )
            ]
        )

        assert repeated_counterparty_ai.empty

        customer_ai_queue = (
            refreshed_queue.loc[
                refreshed_queue[
                    "action_type"
                ].eq("RUN_CUSTOMER_AI")
            ]
        )

        queued_customer_keys = set(
            customer_ai_queue[
                "subject_key"
            ].astype("string")
        )

        assert queued_customer_keys

        assert queued_customer_keys.issubset(
            affected_customer_keys
        )

        assert (
            execution_result
            .refreshed_plan
            .queued_expansion_action_count
            == 0
        )

        after_nodes = (
            execution_result
            .refreshed_plan
            .projection
            .nodes
        )

        changed_counterparty = (
            after_nodes.loc[
                after_nodes[
                    "counterparty_key"
                ].eq(
                    changed_counterparty_key
                )
            ]
        )

        assert not changed_counterparty.empty

        assert changed_counterparty[
            "node_status"
        ].eq(
            "COUNTERPARTY_APPROVED_SUSPICIOUS"
        ).all()

        queued_customer_nodes = (
            after_nodes.loc[
                after_nodes["entity_key"].isin(
                    queued_customer_keys
                )
            ]
        )

        assert not queued_customer_nodes.empty

        assert queued_customer_nodes[
            "customer_assessment_status"
        ].eq(
            "PENDING_CUSTOMER_AI"
        ).all()

        affected_node_keys = {
            str(counterparty_node["node_key"])
        }

        affected_node_keys.update(
            before_nodes.loc[
                before_nodes[
                    "entity_key"
                ].isin(affected_customer_keys),
                "node_key",
            ].astype("string")
        )

        before_unrelated = normalized_node_state(
            nodes=before_nodes,
            excluded_node_keys=(
                affected_node_keys
            ),
        )

        after_unrelated = normalized_node_state(
            nodes=after_nodes,
            excluded_node_keys=(
                affected_node_keys
            ),
        )

        assert_frame_equal(
            before_unrelated,
            after_unrelated,
            check_dtype=True,
        )

        after_hashes = subject_hashes(
            execution_result
            .refreshed_plan
            .projection
            .subject_snapshots
        )

        assert_frame_equal(
            before_hashes,
            after_hashes,
            check_dtype=True,
        )

        decision_store = (
            state_store.load_decision_store()
        )

        new_matching_decisions = (
            decision_store.loc[
                decision_store[
                    "subject_type"
                ].eq("COUNTERPARTY")
                & decision_store[
                    "subject_key"
                ].eq(
                    changed_counterparty_key
                )
                & decision_store[
                    "feature_snapshot_hash"
                ].eq(
                    generated_decision[
                        "feature_snapshot_hash"
                    ]
                )
            ]
        )

        assert len(
            new_matching_decisions
        ) == 1

    print(
        "Daily changed-branch smoke test passed."
    )

    print(
        "Executed counterparty AI actions: 1"
    )

    print(
        "Repeated counterparty AI actions: 0"
    )

    print(
        "Affected customer branches exposed: "
        f"{len(queued_customer_keys)}"
    )

    print(
        "Queued customer AI is limited "
        "to the affected branch: passed"
    )

    print(
        "Queued relationship expansions: 0"
    )

    print(
        "Unrelated node state changes: 0"
    )

    print(
        "Evidence hashes changed after decision: 0"
    )

    print(
        "Evidence payload delivered to adapter: passed"
    )


if __name__ == "__main__":
    main()
