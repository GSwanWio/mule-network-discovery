"""Persistent state contracts for incremental daily discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from network_mule_discovery.decision_engine import (
    DECISION_REQUIRED_COLUMNS,
    DecisionProjectionResult,
    apply_persisted_decisions,
    prepare_decisions,
)
from network_mule_discovery.recursive_expansion import (
    RecursiveExpansionResult,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


NETWORK_GROUPS_FILENAME = "network_state_groups.csv"
NETWORK_NODES_FILENAME = "network_state_nodes.csv"
NETWORK_EDGES_FILENAME = "network_state_edges.csv"

DECISION_STORE_FILENAME = "decision_store.csv"
EXPANSION_LEDGER_FILENAME = "expansion_ledger.csv"
FRONTIER_QUEUE_FILENAME = "frontier_queue.csv"


EXPANSION_LEDGER_COLUMNS = (
    "run_date",
    "round_number",
    "queue_item_id",
    "source_entity_key",
    "group_ids",
    "relationship_rows_found",
    "expansion_status",
)

FRONTIER_QUEUE_COLUMNS = (
    "queue_item_id",
    "run_date",
    "action_type",
    "subject_type",
    "subject_key",
    "feature_snapshot_hash",
    "group_ids",
    "trigger_decision_id",
    "queue_reason",
    "priority",
    "queue_status",
)


@dataclass(frozen=True)
class DailyStateSnapshot:
    """Complete persisted state loaded for one daily run."""

    network: UnifiedGroupResult
    decision_store: pd.DataFrame
    expansion_ledger: pd.DataFrame
    frontier_queue: pd.DataFrame


@dataclass(frozen=True)
class DailyIncrementalPlan:
    """Actionable work after persisted state is reconciled."""

    projection: DecisionProjectionResult
    actionable_queue: pd.DataFrame
    applied_decision_count: int
    queued_ai_action_count: int
    queued_expansion_action_count: int
    completed_queue_item_count: int


class CsvDailyStateStore:
    """
    CSV-backed implementation of the four persistent contracts.

    The same interface can later be implemented using Databricks tables
    without changing the incremental planning logic.
    """

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state_directory = Path(
            state_directory
        )

        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _path(
        self,
        filename: str,
    ) -> Path:
        return self.state_directory / filename

    def _write(
        self,
        frame: pd.DataFrame,
        filename: str,
    ) -> None:
        frame.to_csv(
            self._path(filename),
            index=False,
            lineterminator="\n",
        )

    def _read_network_frame(
        self,
        filename: str,
    ) -> pd.DataFrame:
        path = self._path(filename)

        if not path.exists():
            raise FileNotFoundError(
                f"Missing persisted network state: {path}"
            )

        return pd.read_csv(path)

    def _read_string_frame(
        self,
        filename: str,
        columns: tuple[str, ...],
    ) -> pd.DataFrame:
        path = self._path(filename)

        if not path.exists():
            return pd.DataFrame(
                columns=list(columns)
            )

        return pd.read_csv(
            path,
            dtype="string",
            keep_default_na=False,
        )

    def save_network_state(
        self,
        network: UnifiedGroupResult,
        run_date: date | str,
    ) -> None:
        """Persist the current canonical network snapshot."""
        resolved_run_date = parse_run_date(
            run_date
        )

        groups = network.groups.copy()
        nodes = network.nodes.copy()
        edges = network.edges.copy()

        for frame in (
            groups,
            nodes,
            edges,
        ):
            frame["state_updated_date"] = (
                resolved_run_date
            )

        self._write(
            groups,
            NETWORK_GROUPS_FILENAME,
        )

        self._write(
            nodes,
            NETWORK_NODES_FILENAME,
        )

        self._write(
            edges,
            NETWORK_EDGES_FILENAME,
        )

    def load_network_state(
        self,
    ) -> UnifiedGroupResult:
        """Load the canonical network snapshot."""
        return UnifiedGroupResult(
            groups=self._read_network_frame(
                NETWORK_GROUPS_FILENAME
            ),
            nodes=self._read_network_frame(
                NETWORK_NODES_FILENAME
            ),
            edges=self._read_network_frame(
                NETWORK_EDGES_FILENAME
            ),
        )

    def load_decision_store(
        self,
    ) -> pd.DataFrame:
        """Load all persisted structured decisions."""
        return self._read_string_frame(
            DECISION_STORE_FILENAME,
            DECISION_REQUIRED_COLUMNS,
        )

    def append_decisions(
        self,
        decisions: pd.DataFrame,
    ) -> pd.DataFrame:
        """Append decisions while retaining one decision per hash."""
        existing = self.load_decision_store()

        if decisions.empty:
            return existing

        combined = pd.concat(
            [
                existing,
                decisions[
                    list(
                        DECISION_REQUIRED_COLUMNS
                    )
                ],
            ],
            ignore_index=True,
        )

        prepared = prepare_decisions(
            combined
        )[
            list(DECISION_REQUIRED_COLUMNS)
        ]

        self._write(
            prepared,
            DECISION_STORE_FILENAME,
        )

        return prepared

    def load_expansion_ledger(
        self,
    ) -> pd.DataFrame:
        """Load completed or attempted expansion work."""
        return self._read_string_frame(
            EXPANSION_LEDGER_FILENAME,
            EXPANSION_LEDGER_COLUMNS,
        )

    def append_expansion_ledger(
        self,
        expansion_ledger: pd.DataFrame,
    ) -> pd.DataFrame:
        """Append expansion outcomes using the queue ID as key."""
        existing = self.load_expansion_ledger()

        if expansion_ledger.empty:
            return existing

        missing_columns = sorted(
            set(EXPANSION_LEDGER_COLUMNS)
            - set(expansion_ledger.columns)
        )

        if missing_columns:
            raise ValueError(
                "Expansion ledger is missing columns: "
                f"{missing_columns}"
            )

        combined = pd.concat(
            [
                existing,
                expansion_ledger[
                    list(
                        EXPANSION_LEDGER_COLUMNS
                    )
                ],
            ],
            ignore_index=True,
        )

        combined = (
            combined
            .drop_duplicates(
                subset=["queue_item_id"],
                keep="last",
            )
            .sort_values(
                by=[
                    "run_date",
                    "round_number",
                    "queue_item_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        self._write(
            combined,
            EXPANSION_LEDGER_FILENAME,
        )

        return combined

    def load_frontier_queue(
        self,
    ) -> pd.DataFrame:
        """Load unresolved persisted work."""
        return self._read_string_frame(
            FRONTIER_QUEUE_FILENAME,
            FRONTIER_QUEUE_COLUMNS,
        )

    def save_frontier_queue(
        self,
        frontier_queue: pd.DataFrame,
    ) -> None:
        """Replace the frontier with the current unresolved work."""
        if frontier_queue.empty:
            prepared = pd.DataFrame(
                columns=list(
                    FRONTIER_QUEUE_COLUMNS
                )
            )
        else:
            missing_columns = sorted(
                set(FRONTIER_QUEUE_COLUMNS)
                - set(frontier_queue.columns)
            )

            if missing_columns:
                raise ValueError(
                    "Frontier queue is missing columns: "
                    f"{missing_columns}"
                )

            prepared = (
                frontier_queue[
                    list(
                        FRONTIER_QUEUE_COLUMNS
                    )
                ]
                .drop_duplicates(
                    subset=["queue_item_id"],
                    keep="last",
                )
                .sort_values(
                    by=[
                        "priority",
                        "action_type",
                        "subject_key",
                    ],
                    kind="stable",
                )
                .reset_index(drop=True)
            )

        self._write(
            prepared,
            FRONTIER_QUEUE_FILENAME,
        )

    def load_snapshot(
        self,
    ) -> DailyStateSnapshot:
        """Load all persistent state contracts together."""
        return DailyStateSnapshot(
            network=self.load_network_state(),
            decision_store=(
                self.load_decision_store()
            ),
            expansion_ledger=(
                self.load_expansion_ledger()
            ),
            frontier_queue=(
                self.load_frontier_queue()
            ),
        )

    def commit_recursive_result(
        self,
        result: RecursiveExpansionResult,
        run_date: date | str,
    ) -> DailyStateSnapshot:
        """Commit one completed recursive run atomically by contract."""
        network = UnifiedGroupResult(
            groups=result.groups,
            nodes=result.nodes,
            edges=result.edges,
        )

        self.save_network_state(
            network=network,
            run_date=run_date,
        )

        self.append_decisions(
            result.decision_history
        )

        self.append_expansion_ledger(
            result.expansion_ledger
        )

        self.save_frontier_queue(
            result.remaining_queue
        )

        return self.load_snapshot()


def build_incremental_daily_plan(
    state_store: CsvDailyStateStore,
    run_date: date | str,
) -> DailyIncrementalPlan:
    """
    Reconcile current evidence with persisted decisions and expansions.

    Unchanged decisions are reused by matching feature hashes. Completed
    discovery actions are removed by their stable queue item IDs.
    """
    resolved_run_date = parse_run_date(
        run_date
    )

    snapshot = state_store.load_snapshot()

    projection = apply_persisted_decisions(
        unified_result=snapshot.network,
        decisions=snapshot.decision_store,
        run_date=resolved_run_date,
    )

    completed_queue_ids = set(
        snapshot.expansion_ledger.loc[
            snapshot.expansion_ledger[
                "expansion_status"
            ]
            .astype("string")
            .str.upper()
            .eq("COMPLETED"),
            "queue_item_id",
        ]
    )

    actionable_queue = (
        projection.expansion_queue.loc[
            ~projection.expansion_queue[
                "queue_item_id"
            ].isin(completed_queue_ids)
        ]
        .copy()
        .reset_index(drop=True)
    )

    state_store.save_frontier_queue(
        actionable_queue
    )

    queued_ai_action_count = int(
        actionable_queue[
            "action_type"
        ]
        .isin(
            [
                "RUN_COUNTERPARTY_AI",
                "RUN_CUSTOMER_AI",
            ]
        )
        .sum()
    )

    queued_expansion_action_count = int(
        actionable_queue[
            "action_type"
        ]
        .eq(
            "DISCOVER_CUSTOMER_RELATIONSHIPS"
        )
        .sum()
    )

    return DailyIncrementalPlan(
        projection=projection,
        actionable_queue=actionable_queue,
        applied_decision_count=len(
            projection.applied_decisions
        ),
        queued_ai_action_count=(
            queued_ai_action_count
        ),
        queued_expansion_action_count=(
            queued_expansion_action_count
        ),
        completed_queue_item_count=len(
            completed_queue_ids
        ),
    )
