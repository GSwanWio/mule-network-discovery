"""Read-only application contract over persisted run state."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from network_mule_discovery.consolidated_state import (
    ConsolidatedStateSnapshot,
    ConsolidatedStateStore,
)
from network_mule_discovery.run_state_manifest import (
    RunStateManifest,
)


ANALYST_RUN_TABLE_COLUMNS = (
    "run_id",
    "run_date",
    "provider_name",
    "dataset_id",
    "state_namespace",
    "run_status",
    "termination_status",
    "termination_reason",
    "is_current",
    "artifact_count",
    "missing_artifact_count",
)

ANALYST_GROUP_REQUIRED_COLUMNS = (
    "group_id",
    "group_anchor_seed_entity_key",
    "group_status",
    "customer_count",
    "counterparty_count",
    "eid_link_count",
    "shared_counterparty_customer_count",
    "beneficiary_seed_link_count",
    "customer_assessment_pending_count",
    "counterparty_ai_pending_count",
    "total_node_count",
    "total_edge_count",
)

ANALYST_GROUP_TABLE_COLUMNS = (
    "run_id",
    *ANALYST_GROUP_REQUIRED_COLUMNS,
    "approved_suspicious_counterparty_count",
    "suppressed_counterparty_count",
    "mule_like_customer_count",
    "ready_action_count",
    "failed_closed_action_count",
)


class AnalystApplicationStateError(RuntimeError):
    """Persisted analyst application state is invalid."""


@dataclass(frozen=True)
class AnalystRunSummary:
    """One analyst-visible persisted run."""

    run_id: str
    run_date: str
    provider_name: str
    dataset_id: str
    state_namespace: str
    run_status: str
    termination_status: str
    termination_reason: str
    is_current: bool
    artifact_count: int
    missing_artifact_count: int

    def to_record(self) -> dict[str, object]:
        """Return one tabular run record."""
        return asdict(self)


def _integer_value(
    row: pd.Series,
    column: str,
    *,
    default: int = 0,
) -> int:
    """Return one validated integer field."""
    if column not in row.index:
        return default

    value = row[column]

    if (
        pd.isna(value)
        or not str(value).strip()
    ):
        return default

    numeric = pd.to_numeric(
        pd.Series([value]),
        errors="coerce",
    ).iloc[0]

    if (
        pd.isna(numeric)
        or not float(numeric).is_integer()
    ):
        raise AnalystApplicationStateError(
            "Group field must contain an integer: "
            f"{column}={value}"
        )

    return int(numeric)


def _parse_group_ids(
    value: object,
) -> tuple[str, ...]:
    """Return normalized group IDs from one queue record."""
    if value is None or pd.isna(value):
        return ()

    return tuple(
        group_id
        for group_id in (
            part.strip()
            for part in str(value).split("|")
        )
        if group_id
    )


@dataclass(frozen=True)
class AnalystGroupSummary:
    """One analyst-visible persisted network group."""

    run_id: str
    group_id: str
    group_anchor_seed_entity_key: str
    group_status: str
    customer_count: int
    counterparty_count: int
    eid_link_count: int
    shared_counterparty_customer_count: int
    beneficiary_seed_link_count: int
    customer_assessment_pending_count: int
    counterparty_ai_pending_count: int
    total_node_count: int
    total_edge_count: int
    approved_suspicious_counterparty_count: int
    suppressed_counterparty_count: int
    mule_like_customer_count: int
    ready_action_count: int
    failed_closed_action_count: int

    def to_record(self) -> dict[str, object]:
        """Return one tabular group record."""
        return asdict(self)


class AnalystApplicationStateStore:
    """Read-only facade used by the analyst application."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state = ConsolidatedStateStore(
            state_directory
        )

    def _current_manifest(
        self,
    ) -> RunStateManifest | None:
        try:
            return self.state.manifest.load()
        except FileNotFoundError:
            return None

    def _artifact_directory(
        self,
        *,
        manifest: RunStateManifest,
        is_current: bool,
    ) -> Path:
        if is_current:
            return self.state.state_directory

        return (
            self.state.artifact_history_directory
            / manifest.run_id
        )

    def list_runs(
        self,
    ) -> tuple[AnalystRunSummary, ...]:
        """Return retained runs with the current run first."""
        current = self._current_manifest()
        summaries: list[AnalystRunSummary] = []

        for manifest in (
            self.state.manifest.list_manifests()
        ):
            is_current = (
                current is not None
                and manifest.run_id == current.run_id
            )
            artifact_directory = (
                self._artifact_directory(
                    manifest=manifest,
                    is_current=is_current,
                )
            )
            artifact_count = sum(
                (
                    artifact_directory
                    / filename
                ).is_file()
                for filename
                in manifest.artifact_filenames
            )

            summaries.append(
                AnalystRunSummary(
                    run_id=manifest.run_id,
                    run_date=manifest.run_date,
                    provider_name=(
                        manifest.provider_name
                    ),
                    dataset_id=manifest.dataset_id,
                    state_namespace=(
                        manifest.state_namespace
                    ),
                    run_status=manifest.run_status,
                    termination_status=(
                        manifest.termination_status
                    ),
                    termination_reason=(
                        manifest.termination_reason
                    ),
                    is_current=is_current,
                    artifact_count=artifact_count,
                    missing_artifact_count=(
                        len(
                            manifest
                            .artifact_filenames
                        )
                        - artifact_count
                    ),
                )
            )

        return tuple(
            sorted(
                summaries,
                key=lambda summary: (
                    summary.is_current,
                    summary.run_date,
                    summary.run_id,
                ),
                reverse=True,
            )
        )

    def run_table(self) -> pd.DataFrame:
        """Return the analyst-visible run table."""
        return pd.DataFrame.from_records(
            [
                summary.to_record()
                for summary in self.list_runs()
            ],
            columns=ANALYST_RUN_TABLE_COLUMNS,
        )

    def list_groups(
        self,
        run_id: str,
    ) -> tuple[AnalystGroupSummary, ...]:
        """Return persisted groups for one selected run."""
        snapshot = self.load_run(run_id)
        groups = (
            snapshot.daily_state.network.groups
            .copy()
        )

        if groups.empty:
            return ()

        missing_columns = sorted(
            set(ANALYST_GROUP_REQUIRED_COLUMNS)
            - set(groups.columns)
        )

        if missing_columns:
            raise AnalystApplicationStateError(
                "Persisted groups are missing columns: "
                f"{missing_columns}"
            )

        queue_counts: dict[
            str,
            dict[str, int],
        ] = {}

        frontier_queue = (
            snapshot.daily_state.frontier_queue
        )

        if not frontier_queue.empty:
            if (
                "group_ids"
                not in frontier_queue.columns
                or "queue_status"
                not in frontier_queue.columns
            ):
                raise AnalystApplicationStateError(
                    "Persisted frontier queue is missing "
                    "group_ids or queue_status."
                )

            for queue_row in (
                frontier_queue.itertuples(
                    index=False
                )
            ):
                queue_status = str(
                    queue_row.queue_status
                ).strip().upper()

                for group_id in _parse_group_ids(
                    queue_row.group_ids
                ):
                    counts = queue_counts.setdefault(
                        group_id,
                        {
                            "READY": 0,
                            "FAILED_CLOSED": 0,
                        },
                    )

                    if queue_status in counts:
                        counts[queue_status] += 1

        summaries: list[
            AnalystGroupSummary
        ] = []

        for row in groups.itertuples(
            index=False,
            name=None,
        ):
            group_row = pd.Series(
                row,
                index=groups.columns,
            )
            group_id = str(
                group_row["group_id"]
            ).strip()
            counts = queue_counts.get(
                group_id,
                {
                    "READY": 0,
                    "FAILED_CLOSED": 0,
                },
            )

            summaries.append(
                AnalystGroupSummary(
                    run_id=(
                        snapshot.manifest.run_id
                    ),
                    group_id=group_id,
                    group_anchor_seed_entity_key=(
                        str(
                            group_row[
                                "group_anchor_seed_entity_key"
                            ]
                        ).strip()
                    ),
                    group_status=str(
                        group_row["group_status"]
                    ).strip(),
                    customer_count=_integer_value(
                        group_row,
                        "customer_count",
                    ),
                    counterparty_count=_integer_value(
                        group_row,
                        "counterparty_count",
                    ),
                    eid_link_count=_integer_value(
                        group_row,
                        "eid_link_count",
                    ),
                    shared_counterparty_customer_count=(
                        _integer_value(
                            group_row,
                            "shared_counterparty_customer_count",
                        )
                    ),
                    beneficiary_seed_link_count=(
                        _integer_value(
                            group_row,
                            "beneficiary_seed_link_count",
                        )
                    ),
                    customer_assessment_pending_count=(
                        _integer_value(
                            group_row,
                            "customer_assessment_pending_count",
                        )
                    ),
                    counterparty_ai_pending_count=(
                        _integer_value(
                            group_row,
                            "counterparty_ai_pending_count",
                        )
                    ),
                    total_node_count=_integer_value(
                        group_row,
                        "total_node_count",
                    ),
                    total_edge_count=_integer_value(
                        group_row,
                        "total_edge_count",
                    ),
                    approved_suspicious_counterparty_count=(
                        _integer_value(
                            group_row,
                            "approved_suspicious_counterparty_count",
                        )
                    ),
                    suppressed_counterparty_count=(
                        _integer_value(
                            group_row,
                            "suppressed_counterparty_count",
                        )
                    ),
                    mule_like_customer_count=(
                        _integer_value(
                            group_row,
                            "mule_like_customer_count",
                        )
                    ),
                    ready_action_count=counts[
                        "READY"
                    ],
                    failed_closed_action_count=(
                        counts["FAILED_CLOSED"]
                    ),
                )
            )

        return tuple(
            sorted(
                summaries,
                key=lambda summary: (
                    summary.group_id
                ),
            )
        )

    def group_table(
        self,
        run_id: str,
    ) -> pd.DataFrame:
        """Return the analyst-visible group table."""
        return pd.DataFrame.from_records(
            [
                summary.to_record()
                for summary
                in self.list_groups(run_id)
            ],
            columns=ANALYST_GROUP_TABLE_COLUMNS,
        )

    def load_run(
        self,
        run_id: str,
    ) -> ConsolidatedStateSnapshot:
        """Load the selected current or historical run."""
        normalized_run_id = str(
            run_id
        ).strip()

        summaries = {
            summary.run_id: summary
            for summary in self.list_runs()
        }

        if normalized_run_id not in summaries:
            raise AnalystApplicationStateError(
                "Unknown persisted run: "
                f"{normalized_run_id}"
            )

        summary = summaries[normalized_run_id]

        return self.state.load(
            run_id=(
                None
                if summary.is_current
                else normalized_run_id
            )
        )
