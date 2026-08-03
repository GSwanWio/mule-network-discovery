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
