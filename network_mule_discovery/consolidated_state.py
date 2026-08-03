"""Unified access to current and historical persisted run state."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path

import pandas as pd

from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    DailyStateSnapshot,
)
from network_mule_discovery.operational_resilience import (
    OPERATIONAL_RESILIENCE_GATE_FILENAME,
)
from network_mule_discovery.run_state_manifest import (
    RUN_STATE_ARTIFACT_FILENAMES,
    RUN_STATE_ARTIFACT_HISTORY_DIRECTORY,
    JsonRunStateManifestStore,
    RunStateManifest,
)
from network_mule_discovery.source_contracts import (
    SourceMetadata,
)
from network_mule_discovery.technical_reprocessing import (
    CsvTechnicalReprocessingLedger,
)


class ConsolidatedStateError(RuntimeError):
    """Consolidated persisted state could not be loaded."""


@dataclass(frozen=True)
class ConsolidatedStateSnapshot:
    """All persisted state associated with one logical run."""

    manifest: RunStateManifest
    artifact_directory: Path
    daily_state: DailyStateSnapshot
    ai_call_ledger: pd.DataFrame
    technical_reprocessing_ledger: pd.DataFrame
    operational_resilience_report: (
        Mapping[str, object] | None
    )
    artifact_presence: Mapping[str, bool]

    @property
    def missing_artifacts(self) -> tuple[str, ...]:
        """Return declared artifacts not currently materialized."""
        return tuple(
            filename
            for filename in RUN_STATE_ARTIFACT_FILENAMES
            if not self.artifact_presence[filename]
        )


class ConsolidatedStateStore:
    """
    Facade over current state and immutable per-run snapshots.

    Existing stores retain responsibility for schemas, deduplication,
    audit rules, and their normal persistence behavior.
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

        self.manifest = JsonRunStateManifestStore(
            self.state_directory
        )
        self.daily_state = CsvDailyStateStore(
            self.state_directory
        )
        self.ai_calls = CsvAiCallLedger(
            self.state_directory
        )
        self.technical_reprocessing = (
            CsvTechnicalReprocessingLedger(
                self.state_directory
            )
        )
        self.artifact_history_directory = (
            self.state_directory
            / RUN_STATE_ARTIFACT_HISTORY_DIRECTORY
        )
        self.artifact_history_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    @property
    def operational_resilience_report_path(
        self,
    ) -> Path:
        """Return the current operational report location."""
        return (
            self.state_directory
            / OPERATIONAL_RESILIENCE_GATE_FILENAME
        )

    def initialize(
        self,
        metadata: SourceMetadata,
    ) -> RunStateManifest:
        """Create, resume, or select one logical run."""
        return self.manifest.initialize(
            metadata
        )

    def update_run_status(
        self,
        *,
        run_status: str,
        termination_status: str = "",
        termination_reason: str = "",
    ) -> RunStateManifest:
        """Persist status while retaining logical run identity."""
        current = self.manifest.load()

        updated = replace(
            current,
            run_status=run_status,
            termination_status=termination_status,
            termination_reason=termination_reason,
        )

        self.manifest.save(updated)

        return updated

    def _historical_artifact_directory(
        self,
        run_id: str,
    ) -> Path:
        manifest = self.manifest.load(
            run_id=run_id
        )

        return (
            self.artifact_history_directory
            / manifest.run_id
        )

    def snapshot_current_run(
        self,
    ) -> Path:
        """Replace the current run's immutable audit snapshot."""
        manifest = self.manifest.load()
        destination = (
            self.artifact_history_directory
            / manifest.run_id
        )
        temporary = (
            self.artifact_history_directory
            / f".{manifest.run_id}.tmp"
        )

        if temporary.exists():
            shutil.rmtree(temporary)

        temporary.mkdir(
            parents=True,
            exist_ok=False,
        )

        for filename in RUN_STATE_ARTIFACT_FILENAMES:
            source = (
                self.state_directory
                / filename
            )

            if source.is_file():
                shutil.copy2(
                    source,
                    temporary / filename,
                )

        if destination.exists():
            shutil.rmtree(destination)

        temporary.replace(destination)

        return destination

    @staticmethod
    def _load_operational_report(
        artifact_directory: Path,
    ) -> Mapping[str, object] | None:
        path = (
            artifact_directory
            / OPERATIONAL_RESILIENCE_GATE_FILENAME
        )

        if not path.is_file():
            return None

        try:
            record = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )
        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:
            raise ConsolidatedStateError(
                "Operational resilience report "
                "could not be read."
            ) from exc

        if not isinstance(record, dict):
            raise ConsolidatedStateError(
                "Operational resilience report "
                "must contain a JSON object."
            )

        return record

    def load(
        self,
        run_id: str | None = None,
    ) -> ConsolidatedStateSnapshot:
        """Load current state or one retained run snapshot."""
        if run_id is None:
            manifest = self.manifest.load()
            artifact_directory = (
                self.state_directory
            )
        else:
            manifest = self.manifest.load(
                run_id=run_id
            )
            artifact_directory = (
                self._historical_artifact_directory(
                    manifest.run_id
                )
            )

            if not artifact_directory.is_dir():
                raise ConsolidatedStateError(
                    "Historical run artifacts are missing: "
                    f"{artifact_directory}"
                )

        artifact_presence = {
            filename: (
                artifact_directory
                / filename
            ).is_file()
            for filename
            in RUN_STATE_ARTIFACT_FILENAMES
        }

        try:
            daily_state = CsvDailyStateStore(
                artifact_directory
            ).load_snapshot()
            ai_call_ledger = CsvAiCallLedger(
                artifact_directory
            ).load()
            technical_reprocessing_ledger = (
                CsvTechnicalReprocessingLedger(
                    artifact_directory
                ).load()
            )
        except Exception as exc:
            raise ConsolidatedStateError(
                "Consolidated run state could not "
                "be loaded."
            ) from exc

        return ConsolidatedStateSnapshot(
            manifest=manifest,
            artifact_directory=artifact_directory,
            daily_state=daily_state,
            ai_call_ledger=ai_call_ledger,
            technical_reprocessing_ledger=(
                technical_reprocessing_ledger
            ),
            operational_resilience_report=(
                self._load_operational_report(
                    artifact_directory
                )
            ),
            artifact_presence=artifact_presence,
        )
