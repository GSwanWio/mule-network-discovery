"""Deterministic run identity over the existing persisted state files."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from network_mule_discovery.daily_ai_runner import (
    AI_CALL_LEDGER_FILENAME,
)
from network_mule_discovery.daily_state import (
    DECISION_STORE_FILENAME,
    EXPANSION_LEDGER_FILENAME,
    FRONTIER_QUEUE_FILENAME,
    NETWORK_EDGES_FILENAME,
    NETWORK_GROUPS_FILENAME,
    NETWORK_NODES_FILENAME,
)
from network_mule_discovery.operational_resilience import (
    OPERATIONAL_RESILIENCE_GATE_FILENAME,
)
from network_mule_discovery.production_ai_runtime import (
    PRODUCTION_AI_RUNTIME_FILENAME,
    PRODUCTION_AI_STARTUP_FAILURE_FILENAME,
)
from network_mule_discovery.source_contracts import (
    SourceMetadata,
)
from network_mule_discovery.technical_reprocessing import (
    TECHNICAL_REPROCESSING_LEDGER_FILENAME,
)


RUN_STATE_MANIFEST_FILENAME = "run_state_manifest.json"
RUN_STATE_MANIFEST_HISTORY_DIRECTORY = "run_manifests"
RUN_STATE_ARTIFACT_HISTORY_DIRECTORY = "run_artifacts"
RUN_STATE_MANIFEST_VERSION = "run-state-manifest-v1"

RUN_STATE_ARTIFACT_FILENAMES = (
    NETWORK_GROUPS_FILENAME,
    NETWORK_NODES_FILENAME,
    NETWORK_EDGES_FILENAME,
    DECISION_STORE_FILENAME,
    EXPANSION_LEDGER_FILENAME,
    FRONTIER_QUEUE_FILENAME,
    AI_CALL_LEDGER_FILENAME,
    PRODUCTION_AI_RUNTIME_FILENAME,
    PRODUCTION_AI_STARTUP_FAILURE_FILENAME,
    TECHNICAL_REPROCESSING_LEDGER_FILENAME,
    OPERATIONAL_RESILIENCE_GATE_FILENAME,
)

RUN_STATUSES = (
    "INITIALIZED",
    "RUNNING",
    "STOPPED",
    "TERMINATED",
    "FAILED",
)


class RunStateManifestError(RuntimeError):
    """Run-level persisted identity or metadata is invalid."""


def _require_nonblank(
    value: object,
    field_name: str,
) -> str:
    normalized = str(value).strip()

    if not normalized:
        raise RunStateManifestError(
            f"{field_name} must be nonblank."
        )

    return normalized


def build_run_id(
    metadata: SourceMetadata,
) -> str:
    """Build a restart-stable ID from source and namespace identity."""
    if not isinstance(metadata, SourceMetadata):
        raise RunStateManifestError(
            "metadata must be SourceMetadata."
        )

    canonical_identity = "|".join(
        [
            metadata.provider_name,
            metadata.dataset_id,
            metadata.state_namespace,
            metadata.run_date.isoformat(),
            metadata.source_snapshot_hash,
        ]
    )

    digest = hashlib.sha256(
        canonical_identity.encode("utf-8")
    ).hexdigest()[:20]

    return f"RUN_{digest}"


@dataclass(frozen=True)
class RunStateManifest:
    """Persisted identity and artifact contract for one logical run."""

    manifest_version: str
    run_id: str
    provider_name: str
    dataset_id: str
    state_namespace: str
    run_date: str
    source_snapshot_hash: str
    run_status: str
    termination_status: str
    termination_reason: str
    artifact_filenames: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.manifest_version != RUN_STATE_MANIFEST_VERSION:
            raise RunStateManifestError(
                "Unsupported run-state manifest version."
            )

        if re.fullmatch(
            r"RUN_[0-9a-f]{20}",
            self.run_id,
        ) is None:
            raise RunStateManifestError(
                "run_id has an invalid format."
            )

        for field_name in (
            "provider_name",
            "dataset_id",
            "state_namespace",
            "run_date",
        ):
            _require_nonblank(
                getattr(self, field_name),
                field_name,
            )

        if re.fullmatch(
            r"[0-9a-f]{64}",
            self.source_snapshot_hash,
        ) is None:
            raise RunStateManifestError(
                "source_snapshot_hash must be a "
                "64-character SHA-256 value."
            )

        if self.run_status not in RUN_STATUSES:
            raise RunStateManifestError(
                "Unsupported run_status: "
                f"{self.run_status}"
            )

        if (
            tuple(self.artifact_filenames)
            != RUN_STATE_ARTIFACT_FILENAMES
        ):
            raise RunStateManifestError(
                "artifact_filenames do not match the "
                "run-state artifact contract."
            )

        if self.run_status in {
            "STOPPED",
            "TERMINATED",
            "FAILED",
        }:
            _require_nonblank(
                self.termination_reason,
                "termination_reason",
            )

    def to_record(self) -> dict[str, object]:
        """Return a JSON-safe manifest record."""
        return {
            "manifest_version": self.manifest_version,
            "run_id": self.run_id,
            "provider_name": self.provider_name,
            "dataset_id": self.dataset_id,
            "state_namespace": self.state_namespace,
            "run_date": self.run_date,
            "source_snapshot_hash": (
                self.source_snapshot_hash
            ),
            "run_status": self.run_status,
            "termination_status": (
                self.termination_status
            ),
            "termination_reason": (
                self.termination_reason
            ),
            "artifact_filenames": list(
                self.artifact_filenames
            ),
        }

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, object],
    ) -> RunStateManifest:
        """Rebuild and validate one persisted record."""
        if not isinstance(record, Mapping):
            raise RunStateManifestError(
                "Run-state manifest must be a mapping."
            )

        required_fields = {
            "manifest_version",
            "run_id",
            "provider_name",
            "dataset_id",
            "state_namespace",
            "run_date",
            "source_snapshot_hash",
            "run_status",
            "termination_status",
            "termination_reason",
            "artifact_filenames",
        }
        missing_fields = sorted(
            required_fields - set(record)
        )

        if missing_fields:
            raise RunStateManifestError(
                "Run-state manifest is missing fields: "
                f"{missing_fields}"
            )

        artifacts = record["artifact_filenames"]

        if not isinstance(artifacts, list):
            raise RunStateManifestError(
                "artifact_filenames must be a list."
            )

        return cls(
            manifest_version=str(
                record["manifest_version"]
            ),
            run_id=str(record["run_id"]),
            provider_name=str(
                record["provider_name"]
            ),
            dataset_id=str(record["dataset_id"]),
            state_namespace=str(
                record["state_namespace"]
            ),
            run_date=str(record["run_date"]),
            source_snapshot_hash=str(
                record["source_snapshot_hash"]
            ),
            run_status=str(record["run_status"]),
            termination_status=str(
                record["termination_status"]
            ),
            termination_reason=str(
                record["termination_reason"]
            ),
            artifact_filenames=tuple(
                str(value)
                for value in artifacts
            ),
        )


def build_run_state_manifest(
    metadata: SourceMetadata,
) -> RunStateManifest:
    """Build an initialized manifest from loaded source metadata."""
    return RunStateManifest(
        manifest_version=RUN_STATE_MANIFEST_VERSION,
        run_id=build_run_id(metadata),
        provider_name=metadata.provider_name,
        dataset_id=metadata.dataset_id,
        state_namespace=metadata.state_namespace,
        run_date=metadata.run_date.isoformat(),
        source_snapshot_hash=(
            metadata.source_snapshot_hash
        ),
        run_status="INITIALIZED",
        termination_status="",
        termination_reason="",
        artifact_filenames=(
            RUN_STATE_ARTIFACT_FILENAMES
        ),
    )


class JsonRunStateManifestStore:
    """Atomic current and historical run-manifest persistence."""

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

        self.path = (
            self.state_directory
            / RUN_STATE_MANIFEST_FILENAME
        )
        self.history_directory = (
            self.state_directory
            / RUN_STATE_MANIFEST_HISTORY_DIRECTORY
        )
        self.history_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _history_path(
        self,
        run_id: str,
    ) -> Path:
        if re.fullmatch(
            r"RUN_[0-9a-f]{20}",
            str(run_id),
        ) is None:
            raise RunStateManifestError(
                "run_id has an invalid format."
            )

        return (
            self.history_directory
            / f"{run_id}.json"
        )

    def _load_path(
        self,
        path: Path,
    ) -> RunStateManifest:
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing run-state manifest: {path}"
            )

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
            raise RunStateManifestError(
                "Run-state manifest could not be read."
            ) from exc

        return RunStateManifest.from_record(
            record
        )

    def load(
        self,
        run_id: str | None = None,
    ) -> RunStateManifest:
        """Load the current or one historical run manifest."""
        path = (
            self.path
            if run_id is None
            else self._history_path(run_id)
        )

        return self._load_path(path)

    def list_manifests(
        self,
    ) -> tuple[RunStateManifest, ...]:
        """Return all retained logical runs in stable order."""
        manifests = [
            self._load_path(path)
            for path in sorted(
                self.history_directory.glob(
                    "RUN_*.json"
                )
            )
        ]

        return tuple(
            sorted(
                manifests,
                key=lambda manifest: (
                    manifest.run_date,
                    manifest.run_id,
                ),
            )
        )

    def _save_path(
        self,
        *,
        path: Path,
        manifest: RunStateManifest,
    ) -> None:
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary = path.with_suffix(
            ".json.tmp"
        )
        temporary.write_text(
            json.dumps(
                manifest.to_record(),
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def save(
        self,
        manifest: RunStateManifest,
    ) -> Path:
        """Persist both historical and current manifest copies."""
        if not isinstance(
            manifest,
            RunStateManifest,
        ):
            raise RunStateManifestError(
                "manifest must be RunStateManifest."
            )

        self._save_path(
            path=self._history_path(
                manifest.run_id
            ),
            manifest=manifest,
        )
        self._save_path(
            path=self.path,
            manifest=manifest,
        )

        return self.path

    @staticmethod
    def _validate_same_identity(
        *,
        existing: RunStateManifest,
        expected: RunStateManifest,
    ) -> None:
        identity_fields = (
            "manifest_version",
            "run_id",
            "provider_name",
            "dataset_id",
            "state_namespace",
            "run_date",
            "source_snapshot_hash",
            "artifact_filenames",
        )

        mismatches = [
            field_name
            for field_name in identity_fields
            if getattr(existing, field_name)
            != getattr(expected, field_name)
        ]

        if mismatches:
            raise RunStateManifestError(
                "Persisted run identity conflicts "
                f"with expected fields: {mismatches}"
            )

    def initialize(
        self,
        metadata: SourceMetadata,
    ) -> RunStateManifest:
        """Create, resume, or switch the current logical run."""
        expected = build_run_state_manifest(
            metadata
        )

        if self.path.is_file():
            current = self._load_path(
                self.path
            )
            current_history_path = (
                self._history_path(
                    current.run_id
                )
            )

            if not current_history_path.is_file():
                self._save_path(
                    path=current_history_path,
                    manifest=current,
                )

            if current.run_id == expected.run_id:
                self._validate_same_identity(
                    existing=current,
                    expected=expected,
                )
                return current

        expected_history_path = (
            self._history_path(
                expected.run_id
            )
        )

        if expected_history_path.is_file():
            existing = self._load_path(
                expected_history_path
            )
            self._validate_same_identity(
                existing=existing,
                expected=expected,
            )
            self._save_path(
                path=self.path,
                manifest=existing,
            )
            return existing

        self.save(expected)

        return expected
