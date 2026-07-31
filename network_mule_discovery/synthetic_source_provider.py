"""Reusable provider for deterministic synthetic source snapshots."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceContractError,
    SourceLoadRequest,
    SourceMetadata,
)
from network_mule_discovery.source_dataset_contracts import (
    SOURCE_DATASET_CONTRACTS,
)
from network_mule_discovery.source_snapshot import (
    calculate_source_snapshot_hash,
)


class SyntheticSourceProvider:
    """Load one generated synthetic scenario through the source contract."""

    def __init__(
        self,
        *,
        source_directory: Path | str,
        source_manifest: Mapping[str, object] | None = None,
    ) -> None:
        resolved_directory = Path(source_directory)

        if not resolved_directory.is_dir():
            raise SourceContractError(
                "Synthetic source directory does not exist: "
                f"{resolved_directory}"
            )

        if source_manifest is None:
            manifest_path = (
                resolved_directory
                / "source_manifest.json"
            )

            if not manifest_path.is_file():
                raise SourceContractError(
                    "Synthetic source manifest does not exist: "
                    f"{manifest_path}"
                )

            try:
                loaded_manifest = json.loads(
                    manifest_path.read_text(
                        encoding="utf-8"
                    )
                )
            except (
                OSError,
                json.JSONDecodeError,
            ) as exc:
                raise SourceContractError(
                    "Synthetic source manifest could not be read."
                ) from exc
        else:
            loaded_manifest = source_manifest

        if not isinstance(
            loaded_manifest,
            Mapping,
        ):
            raise SourceContractError(
                "Synthetic source manifest must be a mapping."
            )

        self._source_directory = resolved_directory
        self._source_manifest = dict(
            loaded_manifest
        )
        self._load_count = 0

    @property
    def provider_name(self) -> str:
        """Return the stable provider identifier."""
        return "synthetic"

    @property
    def load_count(self) -> int:
        """Return the number of attempted snapshot loads."""
        return self._load_count

    @property
    def source_directory(self) -> Path:
        """Return the physical synthetic snapshot location."""
        return self._source_directory

    @property
    def source_manifest(self) -> dict[str, object]:
        """Return a defensive copy of the source manifest."""
        return dict(self._source_manifest)

    def load(
        self,
        request: SourceLoadRequest,
    ) -> DiscoverySourceBundle:
        """Load all nine source datasets for one request."""
        if not isinstance(
            request,
            SourceLoadRequest,
        ):
            raise SourceContractError(
                "request must be a SourceLoadRequest."
            )

        self._load_count += 1

        manifest_source_files = (
            self._source_manifest.get(
                "source_files"
            )
        )

        if not isinstance(
            manifest_source_files,
            list,
        ):
            raise SourceContractError(
                "Synthetic source manifest source_files "
                "must be a list."
            )

        declared_source_files = {
            str(value).strip()
            for value in manifest_source_files
            if str(value).strip()
        }

        frames: dict[str, pd.DataFrame] = {}

        for dataset_name in SOURCE_DATASET_NAMES:
            source_path = (
                self._source_directory
                / f"{dataset_name}.csv"
            )

            if source_path.is_file():
                frames[dataset_name] = pd.read_csv(
                    source_path,
                    dtype="string",
                    keep_default_na=False,
                )
            elif source_path.name in declared_source_files:
                raise SourceContractError(
                    "Declared synthetic source dataset "
                    "does not exist: "
                    f"{source_path}"
                )
            else:
                frames[dataset_name] = pd.DataFrame(
                    columns=(
                        SOURCE_DATASET_CONTRACTS[
                            dataset_name
                        ].columns
                    )
                )

            expected_columns = set(
                SOURCE_DATASET_CONTRACTS[
                    dataset_name
                ].columns
            )
            missing_columns = sorted(
                expected_columns
                - set(frames[dataset_name].columns)
            )

            if missing_columns:
                raise SourceContractError(
                    f"{dataset_name} is missing columns: "
                    f"{missing_columns}"
                )

        snapshot_hash = (
            calculate_source_snapshot_hash(
                dataset_id=request.dataset_id,
                run_date=request.run_date,
                frames=frames,
            )
        )

        return DiscoverySourceBundle(
            metadata=SourceMetadata(
                provider_name=self.provider_name,
                dataset_id=request.dataset_id,
                state_namespace=(
                    request.state_namespace
                ),
                run_date=request.run_date,
                source_manifest=(
                    self._source_manifest
                ),
                source_snapshot_hash=(
                    snapshot_hash
                ),
            ),
            **frames,
        )
