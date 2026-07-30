"""Provider-neutral source contracts for network discovery."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Protocol, runtime_checkable

import pandas as pd


SOURCE_DATASET_NAMES = (
    "seed_mule_pool",
    "customer_identity",
    "customer_account_master",
    "local_inward_payments",
    "local_outward_payments",
    "international_inward_payments",
    "international_outward_payments",
    "retail_beneficiary_master",
    "sme_beneficiary_master",
)


class SourceContractError(ValueError):
    """Raised when a source request or bundle violates its contract."""


def _require_nonblank(
    value: object,
    field_name: str,
) -> str:
    normalized = (
        str(value).strip()
        if value is not None
        else ""
    )

    if not normalized:
        raise SourceContractError(
            f"{field_name} must be a nonblank string."
        )

    return normalized


def _parse_run_date(
    value: date | str,
) -> date:
    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(
            str(value).strip()
        )
    except ValueError as exc:
        raise SourceContractError(
            "run_date must use ISO format YYYY-MM-DD."
        ) from exc


@dataclass(frozen=True)
class SourceLoadRequest:
    """Request passed to any physical source provider."""

    dataset_id: str
    run_date: date
    state_namespace: str

    @classmethod
    def create(
        cls,
        *,
        dataset_id: str,
        run_date: date | str,
        state_namespace: str,
    ) -> SourceLoadRequest:
        return cls(
            dataset_id=_require_nonblank(
                dataset_id,
                "dataset_id",
            ),
            run_date=_parse_run_date(
                run_date
            ),
            state_namespace=_require_nonblank(
                state_namespace,
                "state_namespace",
            ),
        )


@dataclass(frozen=True)
class SourceMetadata:
    """Auditable identity of one loaded source snapshot."""

    provider_name: str
    dataset_id: str
    state_namespace: str
    run_date: date
    source_manifest: Mapping[str, object]
    source_snapshot_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider_name",
            _require_nonblank(
                self.provider_name,
                "provider_name",
            ),
        )

        object.__setattr__(
            self,
            "dataset_id",
            _require_nonblank(
                self.dataset_id,
                "dataset_id",
            ),
        )

        object.__setattr__(
            self,
            "state_namespace",
            _require_nonblank(
                self.state_namespace,
                "state_namespace",
            ),
        )

        object.__setattr__(
            self,
            "run_date",
            _parse_run_date(
                self.run_date
            ),
        )

        if not isinstance(
            self.source_manifest,
            Mapping,
        ):
            raise SourceContractError(
                "source_manifest must be a mapping."
            )

        normalized_hash = _require_nonblank(
            self.source_snapshot_hash,
            "source_snapshot_hash",
        ).lower()

        if re.fullmatch(
            r"[0-9a-f]{64}",
            normalized_hash,
        ) is None:
            raise SourceContractError(
                "source_snapshot_hash must be a "
                "64-character SHA-256 hexadecimal value."
            )

        object.__setattr__(
            self,
            "source_snapshot_hash",
            normalized_hash,
        )


@dataclass(frozen=True)
class DiscoverySourceBundle:
    """Normalized frames consumed by the application pipeline."""

    metadata: SourceMetadata
    seed_mule_pool: pd.DataFrame
    customer_identity: pd.DataFrame
    customer_account_master: pd.DataFrame
    local_inward_payments: pd.DataFrame
    local_outward_payments: pd.DataFrame
    international_inward_payments: pd.DataFrame
    international_outward_payments: pd.DataFrame
    retail_beneficiary_master: pd.DataFrame
    sme_beneficiary_master: pd.DataFrame

    def __post_init__(self) -> None:
        for (
            dataset_name,
            frame,
        ) in self.as_mapping().items():
            if not isinstance(
                frame,
                pd.DataFrame,
            ):
                raise SourceContractError(
                    f"{dataset_name} must be a "
                    "pandas DataFrame."
                )

    def as_mapping(
        self,
    ) -> dict[str, pd.DataFrame]:
        """Return frames in stable contract order."""
        return {
            dataset_name: getattr(
                self,
                dataset_name,
            )
            for dataset_name
            in SOURCE_DATASET_NAMES
        }

    def row_counts(
        self,
    ) -> dict[str, int]:
        """Return auditable source row counts."""
        return {
            dataset_name: len(frame)
            for (
                dataset_name,
                frame,
            ) in self.as_mapping().items()
        }


@runtime_checkable
class DiscoverySourceProvider(Protocol):
    """Source boundary shared by synthetic and Databricks."""

    @property
    def provider_name(self) -> str:
        """Return the stable provider identifier."""
        ...

    def load(
        self,
        request: SourceLoadRequest,
    ) -> DiscoverySourceBundle:
        """Load one validated source snapshot."""
        ...
