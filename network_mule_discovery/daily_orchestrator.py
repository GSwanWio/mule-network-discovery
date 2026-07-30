"""Production-path orchestration for daily network discovery."""

from __future__ import annotations

from dataclasses import dataclass

from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    DiscoverySourceProvider,
    SourceContractError,
    SourceLoadRequest,
    SourceMetadata,
)
from network_mule_discovery.source_dataset_contracts import (
    validate_source_bundle,
)
from network_mule_discovery.source_snapshot import (
    verify_source_bundle_snapshot,
)


@dataclass(frozen=True)
class DailySourcePreflightResult:
    """Validated source snapshot ready for discovery stages."""

    source_bundle: DiscoverySourceBundle
    source_snapshot_hash: str
    source_row_counts: tuple[tuple[str, int], ...]


def _provider_name(
    source_provider: DiscoverySourceProvider,
) -> str:
    provider_name = str(
        source_provider.provider_name
    ).strip()

    if not provider_name:
        raise SourceContractError(
            "source_provider.provider_name must be nonblank."
        )

    return provider_name


def _validate_source_identity(
    *,
    source_provider: DiscoverySourceProvider,
    source_request: SourceLoadRequest,
    source_bundle: DiscoverySourceBundle,
) -> None:
    metadata = source_bundle.metadata

    if not isinstance(metadata, SourceMetadata):
        raise SourceContractError(
            "source bundle metadata must be SourceMetadata."
        )

    expected_values = {
        "provider_name": _provider_name(
            source_provider
        ),
        "dataset_id": source_request.dataset_id,
        "run_date": source_request.run_date,
        "state_namespace": (
            source_request.state_namespace
        ),
    }

    for field_name, expected_value in (
        expected_values.items()
    ):
        actual_value = getattr(
            metadata,
            field_name,
        )

        if actual_value != expected_value:
            raise SourceContractError(
                "Loaded source metadata "
                f"{field_name} does not match "
                "the source request."
            )


def run_source_preflight(
    *,
    source_provider: DiscoverySourceProvider,
    source_request: SourceLoadRequest,
) -> DailySourcePreflightResult:
    """Load, validate, and verify one daily source snapshot."""
    if not isinstance(
        source_request,
        SourceLoadRequest,
    ):
        raise SourceContractError(
            "source_request must be a SourceLoadRequest."
        )

    normalized_request = SourceLoadRequest.create(
        dataset_id=source_request.dataset_id,
        run_date=source_request.run_date,
        state_namespace=(
            source_request.state_namespace
        ),
    )

    if not isinstance(
        source_provider,
        DiscoverySourceProvider,
    ):
        raise SourceContractError(
            "source_provider must implement "
            "DiscoverySourceProvider."
        )

    loaded_bundle = source_provider.load(
        normalized_request
    )

    validated_bundle = validate_source_bundle(
        loaded_bundle
    )

    _validate_source_identity(
        source_provider=source_provider,
        source_request=normalized_request,
        source_bundle=validated_bundle,
    )

    source_snapshot_hash = (
        verify_source_bundle_snapshot(
            validated_bundle
        )
    )

    row_counts = validated_bundle.row_counts()

    return DailySourcePreflightResult(
        source_bundle=validated_bundle,
        source_snapshot_hash=(
            source_snapshot_hash
        ),
        source_row_counts=tuple(
            (
                dataset_name,
                row_counts[dataset_name],
            )
            for dataset_name
            in SOURCE_DATASET_NAMES
        ),
    )
