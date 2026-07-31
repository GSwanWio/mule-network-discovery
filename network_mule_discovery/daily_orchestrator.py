"""Production-path orchestration for daily network discovery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from network_mule_discovery.bundle_behavioral_sources import (
    build_bundle_counterparty_payloads,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.frontier_ai import (
    CounterpartyFrontierRunResult,
    run_counterparty_ai_frontier,
)
from network_mule_discovery.bundle_source_adapter import (
    build_canonical_discovery_inputs_from_bundle,
)
from network_mule_discovery.counterparty_discovery import (
    CounterpartyDiscoveryResult,
    discover_counterparty_candidates,
)
from network_mule_discovery.eid_discovery import (
    EidDiscoveryResult,
    discover_entities_by_seed_eids,
)
from network_mule_discovery.graph_builder import (
    GraphBuildResult,
    build_eid_graph,
)
from network_mule_discovery.in_memory_data_source import (
    InMemoryCounterpartyNetworkDataSource,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
    build_unified_seed_groups,
)
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


@dataclass(frozen=True)
class DailyInitialDiscoveryResult:
    """Deterministic outputs produced before AI execution."""

    source_preflight: DailySourcePreflightResult
    eid_discovery: EidDiscoveryResult
    eid_graph: GraphBuildResult
    counterparty_discovery: CounterpartyDiscoveryResult
    unified_groups: UnifiedGroupResult


def run_initial_discovery(
    *,
    source_preflight: DailySourcePreflightResult,
    assess_eid_linked_customers: bool = True,
) -> DailyInitialDiscoveryResult:
    """Run deterministic initial discovery from validated sources."""
    if not isinstance(
        source_preflight,
        DailySourcePreflightResult,
    ):
        raise SourceContractError(
            "source_preflight must be a "
            "DailySourcePreflightResult."
        )

    canonical_inputs = (
        build_canonical_discovery_inputs_from_bundle(
            source_preflight.source_bundle
        )
    )

    data_source = (
        InMemoryCounterpartyNetworkDataSource(
            canonical_inputs
        )
    )

    run_date = (
        source_preflight
        .source_bundle
        .metadata
        .run_date
    )

    eid_discovery = discover_entities_by_seed_eids(
        data_source=data_source,
        run_date=run_date,
    )

    eid_graph = build_eid_graph(
        discovery_result=eid_discovery,
        run_date=run_date,
    )

    counterparty_discovery = (
        discover_counterparty_candidates(
            data_source=data_source,
            run_date=run_date,
        )
    )

    unified_groups = build_unified_seed_groups(
        eid_discovery=eid_discovery,
        counterparty_discovery=counterparty_discovery,
        run_date=run_date,
        assess_eid_linked_customers=(
            assess_eid_linked_customers
        ),
    )

    return DailyInitialDiscoveryResult(
        source_preflight=source_preflight,
        eid_discovery=eid_discovery,
        eid_graph=eid_graph,
        counterparty_discovery=(
            counterparty_discovery
        ),
        unified_groups=unified_groups,
    )

@dataclass(frozen=True)
class DailyCounterpartyAiPhaseResult:
    """Outputs from the counterparty-first AI phase."""

    initial_discovery: DailyInitialDiscoveryResult
    counterparty_payloads: pd.DataFrame
    counterparty_frontier: CounterpartyFrontierRunResult


def run_counterparty_ai_phase(
    *,
    initial_discovery: DailyInitialDiscoveryResult,
    state_directory: Path | str,
    settings: DailyAiSettings,
    reset_state: bool = False,
    adapter_factory=None,
) -> DailyCounterpartyAiPhaseResult:
    """Build evidence and execute the counterparty AI frontier."""
    if not isinstance(
        initial_discovery,
        DailyInitialDiscoveryResult,
    ):
        raise SourceContractError(
            "initial_discovery must be a "
            "DailyInitialDiscoveryResult."
        )

    if not isinstance(
        settings,
        DailyAiSettings,
    ):
        raise SourceContractError(
            "settings must be DailyAiSettings."
        )

    seed_counterparties = (
        initial_discovery
        .counterparty_discovery
        .seed_counterparties
    )

    if "counterparty_key" not in seed_counterparties.columns:
        raise SourceContractError(
            "seed_counterparties is missing "
            "counterparty_key."
        )

    counterparty_keys = sorted({
        str(value).strip()
        for value in seed_counterparties[
            "counterparty_key"
        ]
        if not pd.isna(value)
        and str(value).strip()
    })

    if counterparty_keys:
        counterparty_payloads = (
            build_bundle_counterparty_payloads(
                source_bundle=(
                    initial_discovery
                    .source_preflight
                    .source_bundle
                ),
                counterparty_keys=counterparty_keys,
            )
        )
    else:
        counterparty_payloads = pd.DataFrame(
            columns=[
                "subject_type",
                "subject_key",
                "feature_payload_json",
            ]
        )

    run_kwargs = {
        "unified_result": (
            initial_discovery.unified_groups
        ),
        "supplemental_subject_payloads": (
            counterparty_payloads
        ),
        "state_directory": state_directory,
        "run_date": (
            initial_discovery
            .source_preflight
            .source_bundle
            .metadata
            .run_date
        ),
        "settings": settings,
        "reset_state": reset_state,
    }

    if adapter_factory is not None:
        run_kwargs["adapter_factory"] = (
            adapter_factory
        )

    counterparty_frontier = (
        run_counterparty_ai_frontier(
            **run_kwargs
        )
    )

    return DailyCounterpartyAiPhaseResult(
        initial_discovery=initial_discovery,
        counterparty_payloads=(
            counterparty_payloads
        ),
        counterparty_frontier=(
            counterparty_frontier
        ),
    )
