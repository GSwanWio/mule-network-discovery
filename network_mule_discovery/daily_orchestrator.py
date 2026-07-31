"""Production-path orchestration for daily network discovery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from network_mule_discovery.bundle_behavioral_sources import (
    build_bundle_behavioral_sources,
    build_bundle_counterparty_payloads,
)
from network_mule_discovery.customer_behavioral_features import (
    build_customer_behavioral_features,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_ai import (
    CounterpartyFrontierRunResult,
    CustomerFrontierRunResult,
    run_counterparty_ai_frontier,
    run_customer_ai_frontier,
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

@dataclass(frozen=True)
class DailyCustomerAiPhaseResult:
    """Outputs from the customer-second AI phase."""

    counterparty_phase: DailyCounterpartyAiPhaseResult
    customer_payloads: pd.DataFrame
    supplemental_subject_payloads: pd.DataFrame
    customer_frontier: CustomerFrontierRunResult


def run_customer_ai_phase(
    *,
    counterparty_phase: DailyCounterpartyAiPhaseResult,
    state_directory: Path | str,
    settings: DailyAiSettings,
    adapter_factory=None,
) -> DailyCustomerAiPhaseResult:
    """Execute customer AI after counterparty decisions close."""
    if not isinstance(
        counterparty_phase,
        DailyCounterpartyAiPhaseResult,
    ):
        raise SourceContractError(
            "counterparty_phase must be a "
            "DailyCounterpartyAiPhaseResult."
        )

    if not isinstance(
        settings,
        DailyAiSettings,
    ):
        raise SourceContractError(
            "settings must be DailyAiSettings."
        )

    initial_discovery = (
        counterparty_phase.initial_discovery
    )
    source_bundle = (
        initial_discovery
        .source_preflight
        .source_bundle
    )
    run_date = source_bundle.metadata.run_date
    resolved_state_directory = Path(
        state_directory
    )

    state_store = CsvDailyStateStore(
        resolved_state_directory
    )

    preflight_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=run_date,
        supplemental_subject_payloads=(
            counterparty_phase
            .counterparty_payloads
        ),
    )

    unresolved_counterparties = (
        preflight_plan.actionable_queue.loc[
            preflight_plan.actionable_queue[
                "action_type"
            ].eq("RUN_COUNTERPARTY_AI")
        ]
    )

    if not unresolved_counterparties.empty:
        subjects = sorted(
            unresolved_counterparties[
                "subject_key"
            ].astype("string")
        )

        raise RuntimeError(
            "Customer AI phase cannot start while "
            "counterparty decisions remain unresolved: "
            f"{subjects}"
        )

    customer_keys = sorted({
        str(value).strip()
        for value in preflight_plan.actionable_queue.loc[
            preflight_plan.actionable_queue[
                "action_type"
            ].eq("RUN_CUSTOMER_AI"),
            "subject_key",
        ]
        if not pd.isna(value)
        and str(value).strip()
    })

    behavioral_sources = (
        build_bundle_behavioral_sources(
            source_bundle
        )
    )

    if customer_keys:
        customer_features = (
            build_customer_behavioral_features(
                customer_keys=customer_keys,
                projection=(
                    preflight_plan.projection
                ),
                run_date=run_date,
                raw_sources=(
                    behavioral_sources.raw_sources
                ),
                international_customer_currency_activity=(
                    behavioral_sources
                    .international_customer_currency_activity
                ),
            )
        )

        customer_payloads = (
            customer_features.customer_payloads
        )
    else:
        customer_payloads = pd.DataFrame(
            columns=[
                "subject_type",
                "subject_key",
                "feature_payload_json",
            ]
        )

    supplemental_subject_payloads = pd.concat(
        [
            counterparty_phase
            .counterparty_payloads,
            customer_payloads,
        ],
        ignore_index=True,
    )

    run_kwargs = {
        "unified_result": (
            initial_discovery.unified_groups
        ),
        "supplemental_subject_payloads": (
            supplemental_subject_payloads
        ),
        "state_directory": (
            resolved_state_directory
        ),
        "run_date": run_date,
        "settings": settings,
    }

    if adapter_factory is not None:
        run_kwargs["adapter_factory"] = (
            adapter_factory
        )

    customer_frontier = run_customer_ai_frontier(
        **run_kwargs
    )

    return DailyCustomerAiPhaseResult(
        counterparty_phase=counterparty_phase,
        customer_payloads=customer_payloads,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
        customer_frontier=customer_frontier,
    )

@dataclass(frozen=True)
class DailyFrontierSelection:
    """The next safe persisted-frontier action."""

    action_type: str
    subject_keys: tuple[str, ...]


def select_next_frontier_action(
    *,
    actionable_queue: pd.DataFrame,
    failed_closed_item_count: int,
) -> DailyFrontierSelection:
    """Select exactly one safe breadth-first phase."""
    if failed_closed_item_count < 0:
        raise SourceContractError(
            "failed_closed_item_count cannot be negative."
        )

    if failed_closed_item_count:
        return DailyFrontierSelection(
            action_type="FAIL_CLOSED",
            subject_keys=tuple(),
        )

    required_columns = {
        "action_type",
        "subject_key",
    }

    missing_columns = sorted(
        required_columns - set(actionable_queue.columns)
    )

    if missing_columns:
        raise SourceContractError(
            "actionable_queue is missing columns: "
            f"{missing_columns}"
        )

    if actionable_queue.empty:
        return DailyFrontierSelection(
            action_type="TERMINATE_FRONTIER",
            subject_keys=tuple(),
        )

    prepared = actionable_queue[
        [
            "action_type",
            "subject_key",
        ]
    ].copy()

    prepared["action_type"] = (
        prepared["action_type"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    prepared["subject_key"] = (
        prepared["subject_key"]
        .astype("string")
        .str.strip()
    )

    supported_actions = {
        "RUN_COUNTERPARTY_AI",
        "RUN_CUSTOMER_AI",
        "DISCOVER_CUSTOMER_RELATIONSHIPS",
    }

    observed_actions = set(
        prepared["action_type"]
    )

    unsupported_actions = sorted(
        observed_actions - supported_actions
    )

    if unsupported_actions:
        raise SourceContractError(
            "Unsupported actionable frontier types: "
            f"{unsupported_actions}"
        )

    if len(observed_actions) != 1:
        raise SourceContractError(
            "The actionable frontier contains mixed "
            f"breadth-first phases: {sorted(observed_actions)}"
        )

    action_type = next(iter(observed_actions))
    subject_keys = tuple(
        sorted(
            prepared["subject_key"]
            .drop_duplicates()
            .tolist()
        )
    )

    if not subject_keys or any(
        not subject_key
        for subject_key in subject_keys
    ):
        raise SourceContractError(
            "The actionable frontier contains a blank "
            "subject key."
        )

    if (
        action_type
        == "DISCOVER_CUSTOMER_RELATIONSHIPS"
        and len(subject_keys) != 1
    ):
        raise SourceContractError(
            "Recursive discovery requires exactly one "
            f"approved source; found {len(subject_keys)}."
        )

    return DailyFrontierSelection(
        action_type=action_type,
        subject_keys=subject_keys,
    )
