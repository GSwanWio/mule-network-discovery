"""Production-path orchestration for daily network discovery."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from network_mule_discovery.bundle_behavioral_sources import (
    build_bundle_behavioral_sources,
    build_bundle_counterparty_payloads,
)
from network_mule_discovery.consolidated_state import (
    ConsolidatedStateStore,
)
from network_mule_discovery.customer_behavioral_features import (
    build_customer_behavioral_features,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.production_ai_runtime import (
    ProductionAiStartupError,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    DailyIncrementalPlan,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_ai import (
    CounterpartyFrontierRunResult,
    CustomerFrontierRunResult,
    run_counterparty_ai_frontier,
    run_customer_ai_frontier,
)
from network_mule_discovery.frontier_termination import (
    FrontierTerminationResult,
    run_frontier_exhaustion_termination,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    RecursiveCounterpartyFrontierResult,
    discover_recursive_counterparties,
    run_recursive_counterparty_frontier,
)
from network_mule_discovery.recursive_customer_frontier import (
    RecursiveCustomerFrontierResult,
    run_recursive_customer_frontier,
)
from network_mule_discovery.recursive_termination import (
    RecursiveTerminationResult,
    run_recursive_termination,
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

    resolved_state_directory = Path(
        state_directory
    )

    if (
        reset_state
        and resolved_state_directory.exists()
    ):
        shutil.rmtree(
            resolved_state_directory
        )

    consolidated_store = ConsolidatedStateStore(
        resolved_state_directory
    )
    consolidated_store.initialize(
        initial_discovery
        .source_preflight
        .source_bundle
        .metadata
    )
    consolidated_store.update_run_status(
        run_status="RUNNING"
    )

    graph_nodes = (
        initial_discovery
        .unified_groups
        .nodes
    )

    required_node_columns = {
        "node_type",
        "counterparty_key",
    }
    missing_node_columns = sorted(
        required_node_columns
        - set(graph_nodes.columns)
    )

    if missing_node_columns:
        raise SourceContractError(
            "Unified graph nodes are missing "
            f"columns: {missing_node_columns}"
        )

    counterparty_nodes = graph_nodes.loc[
        graph_nodes["node_type"]
        .astype("string")
        .str.strip()
        .str.upper()
        .eq("COUNTERPARTY")
    ]

    counterparty_keys = sorted({
        str(value).strip()
        for value in counterparty_nodes[
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
        "state_directory": (
            resolved_state_directory
        ),
        "run_date": (
            initial_discovery
            .source_preflight
            .source_bundle
            .metadata
            .run_date
        ),
        "settings": settings,
        "reset_state": False,
    }

    if adapter_factory is not None:
        run_kwargs["adapter_factory"] = (
            adapter_factory
        )

    try:
        counterparty_frontier = (
            run_counterparty_ai_frontier(
                **run_kwargs
            )
        )
    except Exception as exc:
        failure_reason = (
            ProductionAiStartupError.code
            if isinstance(
                exc,
                ProductionAiStartupError,
            )
            else "COUNTERPARTY_AI_PHASE_FAILED"
        )

        try:
            _persist_run_outcome(
                state_directory=(
                    resolved_state_directory
                ),
                termination_status="FAILED",
                termination_reason=failure_reason,
            )
        except Exception as finalization_exc:
            exc.add_note(
                "Run failure finalization also failed: "
                f"{type(finalization_exc).__name__}: "
                f"{finalization_exc}"
            )

        raise

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

@dataclass(frozen=True)
class DailyBreadthFirstSettings:
    """Safety limit for one persisted frontier run."""

    max_frontier_steps: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_frontier_steps, bool)
            or not isinstance(
                self.max_frontier_steps,
                int,
            )
            or self.max_frontier_steps <= 0
        ):
            raise SourceContractError(
                "max_frontier_steps must be a "
                "positive integer."
            )


@dataclass(frozen=True)
class DailyBreadthFirstStepResult:
    """One selected and completed frontier phase."""

    step_number: int
    selection: DailyFrontierSelection
    recursive_counterparty: (
        RecursiveCounterpartyFrontierResult | None
    ) = None
    recursive_customer: (
        RecursiveCustomerFrontierResult | None
    ) = None


@dataclass(frozen=True)
class DailyBreadthFirstRunResult:
    """Final state from one bounded breadth-first run."""

    customer_phase: DailyCustomerAiPhaseResult
    steps: tuple[DailyBreadthFirstStepResult, ...]
    supplemental_subject_payloads: pd.DataFrame
    final_plan: DailyIncrementalPlan
    termination_status: str
    termination_reason: str
    recursive_termination: (
        RecursiveTerminationResult | None
    ) = None
    frontier_termination: (
        FrontierTerminationResult | None
    ) = None

def _merge_supplemental_payloads(
    *frames: pd.DataFrame,
) -> pd.DataFrame:
    """Combine subject evidence with one row per subject."""
    required_columns = {
        "subject_type",
        "subject_key",
        "feature_payload_json",
    }

    prepared_frames: list[pd.DataFrame] = []

    for frame in frames:
        missing_columns = sorted(
            required_columns - set(frame.columns)
        )

        if missing_columns:
            raise SourceContractError(
                "Supplemental payload frame is missing "
                f"columns: {missing_columns}"
            )

        if not frame.empty:
            prepared_frames.append(
                frame[
                    [
                        "subject_type",
                        "subject_key",
                        "feature_payload_json",
                    ]
                ].copy()
            )

    if not prepared_frames:
        return pd.DataFrame(
            columns=[
                "subject_type",
                "subject_key",
                "feature_payload_json",
            ]
        )

    return (
        pd.concat(
            prepared_frames,
            ignore_index=True,
        )
        .drop_duplicates(
            subset=[
                "subject_type",
                "subject_key",
            ],
            keep="last",
        )
        .sort_values(
            by=[
                "subject_type",
                "subject_key",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _termination_values(
    termination_status: pd.DataFrame,
) -> tuple[str, str]:
    """Resolve one consistent persisted termination outcome."""
    required_columns = {
        "termination_status",
        "termination_reason",
    }

    missing_columns = sorted(
        required_columns - set(termination_status.columns)
    )

    if missing_columns:
        raise SourceContractError(
            "Termination status is missing columns: "
            f"{missing_columns}"
        )

    if termination_status.empty:
        raise SourceContractError(
            "Termination status cannot be empty."
        )

    statuses = tuple(
        sorted({
            str(value).strip()
            for value in termination_status[
                "termination_status"
            ]
            if str(value).strip()
        })
    )
    reasons = tuple(
        sorted({
            str(value).strip()
            for value in termination_status[
                "termination_reason"
            ]
            if str(value).strip()
        })
    )

    if len(statuses) != 1 or len(reasons) != 1:
        raise SourceContractError(
            "Termination status must contain one "
            "consistent status and reason."
        )

    return statuses[0], reasons[0]


def _discovery_group_ids(
    actionable_queue: pd.DataFrame,
    source_entity_key: str,
) -> tuple[str, ...]:
    """Resolve all group IDs for one recursive source."""
    required_columns = {
        "action_type",
        "subject_key",
        "group_ids",
    }

    missing_columns = sorted(
        required_columns - set(actionable_queue.columns)
    )

    if missing_columns:
        raise SourceContractError(
            "Recursive discovery queue is missing "
            f"columns: {missing_columns}"
        )

    rows = actionable_queue.loc[
        actionable_queue["action_type"]
        .astype("string")
        .str.strip()
        .str.upper()
        .eq("DISCOVER_CUSTOMER_RELATIONSHIPS")
        & actionable_queue["subject_key"]
        .astype("string")
        .str.strip()
        .eq(source_entity_key)
    ]

    group_ids = tuple(
        sorted({
            group_id
            for value in rows["group_ids"]
            for group_id in str(value).split("|")
            if group_id
        })
    )

    if not group_ids:
        raise SourceContractError(
            "Recursive discovery source has no group IDs."
        )

    return group_ids


def _persist_run_outcome(
    *,
    state_directory: Path | str,
    termination_status: str,
    termination_reason: str,
) -> None:
    """Persist the final status of the current logical run."""
    normalized_status = str(
        termination_status
    ).strip().upper()
    normalized_reason = str(
        termination_reason
    ).strip()

    if normalized_status not in {
        "STOPPED",
        "TERMINATED",
        "FAILED",
    }:
        raise SourceContractError(
            "Unsupported final run status: "
            f"{normalized_status}"
        )

    if not normalized_reason:
        raise SourceContractError(
            "Final termination reason must be nonblank."
        )

    consolidated_store = ConsolidatedStateStore(
        state_directory
    )
    consolidated_store.update_run_status(
        run_status=normalized_status,
        termination_status=normalized_status,
        termination_reason=normalized_reason,
    )
    consolidated_store.snapshot_current_run()


def run_breadth_first_frontier(
    *,
    customer_phase: DailyCustomerAiPhaseResult,
    state_directory: Path | str,
    ai_settings: DailyAiSettings,
    breadth_first_settings: DailyBreadthFirstSettings,
    counterparty_adapter_factory=None,
    customer_adapter_factory=None,
) -> DailyBreadthFirstRunResult:
    """Advance one persisted breadth-first phase per step."""
    if not isinstance(
        customer_phase,
        DailyCustomerAiPhaseResult,
    ):
        raise SourceContractError(
            "customer_phase must be a "
            "DailyCustomerAiPhaseResult."
        )

    if not isinstance(
        ai_settings,
        DailyAiSettings,
    ):
        raise SourceContractError(
            "ai_settings must be DailyAiSettings."
        )

    if not isinstance(
        breadth_first_settings,
        DailyBreadthFirstSettings,
    ):
        raise SourceContractError(
            "breadth_first_settings must be "
            "DailyBreadthFirstSettings."
        )

    initial_discovery = (
        customer_phase
        .counterparty_phase
        .initial_discovery
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
    behavioral_sources = (
        build_bundle_behavioral_sources(
            source_bundle
        )
    )
    supplemental_payloads = (
        _merge_supplemental_payloads(
            customer_phase
            .supplemental_subject_payloads
        )
    )
    steps: list[
        DailyBreadthFirstStepResult
    ] = []

    for step_number in range(
        1,
        (
            breadth_first_settings
            .max_frontier_steps
            + 1
        ),
    ):
        plan = build_incremental_daily_plan(
            state_store=state_store,
            run_date=run_date,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
        )
        selection = select_next_frontier_action(
            actionable_queue=(
                plan.actionable_queue
            ),
            failed_closed_item_count=(
                plan.failed_closed_item_count
            ),
        )

        if selection.action_type == "FAIL_CLOSED":
            steps.append(
                DailyBreadthFirstStepResult(
                    step_number=step_number,
                    selection=selection,
                )
            )

            _persist_run_outcome(
                state_directory=(
                    resolved_state_directory
                ),
                termination_status="STOPPED",
                termination_reason=(
                    "FAILED_CLOSED_FRONTIER"
                ),
            )

            return DailyBreadthFirstRunResult(
                customer_phase=customer_phase,
                steps=tuple(steps),
                supplemental_subject_payloads=(
                    supplemental_payloads
                ),
                final_plan=plan,
                termination_status="STOPPED",
                termination_reason=(
                    "FAILED_CLOSED_FRONTIER"
                ),
            )

        if selection.action_type == "TERMINATE_FRONTIER":
            termination = (
                run_frontier_exhaustion_termination(
                    state_directory=(
                        resolved_state_directory
                    ),
                    run_date=run_date,
                    supplemental_subject_payloads=(
                        supplemental_payloads
                    ),
                )
            )
            status, reason = _termination_values(
                termination.termination_status
            )
            steps.append(
                DailyBreadthFirstStepResult(
                    step_number=step_number,
                    selection=selection,
                )
            )

            _persist_run_outcome(
                state_directory=(
                    resolved_state_directory
                ),
                termination_status=status,
                termination_reason=reason,
            )

            return DailyBreadthFirstRunResult(
                customer_phase=customer_phase,
                steps=tuple(steps),
                supplemental_subject_payloads=(
                    supplemental_payloads
                ),
                final_plan=termination.final_plan,
                termination_status=status,
                termination_reason=reason,
                frontier_termination=termination,
            )

        if (
            selection.action_type
            == "DISCOVER_CUSTOMER_RELATIONSHIPS"
        ):
            source_entity_key = (
                selection.subject_keys[0]
            )
            group_ids = _discovery_group_ids(
                actionable_queue=(
                    plan.actionable_queue
                ),
                source_entity_key=(
                    source_entity_key
                ),
            )
            snapshot = state_store.load_snapshot()
            discovery_preview = (
                discover_recursive_counterparties(
                    observed_network=(
                        snapshot.network
                    ),
                    source_entity_key=(
                        source_entity_key
                    ),
                    group_ids=group_ids,
                    run_date=run_date,
                    raw_sources=(
                        behavioral_sources
                        .raw_sources
                    ),
                )
            )

            if (
                not discovery_preview
                .new_counterparty_keys
                and discovery_preview
                .relationships
                .empty
            ):
                termination = (
                    run_recursive_termination(
                        state_directory=(
                            resolved_state_directory
                        ),
                        run_date=run_date,
                        supplemental_subject_payloads=(
                            supplemental_payloads
                        ),
                        raw_sources=(
                            behavioral_sources
                            .raw_sources
                        ),
                    )
                )
                status, reason = (
                    _termination_values(
                        termination
                        .termination_status
                    )
                )
                steps.append(
                    DailyBreadthFirstStepResult(
                        step_number=step_number,
                        selection=selection,
                    )
                )

                _persist_run_outcome(
                    state_directory=(
                        resolved_state_directory
                    ),
                    termination_status=status,
                    termination_reason=reason,
                )

                return DailyBreadthFirstRunResult(
                    customer_phase=customer_phase,
                    steps=tuple(steps),
                    supplemental_subject_payloads=(
                        supplemental_payloads
                    ),
                    final_plan=(
                        termination.final_plan
                    ),
                    termination_status=status,
                    termination_reason=reason,
                    recursive_termination=(
                        termination
                    ),
                )

        if selection.action_type in {
            "RUN_COUNTERPARTY_AI",
            "DISCOVER_CUSTOMER_RELATIONSHIPS",
        }:
            run_kwargs = {
                "state_directory": (
                    resolved_state_directory
                ),
                "run_date": run_date,
                "supplemental_subject_payloads": (
                    supplemental_payloads
                ),
                "settings": ai_settings,
                "raw_sources": (
                    behavioral_sources
                    .raw_sources
                ),
                "international_counterparty_currency_activity": (
                    behavioral_sources
                    .international_counterparty_currency_activity
                ),
            }

            if counterparty_adapter_factory is not None:
                run_kwargs[
                    "adapter_factory"
                ] = counterparty_adapter_factory

            recursive_counterparty = (
                run_recursive_counterparty_frontier(
                    **run_kwargs
                )
            )
            supplemental_payloads = (
                _merge_supplemental_payloads(
                    supplemental_payloads,
                    recursive_counterparty
                    .new_features
                    .counterparty_payloads,
                )
            )
            steps.append(
                DailyBreadthFirstStepResult(
                    step_number=step_number,
                    selection=selection,
                    recursive_counterparty=(
                        recursive_counterparty
                    ),
                )
            )
            continue

        if selection.action_type == "RUN_CUSTOMER_AI":
            run_kwargs = {
                "state_directory": (
                    resolved_state_directory
                ),
                "run_date": run_date,
                "supplemental_subject_payloads": (
                    supplemental_payloads
                ),
                "settings": ai_settings,
                "raw_sources": (
                    behavioral_sources
                    .raw_sources
                ),
                "international_customer_currency_activity": (
                    behavioral_sources
                    .international_customer_currency_activity
                ),
                "customer_keys": (
                    selection.subject_keys
                ),
            }

            if customer_adapter_factory is not None:
                run_kwargs[
                    "adapter_factory"
                ] = customer_adapter_factory

            recursive_customer = (
                run_recursive_customer_frontier(
                    **run_kwargs
                )
            )
            supplemental_payloads = (
                _merge_supplemental_payloads(
                    supplemental_payloads,
                    recursive_customer
                    .new_features
                    .customer_payloads,
                )
            )
            steps.append(
                DailyBreadthFirstStepResult(
                    step_number=step_number,
                    selection=selection,
                    recursive_customer=(
                        recursive_customer
                    ),
                )
            )
            continue

        raise SourceContractError(
            "The selected frontier phase was not handled: "
            f"{selection.action_type}"
        )

    final_plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=run_date,
        supplemental_subject_payloads=(
            supplemental_payloads
        ),
    )

    _persist_run_outcome(
        state_directory=resolved_state_directory,
        termination_status="STOPPED",
        termination_reason=(
            "MAX_FRONTIER_STEPS_REACHED"
        ),
    )

    return DailyBreadthFirstRunResult(
        customer_phase=customer_phase,
        steps=tuple(steps),
        supplemental_subject_payloads=(
            supplemental_payloads
        ),
        final_plan=final_plan,
        termination_status="STOPPED",
        termination_reason=(
            "MAX_FRONTIER_STEPS_REACHED"
        ),
    )
