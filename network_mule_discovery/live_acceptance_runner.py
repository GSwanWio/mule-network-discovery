"""Production-path execution for one live synthetic acceptance case."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from network_mule_discovery.consolidated_state import (
    ConsolidatedStateSnapshot,
    ConsolidatedStateStore,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_orchestrator import (
    DailyBreadthFirstRunResult,
    DailyBreadthFirstSettings,
    run_breadth_first_frontier,
    run_counterparty_ai_phase,
    run_customer_ai_phase,
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.live_acceptance_matrix import (
    CONTINUE_DECISIONS,
    STOP_DECISIONS,
    LiveAcceptanceCase,
    get_live_acceptance_case,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)
from network_mule_discovery.synthetic_scenario_registry import (
    create_synthetic_source_provider,
)


AdapterFactory = Callable[[], object]


class LiveAcceptanceRunError(RuntimeError):
    """A synthetic acceptance run violated its contract."""


@dataclass(frozen=True)
class LiveAcceptanceRunResult:
    """Persisted result from one production-path scenario run."""

    case: LiveAcceptanceCase
    source_directory: Path
    state_directory: Path
    run_id: str
    run_date: str
    provider_load_count: int
    calls_before_run: int
    calls_after_run: int
    calls_executed: int
    breadth_first_result: DailyBreadthFirstRunResult
    snapshot: ConsolidatedStateSnapshot


def validate_live_acceptance_snapshot(
    *,
    case: LiveAcceptanceCase,
    snapshot: ConsolidatedStateSnapshot,
    calls_executed: int,
) -> None:
    """Enforce the persisted scenario expectations."""
    manifest = snapshot.manifest
    daily_state = snapshot.daily_state

    if manifest.run_status != "TERMINATED":
        raise LiveAcceptanceRunError(
            "Acceptance run did not terminate: "
            f"{manifest.run_status}"
        )

    if (
        manifest.termination_reason
        != case.expected_termination_reason
    ):
        raise LiveAcceptanceRunError(
            "Unexpected termination reason: "
            f"{manifest.termination_reason}"
        )

    observed_counts = {
        "groups": len(
            daily_state.network.groups
        ),
        "nodes": len(
            daily_state.network.nodes
        ),
        "edges": len(
            daily_state.network.edges
        ),
    }
    expected_counts = {
        "groups": case.expected_group_count,
        "nodes": case.expected_raw_node_count,
        "edges": case.expected_raw_edge_count,
    }

    if observed_counts != expected_counts:
        raise LiveAcceptanceRunError(
            "Persisted graph counts do not match "
            f"the matrix: observed={observed_counts}, "
            f"expected={expected_counts}"
        )

    observed_decision_values = tuple(
        str(value).strip().upper()
        for value in daily_state.decision_store[
            "decision"
        ]
        if str(value).strip()
    )
    observed_decisions = set(
        observed_decision_values
    )

    missing_decisions = sorted(
        set(case.required_decisions)
        - observed_decisions
    )

    if missing_decisions:
        raise LiveAcceptanceRunError(
            "Required AI decisions are missing: "
            f"{missing_decisions}"
        )

    missing_decision_groups = [
        accepted_group
        for accepted_group
        in case.required_any_decision_groups
        if not observed_decisions.intersection(
            accepted_group
        )
    ]

    if missing_decision_groups:
        raise LiveAcceptanceRunError(
            "No accepted decision was produced for "
            "required outcome groups: "
            f"{missing_decision_groups}"
        )

    known_decisions = (
        CONTINUE_DECISIONS
        | STOP_DECISIONS
    )
    unknown_decisions = sorted(
        observed_decisions - known_decisions
    )

    if unknown_decisions:
        raise LiveAcceptanceRunError(
            "Unknown persisted AI decisions: "
            f"{unknown_decisions}"
        )

    if (
        len(observed_decision_values)
        != case.expected_decision_count
    ):
        raise LiveAcceptanceRunError(
            "Unexpected persisted decision count: "
            f"{len(observed_decision_values)} != "
            f"{case.expected_decision_count}"
        )

    continue_count = sum(
        decision in CONTINUE_DECISIONS
        for decision in observed_decision_values
    )
    stop_count = sum(
        decision in STOP_DECISIONS
        for decision in observed_decision_values
    )

    if (
        continue_count
        < case.minimum_continue_decision_count
    ):
        raise LiveAcceptanceRunError(
            "Too few AI expansion decisions: "
            f"{continue_count} < "
            f"{case.minimum_continue_decision_count}"
        )

    if (
        stop_count
        < case.minimum_stop_decision_count
    ):
        raise LiveAcceptanceRunError(
            "Too few AI stopping decisions: "
            f"{stop_count} < "
            f"{case.minimum_stop_decision_count}"
        )

    if calls_executed > case.max_live_calls:
        raise LiveAcceptanceRunError(
            "Scenario exceeded its live-call cap: "
            f"{calls_executed} > "
            f"{case.max_live_calls}"
        )

    if not daily_state.frontier_queue.empty:
        raise LiveAcceptanceRunError(
            "Acceptance run terminated with queued "
            "frontier work."
        )


def run_live_acceptance_case(
    *,
    scenario_id: str,
    workspace_directory: Path | str,
    execute_live_ai: bool,
    daily_call_limit: int,
    reset_state: bool,
    changed_evidence: bool = False,
    state_namespace: str | None = None,
    max_frontier_steps: int = 20,
    counterparty_adapter_factory: (
        AdapterFactory | None
    ) = None,
    customer_adapter_factory: (
        AdapterFactory | None
    ) = None,
) -> LiveAcceptanceRunResult:
    """Execute one scenario through the complete production path."""
    case = get_live_acceptance_case(
        scenario_id
    )

    if (
        changed_evidence
        and not case.changed_evidence_supported
    ):
        raise LiveAcceptanceRunError(
            "Changed evidence is not supported for "
            f"{case.scenario_id}."
        )

    if daily_call_limit < 0:
        raise LiveAcceptanceRunError(
            "daily_call_limit cannot be negative."
        )

    if (
        execute_live_ai
        and case.max_live_calls == 0
    ):
        raise LiveAcceptanceRunError(
            f"{case.scenario_id} must execute with "
            "live AI disabled because it has no AI work."
        )

    if (
        not execute_live_ai
        and case.max_live_calls > 0
    ):
        raise LiveAcceptanceRunError(
            f"{case.scenario_id} requires AI execution."
        )

    if (
        execute_live_ai
        and daily_call_limit
        < case.max_live_calls
    ):
        raise LiveAcceptanceRunError(
            "The daily call limit is below the "
            f"scenario cap: {daily_call_limit} < "
            f"{case.max_live_calls}"
        )

    workspace = (
        Path(workspace_directory)
        / case.scenario_id
    )
    source_directory = workspace / "source"
    state_directory = workspace / "state"

    workspace.mkdir(
        parents=True,
        exist_ok=True,
    )

    if reset_state:
        shutil.rmtree(
            state_directory,
            ignore_errors=True,
        )

    provider = create_synthetic_source_provider(
        scenario_id=case.scenario_id,
        output_directory=source_directory,
        changed_evidence=changed_evidence,
    )
    source_manifest = provider.source_manifest
    run_date = str(
        source_manifest["run_date"]
    )

    request = SourceLoadRequest.create(
        dataset_id=case.scenario_id,
        run_date=run_date,
        state_namespace=(
            state_namespace
            or (
                "step8-live-acceptance-"
                f"{case.scenario_id}"
            )
        ),
    )

    preflight = run_source_preflight(
        source_provider=provider,
        source_request=request,
    )
    initial_discovery = run_initial_discovery(
        source_preflight=preflight,
    )

    settings = DailyAiSettings(
        live_ai_enabled=execute_live_ai,
        daily_call_limit=daily_call_limit,
        run_call_limit=max(
            case.max_live_calls,
            1,
        ),
    )

    state_store = ConsolidatedStateStore(
        state_directory
    )
    calls_before_run = (
        state_store.ai_calls.count_calls(
            run_date
        )
    )

    counterparty_phase = (
        run_counterparty_ai_phase(
            initial_discovery=initial_discovery,
            state_directory=state_directory,
            settings=settings,
            reset_state=False,
            adapter_factory=(
                counterparty_adapter_factory
            ),
        )
    )

    customer_phase = run_customer_ai_phase(
        counterparty_phase=counterparty_phase,
        state_directory=state_directory,
        settings=settings,
        adapter_factory=(
            customer_adapter_factory
        ),
    )

    breadth_first_result = (
        run_breadth_first_frontier(
            customer_phase=customer_phase,
            state_directory=state_directory,
            ai_settings=settings,
            breadth_first_settings=(
                DailyBreadthFirstSettings(
                    max_frontier_steps=(
                        max_frontier_steps
                    )
                )
            ),
            counterparty_adapter_factory=(
                counterparty_adapter_factory
            ),
            customer_adapter_factory=(
                customer_adapter_factory
            ),
        )
    )

    calls_after_run = (
        state_store.ai_calls.count_calls(
            run_date
        )
    )
    calls_executed = (
        calls_after_run
        - calls_before_run
    )

    current_snapshot = state_store.load()
    run_id = current_snapshot.manifest.run_id
    historical_snapshot = state_store.load(
        run_id=run_id
    )

    if provider.load_count != 1:
        raise LiveAcceptanceRunError(
            "Source provider was loaded more than once: "
            f"{provider.load_count}"
        )

    if (
        historical_snapshot.manifest
        != current_snapshot.manifest
    ):
        raise LiveAcceptanceRunError(
            "Historical run manifest does not match "
            "the finalized current manifest."
        )

    validate_live_acceptance_snapshot(
        case=case,
        snapshot=historical_snapshot,
        calls_executed=calls_executed,
    )

    return LiveAcceptanceRunResult(
        case=case,
        source_directory=source_directory,
        state_directory=state_directory,
        run_id=run_id,
        run_date=run_date,
        provider_load_count=provider.load_count,
        calls_before_run=calls_before_run,
        calls_after_run=calls_after_run,
        calls_executed=calls_executed,
        breadth_first_result=(
            breadth_first_result
        ),
        snapshot=historical_snapshot,
    )
