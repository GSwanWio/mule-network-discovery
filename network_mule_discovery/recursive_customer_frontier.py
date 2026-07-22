"""Run a bounded customer-AI phase after recursive counterparty approval."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from network_mule_discovery.customer_behavioral_features import (
    CustomerBehavioralFeatureResult,
    build_customer_behavioral_features,
)
from network_mule_discovery.daily_ai_runner import (
    CsvAiCallLedger,
    DailyAiSettings,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_ai import (
    CustomerFrontierRunResult,
    run_customer_ai_frontier,
)
from network_mule_discovery.recursive_counterparty_frontier import (
    build_guardrail_telemetry,
)
from network_mule_discovery.schemas import parse_run_date
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


class RecursiveCustomerFrontierError(RuntimeError):
    """The recursive customer phase could not start safely."""


@dataclass(frozen=True)
class RecursiveCustomerFrontierResult:
    """One breadth-first recursive customer assessment phase."""

    customer_keys: tuple[str, ...]
    new_features: CustomerBehavioralFeatureResult
    customer_frontier: CustomerFrontierRunResult
    decision_store: pd.DataFrame
    ai_call_ledger: pd.DataFrame
    guardrail_telemetry: pd.DataFrame


def _normalize_customer_keys(
    values: Iterable[str],
) -> tuple[str, ...]:
    return tuple(
        sorted({
            str(value).strip()
            for value in values
            if str(value).strip()
        })
    )


def _resolve_customer_keys(
    *,
    actionable_queue: pd.DataFrame,
    customer_keys: Iterable[str] | None,
) -> tuple[str, ...]:
    queued_keys = _normalize_customer_keys(
        actionable_queue.loc[
            actionable_queue["action_type"].eq(
                "RUN_CUSTOMER_AI"
            ),
            "subject_key",
        ]
    )

    if customer_keys is None:
        if not queued_keys:
            raise RecursiveCustomerFrontierError(
                "No recursive customer AI subjects are ready and no "
                "persisted recursive customer feature keys were supplied."
            )

        return queued_keys

    supplied_keys = _normalize_customer_keys(customer_keys)

    if not supplied_keys:
        raise RecursiveCustomerFrontierError(
            "Persisted recursive customer feature keys cannot be empty."
        )

    unexpected_queued_keys = sorted(
        set(queued_keys) - set(supplied_keys)
    )

    if unexpected_queued_keys:
        raise RecursiveCustomerFrontierError(
            "Ready recursive customers are missing from the supplied "
            "feature-key set: "
            f"{unexpected_queued_keys}"
        )

    return supplied_keys


def run_recursive_customer_frontier(
    *,
    source_directory: Path | str,
    state_directory: Path | str,
    run_date: date | str,
    supplemental_subject_payloads: pd.DataFrame,
    settings: DailyAiSettings,
    customer_keys: Iterable[str] | None = None,
    adapter_factory=None,
) -> RecursiveCustomerFrontierResult:
    """Assess one recursive customer frontier and stop before discovery."""
    resolved_run_date = parse_run_date(run_date)
    state_store = CsvDailyStateStore(state_directory)

    try:
        snapshot = state_store.load_snapshot()
    except FileNotFoundError as exc:
        raise RecursiveCustomerFrontierError(
            "Persisted recursive counterparty state is unavailable. "
            "Complete the prior recursive counterparty frontier first."
        ) from exc

    preflight = build_incremental_daily_plan(
        state_store=state_store,
        run_date=resolved_run_date,
        supplemental_subject_payloads=(
            supplemental_subject_payloads
        ),
    )
    unresolved_counterparties = (
        preflight.actionable_queue.loc[
            preflight.actionable_queue["action_type"].eq(
                "RUN_COUNTERPARTY_AI"
            )
        ]
    )

    if not unresolved_counterparties.empty:
        subjects = sorted(
            unresolved_counterparties[
                "subject_key"
            ].astype("string")
        )
        raise RecursiveCustomerFrontierError(
            "Recursive customer AI cannot start while counterparty "
            "decisions remain unresolved: "
            f"{subjects}"
        )

    resolved_customer_keys = _resolve_customer_keys(
        actionable_queue=preflight.actionable_queue,
        customer_keys=customer_keys,
    )
    new_features = build_customer_behavioral_features(
        source_directory=source_directory,
        customer_keys=resolved_customer_keys,
        projection=preflight.projection,
        run_date=resolved_run_date,
    )
    combined_payloads = pd.concat(
        [
            supplemental_subject_payloads,
            new_features.customer_payloads,
        ],
        ignore_index=True,
    )
    combined_payloads = (
        combined_payloads
        .drop_duplicates(
            subset=["subject_type", "subject_key"],
            keep="last",
        )
        .sort_values(
            by=["subject_type", "subject_key"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    frontier_kwargs = {
        "unified_result": snapshot.network,
        "supplemental_subject_payloads": combined_payloads,
        "state_directory": state_directory,
        "run_date": resolved_run_date,
        "settings": settings,
    }

    if adapter_factory is not None:
        frontier_kwargs["adapter_factory"] = adapter_factory

    customer_frontier = run_customer_ai_frontier(
        **frontier_kwargs
    )
    final_plan = customer_frontier.controlled_run.final_plan
    telemetry_network = UnifiedGroupResult(
        groups=final_plan.projection.groups,
        nodes=final_plan.projection.nodes,
        edges=final_plan.projection.edges,
    )
    telemetry = build_guardrail_telemetry(
        network=telemetry_network,
        frontier_queue=final_plan.frontier_queue,
        run_date=resolved_run_date,
        stage="RECURSIVE_CUSTOMER_FRONTIER",
        new_node_count=0,
        new_edge_count=0,
    )

    return RecursiveCustomerFrontierResult(
        customer_keys=resolved_customer_keys,
        new_features=new_features,
        customer_frontier=customer_frontier,
        decision_store=state_store.load_decision_store(),
        ai_call_ledger=CsvAiCallLedger(
            state_directory
        ).load(),
        guardrail_telemetry=telemetry,
    )
