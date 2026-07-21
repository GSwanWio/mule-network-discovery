"""Execute one breadth-first AI frontier with persisted decisions."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import pandas as pd

from network_mule_discovery.daily_ai_runner import (
    ControlledDailyAiRunResult,
    CsvAiCallLedger,
    DailyAiSettings,
    run_controlled_daily_ai,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
)
from network_mule_discovery.incremental_processor import (
    IncrementalDecisionAdapter,
)
from network_mule_discovery.schemas import (
    parse_run_date,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
)


COUNTERPARTY_AI_ACTION_TYPES = frozenset({
    "RUN_COUNTERPARTY_AI",
})


@dataclass(frozen=True)
class CounterpartyFrontierRunResult:
    """Persisted outputs from one counterparty-only frontier."""

    controlled_run: ControlledDailyAiRunResult
    decision_store: pd.DataFrame
    ai_call_ledger: pd.DataFrame


def load_unified_result(
    directory: Path | str,
) -> UnifiedGroupResult:
    """Load one previously materialized observed graph."""
    resolved_directory = Path(directory)

    filenames = {
        "groups": "unified_groups.csv",
        "nodes": "unified_nodes.csv",
        "edges": "unified_edges.csv",
    }

    frames: dict[str, pd.DataFrame] = {}

    for name, filename in filenames.items():
        path = resolved_directory / filename

        if not path.is_file():
            raise FileNotFoundError(
                "Observed graph file is missing: "
                f"{path}"
            )

        frames[name] = pd.read_csv(
            path,
            keep_default_na=False,
        )

    return UnifiedGroupResult(
        groups=frames["groups"],
        nodes=frames["nodes"],
        edges=frames["edges"],
    )


def load_supplemental_subject_payloads(
    path: Path | str,
) -> pd.DataFrame:
    """Load neutral evidence payloads for observed subjects."""
    resolved_path = Path(path)

    if not resolved_path.is_file():
        raise FileNotFoundError(
            "Supplemental subject payload file is missing: "
            f"{resolved_path}"
        )

    frame = pd.read_csv(
        resolved_path,
        dtype="string",
        keep_default_na=False,
    )

    required_columns = {
        "subject_type",
        "subject_key",
        "feature_payload_json",
    }

    missing_columns = sorted(
        required_columns
        - set(frame.columns)
    )

    if missing_columns:
        raise ValueError(
            "Supplemental subject payload file is missing columns: "
            f"{missing_columns}"
        )

    return frame[
        [
            "subject_type",
            "subject_key",
            "feature_payload_json",
        ]
    ].copy()


def run_counterparty_ai_frontier(
    *,
    unified_result: UnifiedGroupResult,
    supplemental_subject_payloads: pd.DataFrame,
    state_directory: Path | str,
    run_date: date | str,
    settings: DailyAiSettings,
    reset_state: bool = False,
    adapter_factory=None,
) -> CounterpartyFrontierRunResult:
    """Run only counterparty AI actions from the current frontier."""
    resolved_run_date = parse_run_date(run_date)
    resolved_state_directory = Path(
        state_directory
    )

    if reset_state and resolved_state_directory.exists():
        shutil.rmtree(
            resolved_state_directory
        )

    state_store = CsvDailyStateStore(
        resolved_state_directory
    )

    # The observed graph is rebuilt from source evidence each run. The
    # decision store, failed queue state, and API ledger remain persisted.
    state_store.save_network_state(
        network=unified_result,
        run_date=resolved_run_date,
    )

    frontier_subject_count = int(
        supplemental_subject_payloads.loc[
            supplemental_subject_payloads[
                "subject_type"
            ]
            .astype("string")
            .str.upper()
            .eq("COUNTERPARTY"),
            "subject_key",
        ].nunique()
    )

    bounded_settings = replace(
        settings,
        run_call_limit=min(
            settings.run_call_limit,
            frontier_subject_count,
        ),
    )

    run_kwargs = {
        "state_directory": (
            resolved_state_directory
        ),
        "run_date": resolved_run_date,
        "settings": bounded_settings,
        "allowed_action_types": (
            COUNTERPARTY_AI_ACTION_TYPES
        ),
        "supplemental_subject_payloads": (
            supplemental_subject_payloads
        ),
    }

    if adapter_factory is not None:
        run_kwargs["adapter_factory"] = (
            adapter_factory
        )

    controlled_run = run_controlled_daily_ai(
        **run_kwargs
    )

    return CounterpartyFrontierRunResult(
        controlled_run=controlled_run,
        decision_store=(
            state_store.load_decision_store()
        ),
        ai_call_ledger=CsvAiCallLedger(
            resolved_state_directory
        ).load(),
    )


def write_counterparty_frontier_outputs(
    *,
    result: CounterpartyFrontierRunResult,
    output_directory: Path | str,
) -> None:
    """Write reviewable projection and AI audit outputs."""
    resolved_output_directory = Path(
        output_directory
    )
    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    projection = result.controlled_run.final_plan.projection

    outputs = {
        "decision_groups.csv": projection.groups,
        "decision_group_nodes.csv": projection.nodes,
        "decision_group_edges.csv": projection.edges,
        "decision_subject_snapshots.csv": (
            projection.subject_snapshots
        ),
        "frontier_queue.csv": (
            result.controlled_run.final_plan.frontier_queue
        ),
        "decision_store.csv": result.decision_store,
        "ai_call_ledger.csv": result.ai_call_ledger,
    }

    for filename, frame in outputs.items():
        frame.to_csv(
            resolved_output_directory / filename,
            index=False,
        )
