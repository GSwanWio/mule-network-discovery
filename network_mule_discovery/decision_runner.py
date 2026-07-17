"""Run and persist the incremental decision projection."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from network_mule_discovery.counterparty_data_sources import (
    CounterpartyNetworkDataSource,
)
from network_mule_discovery.decision_engine import (
    DecisionProjectionResult,
    apply_persisted_decisions,
)
from network_mule_discovery.unified_group_runner import (
    run_unified_group_projection,
)


def run_decision_projection(
    data_source: CounterpartyNetworkDataSource,
    decisions: pd.DataFrame,
    run_date: date | str,
    output_directory: Path | str,
    persist_outputs: bool = True,
) -> DecisionProjectionResult:
    """Build groups, reuse decisions, and queue unresolved work."""
    unified_result = run_unified_group_projection(
        data_source=data_source,
        run_date=run_date,
        output_directory=output_directory,
        persist_outputs=False,
    )

    decision_result = apply_persisted_decisions(
        unified_result=unified_result,
        decisions=decisions,
        run_date=run_date,
    )

    if persist_outputs:
        resolved_output_directory = Path(
            output_directory
        )

        resolved_output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_frames = {
            "decision_groups.csv": (
                decision_result.groups
            ),
            "decision_group_nodes.csv": (
                decision_result.nodes
            ),
            "decision_group_edges.csv": (
                decision_result.edges
            ),
            "decision_subject_snapshots.csv": (
                decision_result.subject_snapshots
            ),
            "applied_decisions.csv": (
                decision_result.applied_decisions
            ),
            "ignored_decisions.csv": (
                decision_result.ignored_decisions
            ),
            "expansion_queue.csv": (
                decision_result.expansion_queue
            ),
        }

        for filename, frame in output_frames.items():
            frame.to_csv(
                resolved_output_directory
                / filename,
                index=False,
                lineterminator="\n",
            )

    return decision_result
