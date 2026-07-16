"""Run and persist the unified seed-led group projection."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from network_mule_discovery.counterparty_data_sources import (
    CounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    discover_counterparty_candidates,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.unified_group_builder import (
    UnifiedGroupResult,
    build_unified_seed_groups,
)


def run_unified_group_projection(
    data_source: CounterpartyNetworkDataSource,
    run_date: date | str,
    output_directory: Path | str,
    persist_outputs: bool = True,
) -> UnifiedGroupResult:
    """Run EID and counterparty discovery into one group projection."""
    eid_discovery = discover_entities_by_seed_eids(
        data_source=data_source,
        run_date=run_date,
    )

    counterparty_discovery = (
        discover_counterparty_candidates(
            data_source=data_source,
            run_date=run_date,
        )
    )

    unified_result = build_unified_seed_groups(
        eid_discovery=eid_discovery,
        counterparty_discovery=counterparty_discovery,
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

        unified_result.groups.to_csv(
            resolved_output_directory
            / "unified_groups.csv",
            index=False,
            lineterminator="\n",
        )

        unified_result.nodes.to_csv(
            resolved_output_directory
            / "unified_group_nodes.csv",
            index=False,
            lineterminator="\n",
        )

        unified_result.edges.to_csv(
            resolved_output_directory
            / "unified_group_edges.csv",
            index=False,
            lineterminator="\n",
        )

    return unified_result
