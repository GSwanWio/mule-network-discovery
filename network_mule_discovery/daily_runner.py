"""Daily orchestration for seed-led network discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from network_mule_discovery.data_sources import NetworkDataSource
from network_mule_discovery.eid_discovery import (
    EidDiscoveryResult,
    discover_entities_by_seed_eids,
)
from network_mule_discovery.graph_builder import (
    GraphBuildResult,
    build_eid_graph,
)


@dataclass(frozen=True)
class EidRunResult:
    """Complete result from one EID discovery run."""

    discovery: EidDiscoveryResult
    graph: GraphBuildResult


def run_eid_discovery(
    data_source: NetworkDataSource,
    run_date: date | str,
    persist_outputs: bool = True,
) -> EidRunResult:
    """Run deterministic EID discovery and optionally persist outputs."""
    discovery_result = discover_entities_by_seed_eids(
        data_source=data_source,
        run_date=run_date,
    )

    graph_result = build_eid_graph(
        discovery_result=discovery_result,
        run_date=run_date,
    )

    if persist_outputs:
        data_source.save_discovered_groups(
            groups=graph_result.groups,
            nodes=graph_result.nodes,
            edges=graph_result.edges,
        )

    return EidRunResult(
        discovery=discovery_result,
        graph=graph_result,
    )
