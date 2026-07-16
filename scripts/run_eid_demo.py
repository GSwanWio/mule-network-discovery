"""Run the Section 1 EID-only discovery demo."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.daily_runner import run_eid_discovery
from network_mule_discovery.data_sources import CsvNetworkDataSource


RUN_DATE = "2026-07-16"


def main() -> None:
    """Run and persist the EID-only demo."""
    data_source = CsvNetworkDataSource(
        seed_mule_pool_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_pool.csv"
        ),
        customer_identity_path=(
            PROJECT_ROOT
            / "data/demo/customer_identity.csv"
        ),
        output_directory=(
            PROJECT_ROOT
            / "data/demo/output"
        ),
    )

    result = run_eid_discovery(
        data_source=data_source,
        run_date=RUN_DATE,
        persist_outputs=True,
    )

    discovery = result.discovery
    graph = result.graph

    print("EID discovery run completed.")
    print(f"Run date: {RUN_DATE}")
    print(
        "Distinct seeds: "
        f"{len(discovery.seed_resolution.seeds)}"
    )
    print(
        "Resolved seed entities: "
        f"{discovery.seed_resolution.seed_entities['entity_key'].nunique()}"
    )
    print(
        "Unresolved seeds: "
        f"{len(discovery.seed_resolution.unresolved_seeds)}"
    )
    print(
        "Discovered entities: "
        f"{discovery.discovered_entities['candidate_entity_key'].nunique()}"
    )
    print(f"Groups written: {len(graph.groups)}")
    print(f"Nodes written: {len(graph.nodes)}")
    print(f"Edges written: {len(graph.edges)}")


if __name__ == "__main__":
    main()
