"""Run the unified seed-led group projection demo."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.unified_group_runner import (
    run_unified_group_projection,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the full demo data source."""
    return CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_pool.csv"
        ),
        customer_identity_path=(
            PROJECT_ROOT
            / "data/demo/customer_identity.csv"
        ),
        seed_mule_events_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_events.csv"
        ),
        counterparty_events_path=(
            PROJECT_ROOT
            / "data/demo/counterparty_events.csv"
        ),
        output_directory=OUTPUT_DIRECTORY,
    )


def main() -> None:
    """Run and persist unified groups."""
    result = run_unified_group_projection(
        data_source=build_data_source(),
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        persist_outputs=True,
    )

    print("Unified group projection completed.")
    print(f"Groups: {len(result.groups)}")
    print(f"Nodes: {len(result.nodes)}")
    print(f"Edges: {len(result.edges)}")
    print(
        "Customer assessments pending: "
        f"{result.groups['customer_assessment_pending_count'].sum()}"
    )
    print(
        "Counterparty AI decisions pending: "
        f"{result.groups['counterparty_ai_pending_count'].sum()}"
    )
    print(
        "Recursive expansion sources: "
        f"{result.groups['recursive_expansion_source_count'].sum()}"
    )


if __name__ == "__main__":
    main()
