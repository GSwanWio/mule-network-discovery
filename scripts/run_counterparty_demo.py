"""Run the Section 2 counterparty candidate discovery demo."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_runner import (
    run_counterparty_candidate_discovery,
)


RUN_DATE = "2026-07-16"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the demo counterparty data source."""
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
        output_directory=(
            PROJECT_ROOT
            / "data/demo/output"
        ),
    )


def main() -> None:
    """Run and persist the Section 2 demo."""
    result = run_counterparty_candidate_discovery(
        data_source=build_data_source(),
        run_date=RUN_DATE,
        output_directory=(
            PROJECT_ROOT
            / "data/demo/output"
        ),
        persist_outputs=True,
    )

    print(
        "Counterparty candidate discovery completed."
    )
    print(f"Run date: {RUN_DATE}")
    print(
        "Eligible seed transfer events: "
        f"{len(result.seed_transfer_events)}"
    )
    print(
        "Seed counterparties: "
        f"{len(result.seed_counterparties)}"
    )
    print(
        "Shared-counterparty links: "
        f"{len(result.candidate_customer_links)}"
    )
    print(
        "Counterparty candidates: "
        f"{len(result.candidate_counterparties)}"
    )
    print(
        "Beneficiary-to-seed links: "
        f"{len(result.beneficiary_seed_links)}"
    )
    print(
        "Expansion allowed: "
        f"{result.candidate_customer_links['expansion_allowed_flag'].any()}"
    )


if __name__ == "__main__":
    main()
