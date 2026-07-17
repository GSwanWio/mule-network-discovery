"""Run the persisted-decision projection demo."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.decision_runner import (
    run_decision_projection,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"
DECISION_PATH = PROJECT_ROOT / "data/demo/ai_decisions.csv"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the complete demo data source."""
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
    """Run and persist the decision-aware group projection."""
    decisions = pd.read_csv(
        DECISION_PATH,
        dtype="string",
        keep_default_na=False,
    )

    result = run_decision_projection(
        data_source=build_data_source(),
        decisions=decisions,
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        persist_outputs=True,
    )

    print("Decision projection completed.")
    print(f"Groups: {len(result.groups)}")
    print(
        "Applied cached decisions: "
        f"{len(result.applied_decisions)}"
    )
    print(
        "Ignored stale decisions: "
        f"{len(result.ignored_decisions)}"
    )
    print(
        "Queued actions: "
        f"{len(result.expansion_queue)}"
    )
    print(
        "Mule-like customers: "
        f"{result.groups['mule_like_customer_count'].sum()}"
    )
    print(
        "Approved suspicious counterparties: "
        f"{result.groups[
            'approved_suspicious_counterparty_count'
        ].sum()}"
    )


if __name__ == "__main__":
    main()
