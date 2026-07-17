"""Run the bounded recursive expansion demo."""

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
from network_mule_discovery.recursive_expansion import (
    DeterministicDemoDecisionAdapter,
    PreparedExpansionEvidenceSource,
    RecursiveGuardrails,
    run_recursive_expansion,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


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
    """Run and persist recursive expansion."""
    initial_decisions = pd.read_csv(
        PROJECT_ROOT
        / "data/demo/ai_decisions.csv",
        dtype="string",
        keep_default_na=False,
    )

    result = run_recursive_expansion(
        data_source=build_data_source(),
        initial_decisions=initial_decisions,
        evidence_source=(
            PreparedExpansionEvidenceSource(
                PROJECT_ROOT
                / "data/demo/"
                "recursive_relationship_candidates.csv"
            )
        ),
        decision_adapter=(
            DeterministicDemoDecisionAdapter()
        ),
        run_date=RUN_DATE,
        output_directory=OUTPUT_DIRECTORY,
        guardrails=RecursiveGuardrails(),
        persist_outputs=True,
    )

    print("Recursive expansion completed.")
    print(
        f"Termination: "
        f"{result.termination_reason}"
    )
    print(f"Groups: {len(result.groups)}")
    print(f"Nodes: {len(result.nodes)}")
    print(f"Edges: {len(result.edges)}")
    print(
        "Generated AI decisions: "
        f"{len(result.generated_decisions)}"
    )
    print(
        "Completed customer expansions: "
        f"{len(result.expansion_ledger)}"
    )
    print(
        "Remaining queue items: "
        f"{len(result.remaining_queue)}"
    )
    print(
        "Expansion sources: "
        f"{result.groups[
            'recursive_expansion_source_count'
        ].sum()}"
    )


if __name__ == "__main__":
    main()
