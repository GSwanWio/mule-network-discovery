"""Persistence for Section 2 counterparty candidate outputs."""

from __future__ import annotations

from pathlib import Path

from network_mule_discovery.counterparty_discovery import (
    CounterpartyDiscoveryResult,
)


COUNTERPARTY_OUTPUT_FILENAMES = {
    "seed_cutoffs": "counterparty_seed_cutoffs.csv",
    "seed_transfer_events": "seed_counterparty_events.csv",
    "seed_counterparties": "seed_counterparties.csv",
    "candidate_customer_links": "counterparty_candidate_links.csv",
    "candidate_counterparties": "counterparty_candidates.csv",
    "beneficiary_seed_links": "beneficiary_seed_links.csv",
}


def save_counterparty_discovery_outputs(
    discovery_result: CounterpartyDiscoveryResult,
    output_directory: Path | str,
) -> None:
    """Persist all Section 2 discovery tables as CSV files."""
    resolved_output_directory = Path(output_directory)

    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    for attribute_name, filename in (
        COUNTERPARTY_OUTPUT_FILENAMES.items()
    ):
        frame = getattr(
            discovery_result,
            attribute_name,
        )

        frame.to_csv(
            resolved_output_directory / filename,
            index=False,
            lineterminator="\n",
        )
