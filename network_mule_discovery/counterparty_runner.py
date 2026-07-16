"""Orchestration for Section 2 counterparty candidate discovery."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from network_mule_discovery.counterparty_data_sources import (
    CounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    CounterpartyDiscoveryResult,
    discover_counterparty_candidates,
)
from network_mule_discovery.counterparty_outputs import (
    save_counterparty_discovery_outputs,
)


def run_counterparty_candidate_discovery(
    data_source: CounterpartyNetworkDataSource,
    run_date: date | str,
    output_directory: Path | str,
    persist_outputs: bool = True,
) -> CounterpartyDiscoveryResult:
    """Run Section 2 discovery and optionally persist its outputs."""
    discovery_result = discover_counterparty_candidates(
        data_source=data_source,
        run_date=run_date,
    )

    if persist_outputs:
        save_counterparty_discovery_outputs(
            discovery_result=discovery_result,
            output_directory=output_directory,
        )

    return discovery_result
