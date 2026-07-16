"""Counterparty-capable data-source interfaces."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Protocol

import pandas as pd

from network_mule_discovery.counterparty_schemas import (
    prepare_counterparty_events,
    prepare_seed_mule_events,
)
from network_mule_discovery.data_sources import (
    CsvNetworkDataSource,
    NetworkDataSource,
)
from network_mule_discovery.schemas import parse_run_date


class CounterpartyNetworkDataSource(
    NetworkDataSource,
    Protocol,
):
    """Data-source boundary required by Section 2."""

    def get_seed_mule_events(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        """Return FRC seed events for a run date."""
        ...

    def get_counterparty_events(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        """Return normalized transfer and beneficiary events."""
        ...


class CsvCounterpartyNetworkDataSource(
    CsvNetworkDataSource
):
    """CSV implementation supporting EID and counterparty discovery."""

    def __init__(
        self,
        seed_mule_pool_path: Path | str,
        customer_identity_path: Path | str,
        seed_mule_events_path: Path | str,
        counterparty_events_path: Path | str,
        output_directory: Path | str,
    ) -> None:
        super().__init__(
            seed_mule_pool_path=seed_mule_pool_path,
            customer_identity_path=customer_identity_path,
            output_directory=output_directory,
        )

        self.seed_mule_events_path = Path(
            seed_mule_events_path
        )

        self.counterparty_events_path = Path(
            counterparty_events_path
        )

    def get_seed_mule_events(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        frame = self._read_csv(
            self.seed_mule_events_path
        )

        prepared = prepare_seed_mule_events(frame)

        return (
            prepared.loc[
                prepared["snapshot_date"]
                == resolved_run_date
            ]
            .copy()
            .reset_index(drop=True)
        )

    def get_counterparty_events(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        frame = self._read_csv(
            self.counterparty_events_path
        )

        prepared = prepare_counterparty_events(frame)

        return (
            prepared.loc[
                prepared["snapshot_date"]
                == resolved_run_date
            ]
            .copy()
            .reset_index(drop=True)
        )
