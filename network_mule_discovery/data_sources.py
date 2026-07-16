"""Data-source interfaces and the demo CSV implementation."""

from __future__ import annotations

from collections.abc import Collection
from datetime import date
from pathlib import Path
from typing import Protocol

import pandas as pd

from network_mule_discovery.schemas import (
    normalize_emirates_id,
    parse_run_date,
    prepare_customer_identity,
    prepare_seed_mules,
)


class NetworkDataSource(Protocol):
    """Physical data-source boundary used by discovery services."""

    def get_seed_mules(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        """Return the distinct seed mule pool for a run date."""
        ...

    def get_entities_by_lookup_customer_ids(
        self,
        lookup_customer_ids: Collection[str],
        run_date: date | str,
    ) -> pd.DataFrame:
        """Resolve source customer IDs to SME or retail entities."""
        ...

    def search_entities_by_eids(
        self,
        emirates_ids: Collection[str],
        run_date: date | str,
    ) -> pd.DataFrame:
        """Return identity rows matching a set of normalized EIDs."""
        ...

    def save_discovered_groups(
        self,
        groups: pd.DataFrame,
        nodes: pd.DataFrame,
        edges: pd.DataFrame,
    ) -> None:
        """Persist discovery outputs."""
        ...


class CsvNetworkDataSource:
    """CSV-backed data source used for local development and tests."""

    def __init__(
        self,
        seed_mule_pool_path: Path | str,
        customer_identity_path: Path | str,
        output_directory: Path | str,
    ) -> None:
        self.seed_mule_pool_path = Path(seed_mule_pool_path)
        self.customer_identity_path = Path(customer_identity_path)
        self.output_directory = Path(output_directory)

    @staticmethod
    def _read_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Required CSV file does not exist: {path}")

        return pd.read_csv(
            path,
            dtype="string",
            keep_default_na=False,
        )

    def _load_seed_mules(self) -> pd.DataFrame:
        frame = self._read_csv(self.seed_mule_pool_path)
        return prepare_seed_mules(frame)

    def _load_customer_identity(self) -> pd.DataFrame:
        frame = self._read_csv(self.customer_identity_path)
        return prepare_customer_identity(frame)

    def get_seed_mules(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)
        frame = self._load_seed_mules()

        result = frame.loc[
            frame["snapshot_date"] == resolved_run_date
        ].copy()

        result = (
            result
            .sort_values(
                by=["seed_customer_id", "seed_source"],
                kind="stable",
            )
            .drop_duplicates(
                subset=["seed_customer_id"],
                keep="first",
            )
            .reset_index(drop=True)
        )

        return result

    def get_entities_by_lookup_customer_ids(
        self,
        lookup_customer_ids: Collection[str],
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        normalized_lookup_ids = {
            str(value).strip()
            for value in lookup_customer_ids
            if value is not None and str(value).strip()
        }

        if not normalized_lookup_ids:
            return self._load_customer_identity().iloc[0:0].copy()

        frame = self._load_customer_identity()

        result = frame.loc[
            (frame["snapshot_date"] == resolved_run_date)
            & frame["lookup_customer_id"].isin(normalized_lookup_ids)
        ].copy()

        return (
            result
            .drop_duplicates()
            .sort_values(
                by=[
                    "entity_key",
                    "individual_id",
                    "emirates_id_number",
                ],
                kind="stable",
                na_position="last",
            )
            .reset_index(drop=True)
        )

    def search_entities_by_eids(
        self,
        emirates_ids: Collection[str],
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        normalized_eids = {
            normalized
            for value in emirates_ids
            if (normalized := normalize_emirates_id(value)) is not None
        }

        if not normalized_eids:
            return self._load_customer_identity().iloc[0:0].copy()

        frame = self._load_customer_identity()

        result = frame.loc[
            (frame["snapshot_date"] == resolved_run_date)
            & frame["eid_linkable_flag"]
            & frame["emirates_id_number"].isin(normalized_eids)
        ].copy()

        return (
            result
            .drop_duplicates()
            .sort_values(
                by=[
                    "emirates_id_number",
                    "entity_key",
                    "individual_id",
                ],
                kind="stable",
                na_position="last",
            )
            .reset_index(drop=True)
        )

    def save_discovered_groups(
        self,
        groups: pd.DataFrame,
        nodes: pd.DataFrame,
        edges: pd.DataFrame,
    ) -> None:
        self.output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        groups.to_csv(
            self.output_directory / "discovered_groups.csv",
            index=False,
        )

        nodes.to_csv(
            self.output_directory / "discovered_group_nodes.csv",
            index=False,
        )

        edges.to_csv(
            self.output_directory / "discovered_group_edges.csv",
            index=False,
        )
