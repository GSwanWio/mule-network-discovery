"""In-memory discovery data source over canonical inputs."""

from __future__ import annotations

from collections.abc import Collection
from datetime import date

import pandas as pd

from network_mule_discovery.raw_source_adapter import (
    CanonicalDiscoveryInputs,
)
from network_mule_discovery.schemas import (
    normalize_emirates_id,
    parse_run_date,
)


class InMemoryCounterpartyNetworkDataSource:
    """Serve canonical discovery frames without intermediate CSV files."""

    def __init__(
        self,
        inputs: CanonicalDiscoveryInputs,
    ) -> None:
        if not isinstance(
            inputs,
            CanonicalDiscoveryInputs,
        ):
            raise TypeError(
                "inputs must be CanonicalDiscoveryInputs."
            )

        self._seed_mules = inputs.seed_mules.copy()
        self._customer_identity = (
            inputs.customer_identity.copy()
        )
        self._seed_mule_events = (
            inputs.seed_mule_events.copy()
        )
        self._counterparty_events = (
            inputs.counterparty_events.copy()
        )

        self.saved_groups: pd.DataFrame | None = None
        self.saved_nodes: pd.DataFrame | None = None
        self.saved_edges: pd.DataFrame | None = None

    def get_seed_mules(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        return (
            self._seed_mules.loc[
                self._seed_mules["snapshot_date"]
                == resolved_run_date
            ]
            .copy()
            .sort_values(
                by=[
                    "seed_customer_id",
                    "seed_source",
                ],
                kind="stable",
            )
            .drop_duplicates(
                subset=["seed_customer_id"],
                keep="first",
            )
            .reset_index(drop=True)
        )

    def get_entities_by_lookup_customer_ids(
        self,
        lookup_customer_ids: Collection[str],
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        normalized_lookup_ids = {
            str(value).strip()
            for value in lookup_customer_ids
            if value is not None
            and str(value).strip()
        }

        if not normalized_lookup_ids:
            return self._customer_identity.iloc[
                0:0
            ].copy()

        result = self._customer_identity.loc[
            (
                self._customer_identity["snapshot_date"]
                == resolved_run_date
            )
            & self._customer_identity[
                "lookup_customer_id"
            ].isin(normalized_lookup_ids)
        ].copy()

        return (
            result.drop_duplicates()
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
            if (
                normalized
                := normalize_emirates_id(value)
            )
            is not None
        }

        if not normalized_eids:
            return self._customer_identity.iloc[
                0:0
            ].copy()

        result = self._customer_identity.loc[
            (
                self._customer_identity["snapshot_date"]
                == resolved_run_date
            )
            & self._customer_identity[
                "eid_linkable_flag"
            ]
            & self._customer_identity[
                "emirates_id_number"
            ].isin(normalized_eids)
        ].copy()

        return (
            result.drop_duplicates()
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

    def get_seed_mule_events(
        self,
        run_date: date | str,
    ) -> pd.DataFrame:
        resolved_run_date = parse_run_date(run_date)

        return (
            self._seed_mule_events.loc[
                self._seed_mule_events[
                    "snapshot_date"
                ]
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

        return (
            self._counterparty_events.loc[
                self._counterparty_events[
                    "snapshot_date"
                ]
                == resolved_run_date
            ]
            .copy()
            .reset_index(drop=True)
        )

    def save_discovered_groups(
        self,
        groups: pd.DataFrame,
        nodes: pd.DataFrame,
        edges: pd.DataFrame,
    ) -> None:
        self.saved_groups = groups.copy()
        self.saved_nodes = nodes.copy()
        self.saved_edges = edges.copy()
