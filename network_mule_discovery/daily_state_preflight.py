"""Validate persisted network state before daily processing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
)


REQUIRED_NETWORK_STATE_FILENAMES = (
    "network_state_groups.csv",
    "network_state_nodes.csv",
    "network_state_edges.csv",
)


class DailyStatePreflightError(RuntimeError):
    """Persisted daily state is missing or invalid."""


@dataclass(frozen=True)
class DailyStatePreflightResult:
    """Validated persisted-network summary."""

    state_directory: Path
    group_count: int
    node_count: int
    edge_count: int


def validate_daily_state_preflight(
    state_directory: Path | str,
) -> DailyStatePreflightResult:
    """Validate that daily planning has a usable snapshot."""
    resolved_directory = Path(
        state_directory
    )

    missing_files = [
        filename
        for filename
        in REQUIRED_NETWORK_STATE_FILENAMES
        if not (
            resolved_directory
            / filename
        ).is_file()
    ]

    if missing_files:
        formatted_files = ", ".join(
            missing_files
        )

        raise DailyStatePreflightError(
            "Daily state is not initialized. "
            f"State directory: {resolved_directory}. "
            "Missing required files: "
            f"{formatted_files}. "
            "Materialize and persist the unified network "
            "snapshot before planning or live AI execution."
        )

    state_store = CsvDailyStateStore(
        resolved_directory
    )

    try:
        network = (
            state_store.load_network_state()
        )

    except Exception as exc:
        raise DailyStatePreflightError(
            "Daily state preflight failed while reading "
            f"{resolved_directory}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    group_count = len(network.groups)
    node_count = len(network.nodes)
    edge_count = len(network.edges)

    if group_count == 0:
        raise DailyStatePreflightError(
            "Daily state contains zero groups. "
            "A usable unified network snapshot is required."
        )

    if node_count == 0:
        raise DailyStatePreflightError(
            "Daily state contains zero nodes. "
            "A usable unified network snapshot is required."
        )

    return DailyStatePreflightResult(
        state_directory=resolved_directory,
        group_count=group_count,
        node_count=node_count,
        edge_count=edge_count,
    )
