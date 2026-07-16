"""Smoke validation for Section 1 EID-only discovery."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.daily_runner import run_eid_discovery
from network_mule_discovery.data_sources import CsvNetworkDataSource


RUN_DATE = "2026-07-16"


def build_data_source() -> CsvNetworkDataSource:
    """Construct the demo CSV data source."""
    return CsvNetworkDataSource(
        seed_mule_pool_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_pool.csv"
        ),
        customer_identity_path=(
            PROJECT_ROOT
            / "data/demo/customer_identity.csv"
        ),
        output_directory=(
            PROJECT_ROOT
            / "data/demo/output"
        ),
    )


def main() -> None:
    """Run all Section 1 smoke assertions."""
    first_result = run_eid_discovery(
        data_source=build_data_source(),
        run_date=RUN_DATE,
        persist_outputs=True,
    )

    discovery = first_result.discovery
    graph = first_result.graph

    assert len(discovery.seed_resolution.seeds) == 3

    assert (
        discovery.seed_resolution.seed_entities[
            "entity_key"
        ].nunique()
        == 2
    )

    assert (
        discovery.seed_resolution.unresolved_seeds[
            "seed_customer_id"
        ].tolist()
        == ["UNRESOLVED1"]
    )

    assert discovery.seed_eids[
        "emirates_id_number"
    ].nunique() == 3

    assert len(discovery.eid_links) == 5
    assert len(graph.groups) == 2
    assert len(graph.nodes) == 7
    assert len(graph.edges) == 5

    assert sorted(
        graph.groups["total_entity_count"].tolist()
    ) == [3, 4]

    assert sorted(
        graph.groups["edge_count"].tolist()
    ) == [2, 3]

    assert graph.nodes["node_id"].is_unique
    assert graph.edges["edge_id"].is_unique
    assert graph.groups["group_id"].is_unique

    assert not graph.edges[
        "source_entity_key"
    ].eq(
        graph.edges["target_entity_key"]
    ).any()

    node_ids = set(graph.nodes["node_id"])

    assert set(
        graph.edges["source_node_id"]
    ).issubset(node_ids)

    assert set(
        graph.edges["target_node_id"]
    ).issubset(node_ids)

    assert graph.edges[
        "deterministic_flag"
    ].all()

    assert graph.edges[
        "reason_code"
    ].eq(
        "same_eid_as_seed_mule"
    ).all()

    assert set(
        graph.nodes.loc[
            graph.nodes["discovered_flag"],
            "entity_key",
        ]
    ) == {
        "RETAIL|R1002",
        "SME|B3001",
        "RETAIL|R2002",
        "RETAIL|R2003",
        "SME|B2002",
    }

    assert "SME|B2004" not in set(
        graph.nodes["entity_key"]
    )

    output_directory = (
        PROJECT_ROOT
        / "data/demo/output"
    )

    expected_output_files = [
        output_directory / "discovered_groups.csv",
        output_directory / "discovered_group_nodes.csv",
        output_directory / "discovered_group_edges.csv",
    ]

    for path in expected_output_files:
        assert path.exists(), f"Missing output file: {path}"

    persisted_groups = pd.read_csv(
        output_directory / "discovered_groups.csv"
    )

    persisted_nodes = pd.read_csv(
        output_directory / "discovered_group_nodes.csv"
    )

    persisted_edges = pd.read_csv(
        output_directory / "discovered_group_edges.csv"
    )

    assert len(persisted_groups) == 2
    assert len(persisted_nodes) == 7
    assert len(persisted_edges) == 5

    second_result = run_eid_discovery(
        data_source=build_data_source(),
        run_date=RUN_DATE,
        persist_outputs=False,
    )

    assert_frame_equal(
        graph.groups,
        second_result.graph.groups,
        check_dtype=True,
    )

    assert_frame_equal(
        graph.nodes,
        second_result.graph.nodes,
        check_dtype=True,
    )

    assert_frame_equal(
        graph.edges,
        second_result.graph.edges,
        check_dtype=True,
    )

    print("Network discovery smoke test passed.")
    print(f"Groups: {len(graph.groups)}")
    print(f"Nodes: {len(graph.nodes)}")
    print(f"Edges: {len(graph.edges)}")
    print("Deterministic rerun: passed")


if __name__ == "__main__":
    main()
