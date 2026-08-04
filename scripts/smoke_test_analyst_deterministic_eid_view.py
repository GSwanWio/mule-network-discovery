"""Validate deterministic EID investigation presentation."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_investigation_graph import (
    build_analyst_investigation_graph,
)
from network_mule_discovery.analyst_investigation_view import (
    AI_CALL_REQUIRED_COLUMNS,
    DECISION_REQUIRED_COLUMNS,
    build_analyst_investigation_view,
)


def main() -> None:
    """Validate deterministic EID completion semantics."""
    nodes = pd.DataFrame(
        [
            {
                "group_id": "G_EID",
                "node_id": "N_SEED",
                "node_key": "CUSTOMER|C_SEED",
                "node_type": "CUSTOMER",
                "entity_key": "RETAIL|C_SEED",
                "counterparty_key": "",
                "display_label": "Seed customer",
                "node_roles": "SEED",
                "node_status": "SEED_CONFIRMED",
                "customer_assessment_status": (
                    "SEED_CONFIRMED"
                ),
                "customer_discovery_allowed_flag": True,
                "expansion_source_flag": True,
            },
            {
                "group_id": "G_EID",
                "node_id": "N_LINKED",
                "node_key": "CUSTOMER|C_LINKED",
                "node_type": "CUSTOMER",
                "entity_key": "SME|C_LINKED",
                "counterparty_key": "",
                "display_label": "EID-linked customer",
                "node_roles": "EID_LINKED_CUSTOMER",
                "node_status": "INCLUDED_DETERMINISTIC",
                "customer_assessment_status": (
                    "NOT_APPLICABLE"
                ),
                "customer_discovery_allowed_flag": False,
                "expansion_source_flag": False,
            },
        ]
    )

    edges = pd.DataFrame(
        [
            {
                "group_id": "G_EID",
                "edge_id": "E_EID",
                "source_node_id": "N_SEED",
                "target_node_id": "N_LINKED",
                "edge_type": "SAME_EMIRATES_ID",
                "relationship_status": (
                    "DETERMINISTIC_LINK"
                ),
            }
        ]
    )

    decisions = pd.DataFrame(
        columns=list(DECISION_REQUIRED_COLUMNS)
    )
    ai_calls = pd.DataFrame(
        columns=list(AI_CALL_REQUIRED_COLUMNS)
    )

    view = build_analyst_investigation_view(
        nodes=nodes,
        edges=edges,
        decisions=decisions,
        ai_calls=ai_calls,
    )

    linked = view.nodes.loc[
        view.nodes["node_id"].eq("N_LINKED")
    ].iloc[0]

    graph = build_analyst_investigation_graph(
        view.nodes
    )
    graph_nodes = {
        item["data"]["id"]: item["data"]
        for item in graph.elements["nodes"]
    }
    graph_edges = graph.elements["edges"]

    assert view.investigation_status == (
        "DETERMINISTIC_REVIEW_COMPLETE"
    )
    assert view.seed_count == 1
    assert view.deterministic_node_count == 1
    assert view.expanded_node_count == 0
    assert view.stopped_node_count == 0
    assert view.pending_node_count == 0
    assert view.failed_node_count == 0

    assert (
        linked["decision_category"]
        == "DETERMINISTIC"
    )
    assert (
        linked["ai_decision"]
        == "DETERMINISTIC_EID_LINK"
    )
    assert (
        linked["decision_label"]
        == "Deterministic EID link"
    )
    assert (
        linked["expansion_outcome"]
        == (
            "Included — deterministic "
            "Emirates ID link"
        )
    )
    assert linked["discovered_via"] == (
        "Same Emirates ID"
    )
    assert (
        graph_nodes["N_SEED"]["label"]
        == "SEED_CUSTOMER"
    )
    assert (
        graph_nodes["N_LINKED"]["label"]
        == "DETERMINISTIC_CUSTOMER"
    )
    assert len(graph_edges) == 1
    assert (
        graph_edges[0]["data"]["label"]
        == "DETERMINISTIC_PATH"
    )

    print(
        "Deterministic EID analyst-view "
        "smoke test passed."
    )
    print(
        "Investigation status: "
        "DETERMINISTIC_REVIEW_COMPLETE"
    )
    print("Deterministic linked nodes: 1")
    print("Pending AI nodes: 0")
    print(
        "Deterministic graph styling: passed"
    )
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
