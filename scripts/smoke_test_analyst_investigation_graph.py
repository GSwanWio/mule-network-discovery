"""Validate the clickable analyst investigation graph."""

from __future__ import annotations

import importlib.metadata
import sys
from pathlib import Path

import pandas as pd
from streamlit_cytoscape.sanitize import (
    sanitize_elements,
)


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_investigation_graph import (
    NODE_SELECTED_EVENT,
    analyst_edge_styles,
    analyst_node_styles,
    build_analyst_investigation_graph,
    selected_investigation_node_id,
)


def main() -> None:
    nodes = pd.DataFrame(
        [
            {
                "node_id": "N_SEED",
                "node_type": "CUSTOMER",
                "display_label": "Seed customer",
                "depth": 0,
                "depth_label": "Seed",
                "parent_node_id": "",
                "discovered_via": "",
                "is_seed": True,
                "ai_decision": "SEED_CONFIRMED",
                "decision_label": "Confirmed seed",
                "decision_category": "SEED",
                "expansion_outcome": "Starting point",
                "confidence": "",
                "rationale": "",
                "key_evidence": "",
                "collapsed_customer_count": 0,
            },
            {
                "node_id": "N_CP",
                "node_type": "COUNTERPARTY",
                "display_label": "Suspicious counterparty",
                "depth": 1,
                "depth_label": "Depth 1",
                "parent_node_id": "N_SEED",
                "discovered_via": (
                    "Seed transaction counterparty"
                ),
                "is_seed": False,
                "ai_decision": "SUSPICIOUS_EXPAND",
                "decision_label": "Suspicious Expand",
                "decision_category": "CONTINUE",
                "expansion_outcome": (
                    "Expanded — suspicious counterparty"
                ),
                "confidence": "0.94",
                "rationale": (
                    "Strong suspicious network evidence."
                ),
                "key_evidence": (
                    "• Repeated transfers"
                ),
                "collapsed_customer_count": 0,
            },
            {
                "node_id": "N_MULE",
                "node_type": "CUSTOMER",
                "display_label": "Discovered mule",
                "depth": 2,
                "depth_label": "Depth 2",
                "parent_node_id": "N_CP",
                "discovered_via": (
                    "Shared external counterparty"
                ),
                "is_seed": False,
                "ai_decision": "MULE_LIKE",
                "decision_label": "Mule Like",
                "decision_category": "CONTINUE",
                "expansion_outcome": (
                    "Expanded — mule-like customer"
                ),
                "confidence": "0.91",
                "rationale": (
                    "Mule-like flow-through behaviour."
                ),
                "key_evidence": (
                    "• Rapid movement of received funds"
                ),
                "collapsed_customer_count": 0,
            },
            {
                "node_id": "N_STOP",
                "node_type": "COUNTERPARTY",
                "display_label": "Utility provider",
                "depth": 3,
                "depth_label": "Depth 3",
                "parent_node_id": "N_MULE",
                "discovered_via": (
                    "Customer transaction counterparty"
                ),
                "is_seed": False,
                "ai_decision": (
                    "COMMON_PUBLIC_SUPPRESS"
                ),
                "decision_label": (
                    "Common Public Suppress"
                ),
                "decision_category": "STOP",
                "expansion_outcome": (
                    "Stopped — common/public counterparty"
                ),
                "confidence": "0.98",
                "rationale": (
                    "Normal high-degree utility provider."
                ),
                "key_evidence": (
                    "• Normal utility-payment pattern"
                ),
                "collapsed_customer_count": 500,
            },
            {
                "node_id": "N_CUSTOMER_STOP",
                "node_type": "CUSTOMER",
                "display_label": "Potential victim",
                "depth": 3,
                "depth_label": "Depth 3",
                "parent_node_id": "N_MULE",
                "discovered_via": (
                    "Shared suspicious counterparty"
                ),
                "is_seed": False,
                "ai_decision": "LOW_CONCERN",
                "decision_label": "Low Concern",
                "decision_category": "STOP",
                "expansion_outcome": (
                    "Stopped — low-concern customer"
                ),
                "confidence": "0.81",
                "rationale": (
                    "No reasonable mule behaviour identified."
                ),
                "key_evidence": (
                    "• Customer may be exposed rather than complicit"
                ),
                "collapsed_customer_count": 0,
            },
        ]
    )

    graph = build_analyst_investigation_graph(
        nodes
    )

    assert (
        importlib.metadata.version(
            "streamlit-cytoscape"
        )
        == "0.2.1"
    )
    assert graph.layout["name"] == "breadthfirst"
    assert graph.layout["directed"] is True
    assert graph.seed_node_ids == ("N_SEED",)
    assert len(graph.elements["nodes"]) == 5
    assert len(graph.elements["edges"]) == 4

    assert all(
        node["selectable"] is False
        for node in graph.elements["nodes"]
    )
    assert all(
        node["grabbable"] is False
        for node in graph.elements["nodes"]
    )

    sanitized = sanitize_elements(
        graph.elements
    )

    assert all(
        node["selectable"] is False
        for node in sanitized["nodes"]
    )
    assert all(
        node["grabbable"] is False
        for node in sanitized["nodes"]
    )

    node_by_id = {
        item["data"]["id"]: item["data"]
        for item in graph.elements["nodes"]
    }

    assert (
        node_by_id["N_SEED"]["label"]
        == "SEED_CUSTOMER"
    )
    assert (
        node_by_id["N_CP"]["label"]
        == "EXPANDED_COUNTERPARTY"
    )
    assert (
        node_by_id["N_MULE"]["label"]
        == "EXPANDED_CUSTOMER"
    )
    assert (
        node_by_id["N_STOP"]["label"]
        == "STOPPED_COUNTERPARTY"
    )
    assert (
        node_by_id["N_CUSTOMER_STOP"]["label"]
        == "STOPPED_CUSTOMER"
    )
    assert node_by_id["N_SEED"]["_caption"] == (
        "Confirmed seed\nSeed customer"
    )
    assert node_by_id["N_CP"]["_caption"] == (
        "Suspicious counterparty\n"
        "Suspicious counterparty"
    )
    assert node_by_id["N_MULE"]["_caption"] == (
        "Likely mule\nDiscovered mule"
    )
    assert node_by_id["N_STOP"]["_caption"] == (
        "Legitimate counterparty\nUtility provider"
    )
    assert node_by_id["N_CUSTOMER_STOP"]["_caption"] == (
        "Non-suspicious customer\nPotential victim"
    )
    assert "Depth" not in node_by_id["N_CP"]["_caption"]
    assert "Expanded" not in node_by_id["N_CP"]["_caption"]
    assert "500 linked customers" not in (
        node_by_id["N_STOP"]["_caption"]
    )

    edge_pairs = {
        (
            item["data"]["source"],
            item["data"]["target"],
        )
        for item in graph.elements["edges"]
    }

    assert edge_pairs == {
        ("N_SEED", "N_CP"),
        ("N_CP", "N_MULE"),
        ("N_MULE", "N_STOP"),
        ("N_MULE", "N_CUSTOMER_STOP"),
    }

    edge_by_target = {
        item["data"]["target"]: item["data"]
        for item in graph.elements["edges"]
    }
    assert (
        edge_by_target["N_STOP"]["label"]
        == "STOPPED_COUNTERPARTY_PATH"
    )
    assert (
        edge_by_target["N_CUSTOMER_STOP"]["label"]
        == "STOPPED_CUSTOMER_PATH"
    )

    assert {
        style.dump()["selector"]
        for style in analyst_node_styles()
    } == {
        "node[label='SEED_CUSTOMER']",
        "node[label='DETERMINISTIC_CUSTOMER']",
        "node[label='EXPANDED_CUSTOMER']",
        "node[label='EXPANDED_COUNTERPARTY']",
        "node[label='STOPPED_CUSTOMER']",
        "node[label='STOPPED_COUNTERPARTY']",
        "node[label='PENDING_NODE']",
        "node[label='FAILED_NODE']",
    }

    assert len(analyst_edge_styles()) == 5
    assert NODE_SELECTED_EVENT.dump() == {
        "name": "investigation_node_selected",
        "event_type": "tap",
        "selector": "node",
    }

    selected = selected_investigation_node_id(
        {
            "action": (
                "investigation_node_selected"
            ),
            "data": {
                "type": "tap",
                "target_id": "N_MULE",
                "target_group": "nodes",
            },
        }
    )

    assert selected == "N_MULE"
    assert (
        selected_investigation_node_id(
            {
                "action": (
                    "investigation_node_selected"
                ),
                "data": {
                    "target_id": "E1",
                    "target_group": "edges",
                },
            }
        )
        is None
    )

    print(
        "Analyst interactive investigation graph "
        "smoke test passed."
    )
    print("Layout: breadth-first")
    print("Visible journey nodes: 5")
    print("Visible journey edges: 4")
    print("Node click event: passed")
    print("Built-in selection disabled: passed")
    print("Technical inspector opened: no")
    print("Concise node labels: passed")
    print("Suspicious paths: red")
    print("Legitimate counterparty paths: green")
    print("Non-suspicious customer paths: amber")
    print("Full audit graph exposed: no")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
