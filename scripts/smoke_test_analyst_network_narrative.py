"""Validate deterministic analyst network narrative counts."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_investigation_view import (
    AnalystInvestigationView,
)
from network_mule_discovery.analyst_network_narrative import (
    build_analyst_network_narrative,
)


def main() -> None:
    """Validate counts, shape, and factual narrative wording."""
    nodes = pd.DataFrame(
        [
            {
                "node_id": "N_SEED",
                "node_type": "CUSTOMER",
                "decision_category": "SEED",
                "ai_decision": "SEED_CONFIRMED",
                "is_seed": True,
                "depth": 0,
            },
            {
                "node_id": "N_MULE",
                "node_type": "CUSTOMER",
                "decision_category": "CONTINUE",
                "ai_decision": "MULE_LIKE",
                "is_seed": False,
                "depth": 2,
            },
            {
                "node_id": "N_EID",
                "node_type": "CUSTOMER",
                "decision_category": "DETERMINISTIC",
                "ai_decision": "DETERMINISTIC_EID_LINK",
                "is_seed": False,
                "depth": 1,
            },
            {
                "node_id": "N_EXPOSED",
                "node_type": "CUSTOMER",
                "decision_category": "STOP",
                "ai_decision": "EXPOSED_VULNERABLE",
                "is_seed": False,
                "depth": 3,
            },
            {
                "node_id": "N_SUSPICIOUS_CP",
                "node_type": "COUNTERPARTY",
                "decision_category": "CONTINUE",
                "ai_decision": "SUSPICIOUS_EXPAND",
                "is_seed": False,
                "depth": 1,
            },
            {
                "node_id": "N_LEGIT_CP",
                "node_type": "COUNTERPARTY",
                "decision_category": "STOP",
                "ai_decision": "LEGITIMATE_SUPPRESS",
                "is_seed": False,
                "depth": 1,
            },
            {
                "node_id": "N_UNCERTAIN_CP",
                "node_type": "COUNTERPARTY",
                "decision_category": "STOP",
                "ai_decision": (
                    "INSUFFICIENT_EVIDENCE_SUPPRESS"
                ),
                "is_seed": False,
                "depth": 2,
            },
        ]
    )

    edges = pd.DataFrame(
        [
            {
                "source_node_id": "N_SEED",
                "target_node_id": "N_EID",
            },
            {
                "source_node_id": "N_SEED",
                "target_node_id": "N_SUSPICIOUS_CP",
            },
            {
                "source_node_id": "N_SEED",
                "target_node_id": "N_LEGIT_CP",
            },
            {
                "source_node_id": "N_SUSPICIOUS_CP",
                "target_node_id": "N_MULE",
            },
            {
                "source_node_id": "N_MULE",
                "target_node_id": "N_EXPOSED",
            },
            {
                "source_node_id": "N_MULE",
                "target_node_id": "N_UNCERTAIN_CP",
            },
            {
                "source_node_id": "N_UNCERTAIN_CP",
                "target_node_id": "N_EXPOSED",
            },
        ]
    )

    investigation = AnalystInvestigationView(
        nodes=nodes,
        edges=edges,
        collapsed_counterparties=pd.DataFrame(),
        investigation_status="AI_REVIEW_COMPLETE",
        max_depth=3,
        seed_count=1,
        expanded_node_count=2,
        stopped_node_count=3,
        deterministic_node_count=1,
        pending_node_count=0,
        failed_node_count=0,
        collapsed_customer_count=4,
    )

    narrative = build_analyst_network_narrative(
        investigation
    )

    assert narrative.linked_customer_count == 7
    assert narrative.counterparty_count == 3
    assert narrative.likely_mule_customer_count == 1
    assert narrative.deterministic_customer_count == 1
    assert narrative.non_suspicious_customer_count == 1
    assert narrative.suspicious_counterparty_count == 1
    assert narrative.legitimate_counterparty_count == 1
    assert narrative.uncertain_counterparty_count == 1
    assert narrative.summarized_customer_count == 4
    assert narrative.visible_node_count == 7
    assert narrative.visible_edge_count == 7
    assert narrative.root_branch_count == 3
    assert narrative.cross_link_count == 1
    assert narrative.max_depth == 3
    assert narrative.shape_label == (
        "multi-layer interconnected network"
    )
    assert narrative.headline == (
        "7 linked customers across a "
        "multi-layer interconnected network"
    )
    assert len(narrative.paragraphs) == 4
    assert "2 customers as mule-related" in (
        narrative.paragraphs[1]
    )
    assert "4 customers" in narrative.paragraphs[3]

    print(
        "Analyst network narrative smoke test passed."
    )
    print("Linked customers: 7")
    print("Likely mule customers: 1")
    print("Deterministic customers: 1")
    print("Non-suspicious customers: 1")
    print("Suspicious counterparties: 1")
    print("Legitimate counterparties: 1")
    print("Uncertain counterparties: 1")
    print("Customers summarized: 4")
    print(
        "Network shape: "
        "multi-layer interconnected network"
    )
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
