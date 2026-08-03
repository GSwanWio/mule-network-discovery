"""Validate the analyst breadth-and-depth journey contract."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_investigation_view import (
    build_analyst_investigation_view,
)
from network_mule_discovery.daily_ai_runner import (
    AI_CALL_LEDGER_COLUMNS,
)
from network_mule_discovery.decision_engine import (
    DECISION_REQUIRED_COLUMNS,
)


def _decision(
    *,
    decision_id: str,
    subject_type: str,
    subject_key: str,
    decision: str,
    reason_code: str,
    decided_at: str,
) -> dict[str, str]:
    record = {
        column: ""
        for column in DECISION_REQUIRED_COLUMNS
    }

    record.update(
        {
            "decision_id": decision_id,
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                f"HASH_{decision_id}"
            ),
            "decision": decision,
            "reason_code": reason_code,
            "decision_version": (
                "analyst-journey-test-v1"
            ),
            "decided_at": decided_at,
            "source": "OFFLINE_TEST",
        }
    )

    return record


def _ai_call(
    *,
    ai_call_id: str,
    subject_type: str,
    subject_key: str,
    decision: str,
    reason_code: str,
    confidence: str,
    rationale: str,
    evidence: list[str],
    attempted_at: str,
) -> dict[str, str]:
    record = {
        column: ""
        for column in AI_CALL_LEDGER_COLUMNS
    }

    record.update(
        {
            "ai_call_id": ai_call_id,
            "run_date": "2026-08-03",
            "queue_item_id": (
                f"QUEUE_{ai_call_id}"
            ),
            "action_type": (
                "RUN_COUNTERPARTY_AI"
                if subject_type == "COUNTERPARTY"
                else "RUN_CUSTOMER_AI"
            ),
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                f"HASH_{ai_call_id}"
            ),
            "call_status": "COMPLETED",
            "attempted_at": attempted_at,
            "generated_decision_id": (
                f"DECISION_{ai_call_id}"
            ),
            "decision": decision,
            "reason_code": reason_code,
            "confidence": confidence,
            "rationale": rationale,
            "key_evidence_json": (
                pd.Series([evidence])
                .to_json(orient="values")
            )[1:-1],
            "model": "offline-test-model",
            "prompt_version": (
                "analyst-journey-test-v1"
            ),
        }
    )

    return record


def main() -> None:
    nodes = pd.DataFrame(
        [
            {
                "group_id": "G001",
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
                "group_id": "G001",
                "node_id": "N_CP_SUSPICIOUS",
                "node_key": "COUNTERPARTY|CP_SUSPICIOUS",
                "node_type": "COUNTERPARTY",
                "entity_key": "",
                "counterparty_key": "CP_SUSPICIOUS",
                "display_label": "Suspicious counterparty",
                "node_roles": "COUNTERPARTY",
                "node_status": (
                    "COUNTERPARTY_APPROVED_SUSPICIOUS"
                ),
                "customer_assessment_status": "",
                "customer_discovery_allowed_flag": True,
                "expansion_source_flag": False,
            },
            {
                "group_id": "G001",
                "node_id": "N_MULE",
                "node_key": "CUSTOMER|C_MULE",
                "node_type": "CUSTOMER",
                "entity_key": "RETAIL|C_MULE",
                "counterparty_key": "",
                "display_label": "Discovered mule",
                "node_roles": "DISCOVERED_CUSTOMER",
                "node_status": (
                    "CUSTOMER_APPROVED_MULE_LIKE"
                ),
                "customer_assessment_status": "MULE_LIKE",
                "customer_discovery_allowed_flag": True,
                "expansion_source_flag": True,
            },
            {
                "group_id": "G001",
                "node_id": "N_CP_COMMON",
                "node_key": "COUNTERPARTY|CP_COMMON",
                "node_type": "COUNTERPARTY",
                "entity_key": "",
                "counterparty_key": "CP_COMMON",
                "display_label": "Utility provider",
                "node_roles": "COUNTERPARTY",
                "node_status": (
                    "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC"
                ),
                "customer_assessment_status": "",
                "customer_discovery_allowed_flag": False,
                "expansion_source_flag": False,
            },
            {
                "group_id": "G001",
                "node_id": "N_HIDDEN_CUSTOMER",
                "node_key": "CUSTOMER|C_BACKGROUND",
                "node_type": "CUSTOMER",
                "entity_key": "RETAIL|C_BACKGROUND",
                "counterparty_key": "",
                "display_label": "Background customer",
                "node_roles": "DISCOVERED_CUSTOMER",
                "node_status": (
                    "BLOCKED_PENDING_COUNTERPARTY_AI"
                ),
                "customer_assessment_status": (
                    "BLOCKED_PENDING_COUNTERPARTY_AI"
                ),
                "customer_discovery_allowed_flag": False,
                "expansion_source_flag": False,
            },
        ]
    )

    edges = pd.DataFrame(
        [
            {
                "group_id": "G001",
                "edge_id": "E_SEED_CP",
                "source_node_id": "N_SEED",
                "target_node_id": "N_CP_SUSPICIOUS",
                "edge_type": (
                    "SEED_COUNTERPARTY_EVIDENCE"
                ),
                "relationship_status": (
                    "COUNTERPARTY_APPROVED_SUSPICIOUS"
                ),
            },
            {
                "group_id": "G001",
                "edge_id": "E_CP_MULE",
                "source_node_id": "N_CP_SUSPICIOUS",
                "target_node_id": "N_MULE",
                "edge_type": (
                    "SHARED_EXTERNAL_COUNTERPARTY"
                ),
                "relationship_status": (
                    "COUNTERPARTY_APPROVED_SUSPICIOUS"
                ),
            },
            {
                "group_id": "G001",
                "edge_id": "E_MULE_COMMON",
                "source_node_id": "N_MULE",
                "target_node_id": "N_CP_COMMON",
                "edge_type": (
                    "CUSTOMER_COUNTERPARTY_EVIDENCE"
                ),
                "relationship_status": (
                    "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC"
                ),
            },
            {
                "group_id": "G001",
                "edge_id": "E_COMMON_BACKGROUND",
                "source_node_id": "N_CP_COMMON",
                "target_node_id": "N_HIDDEN_CUSTOMER",
                "edge_type": (
                    "SHARED_EXTERNAL_COUNTERPARTY"
                ),
                "relationship_status": (
                    "COUNTERPARTY_SUPPRESSED_COMMON_PUBLIC"
                ),
            },
        ]
    )

    decisions = pd.DataFrame(
        [
            _decision(
                decision_id="D_CP_SUSPICIOUS",
                subject_type="COUNTERPARTY",
                subject_key="CP_SUSPICIOUS",
                decision="SUSPICIOUS_EXPAND",
                reason_code="STRONG_NETWORK_EVIDENCE",
                decided_at="2026-08-03T10:00:00Z",
            ),
            _decision(
                decision_id="D_MULE",
                subject_type="CUSTOMER",
                subject_key="RETAIL|C_MULE",
                decision="MULE_LIKE",
                reason_code="MULE_BEHAVIOUR_CONFIRMED",
                decided_at="2026-08-03T10:01:00Z",
            ),
            _decision(
                decision_id="D_CP_COMMON",
                subject_type="COUNTERPARTY",
                subject_key="CP_COMMON",
                decision="COMMON_PUBLIC_SUPPRESS",
                reason_code="COMMON_UTILITY_PROVIDER",
                decided_at="2026-08-03T10:02:00Z",
            ),
        ],
        columns=list(
            DECISION_REQUIRED_COLUMNS
        ),
    )

    ai_calls = pd.DataFrame(
        [
            _ai_call(
                ai_call_id="AI_CP_SUSPICIOUS",
                subject_type="COUNTERPARTY",
                subject_key="CP_SUSPICIOUS",
                decision="SUSPICIOUS_EXPAND",
                reason_code="STRONG_NETWORK_EVIDENCE",
                confidence="0.94",
                rationale=(
                    "The counterparty is strongly linked "
                    "to the seed and should be expanded."
                ),
                evidence=[
                    "Repeated transfers from the seed",
                    "Unusual shared-customer activity",
                ],
                attempted_at="2026-08-03T10:00:00Z",
            ),
            _ai_call(
                ai_call_id="AI_MULE",
                subject_type="CUSTOMER",
                subject_key="RETAIL|C_MULE",
                decision="MULE_LIKE",
                reason_code="MULE_BEHAVIOUR_CONFIRMED",
                confidence="0.91",
                rationale=(
                    "The customer displays mule-like "
                    "transaction behaviour."
                ),
                evidence=[
                    "Rapid movement of received funds",
                    "Shared suspicious counterparty",
                ],
                attempted_at="2026-08-03T10:01:00Z",
            ),
            _ai_call(
                ai_call_id="AI_CP_COMMON",
                subject_type="COUNTERPARTY",
                subject_key="CP_COMMON",
                decision="COMMON_PUBLIC_SUPPRESS",
                reason_code="COMMON_UTILITY_PROVIDER",
                confidence="0.98",
                rationale=(
                    "The counterparty is a common utility "
                    "provider and should not be expanded."
                ),
                evidence=[
                    "High customer degree",
                    "Normal utility-payment pattern",
                ],
                attempted_at="2026-08-03T10:02:00Z",
            ),
        ],
        columns=list(
            AI_CALL_LEDGER_COLUMNS
        ),
    )

    view = build_analyst_investigation_view(
        nodes=nodes,
        edges=edges,
        decisions=decisions,
        ai_calls=ai_calls,
    )

    assert view.investigation_status == (
        "AI_REVIEW_COMPLETE"
    )
    assert view.max_depth == 3
    assert view.seed_count == 1
    assert view.expanded_node_count == 2
    assert view.stopped_node_count == 1
    assert view.pending_node_count == 0
    assert view.failed_node_count == 0
    assert view.collapsed_customer_count == 1

    assert set(view.nodes["node_id"]) == {
        "N_SEED",
        "N_CP_SUSPICIOUS",
        "N_MULE",
        "N_CP_COMMON",
    }

    depths = (
        view.nodes.set_index("node_id")[
            "depth"
        ].to_dict()
    )

    assert depths == {
        "N_SEED": 0,
        "N_CP_SUSPICIOUS": 1,
        "N_MULE": 2,
        "N_CP_COMMON": 3,
    }

    common_node = (
        view.nodes.loc[
            view.nodes["node_id"].eq(
                "N_CP_COMMON"
            )
        ]
        .iloc[0]
    )

    assert (
        common_node["decision_category"]
        == "STOP"
    )
    assert (
        common_node["ai_decision"]
        == "COMMON_PUBLIC_SUPPRESS"
    )
    assert (
        common_node["collapsed_customer_count"]
        == 1
    )
    assert (
        common_node["parent_node_id"]
        == "N_MULE"
    )
    assert "utility provider" in (
        common_node["rationale"].lower()
    )
    assert "High customer degree" in (
        common_node["key_evidence"]
    )

    print(
        "Analyst investigation journey smoke test passed."
    )
    print("Seed depth: 0")
    print("Suspicious counterparty depth: 1")
    print("Mule-like customer depth: 2")
    print("Suppressed counterparty depth: 3")
    print("Suppressed branch customers displayed: 0")
    print("AI continue decisions displayed: 2")
    print("AI stop decisions displayed: 1")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
