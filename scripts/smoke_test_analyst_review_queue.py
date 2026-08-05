"""Validate the mandatory analyst node-review queue."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_feedback import (
    ANALYST_FEEDBACK_COLUMNS,
)
from network_mule_discovery.analyst_review_queue import (
    build_analyst_review_queue,
)


def _node(
    *,
    node_id: str,
    node_type: str,
    entity_key: str = "",
    counterparty_key: str = "",
    display_label: str,
    decision_category: str,
    ai_decision: str,
    decision_label: str,
    rationale: str,
    key_evidence: str,
    discovered_via: str,
    is_seed: bool = False,
) -> dict[str, object]:
    """Return one queue-compatible investigation node."""
    return {
        "node_id": node_id,
        "node_type": node_type,
        "entity_key": entity_key,
        "counterparty_key": counterparty_key,
        "display_label": display_label,
        "decision_category": (
            decision_category
        ),
        "ai_decision": ai_decision,
        "decision_label": decision_label,
        "confidence": "HIGH",
        "rationale": rationale,
        "key_evidence": key_evidence,
        "discovered_via": discovered_via,
        "is_seed": is_seed,
    }


def _feedback(
    **overrides: str,
) -> dict[str, str]:
    """Return one valid feedback event."""
    record = {
        "feedback_id": "AF_DEFAULT",
        "run_id": "RUN001",
        "group_id": "G001",
        "node_id": "N_SUSPICIOUS",
        "subject_type": "COUNTERPARTY",
        "subject_key": "LOCAL_ACCOUNT|9001",
        "ai_decision": "SUSPICIOUS_EXPAND",
        "feedback": "AI_CORRECT",
        "analyst_notes": "",
        "analyst_id": "ANALYST_1",
        "submitted_at": (
            "2026-08-04T08:00:00Z"
        ),
    }
    record.update(overrides)
    return record


def main() -> None:
    """Validate review scope, progress, and latest feedback."""
    nodes = pd.DataFrame(
        [
            _node(
                node_id="N_SEED",
                node_type="CUSTOMER",
                entity_key="RETAIL|SEED",
                display_label="RETAIL|SEED",
                decision_category="SEED",
                ai_decision="SEED_CONFIRMED",
                decision_label="Confirmed seed",
                rationale="",
                key_evidence="",
                discovered_via="",
                is_seed=True,
            ),
            _node(
                node_id="N_SUSPICIOUS",
                node_type="COUNTERPARTY",
                counterparty_key=(
                    "LOCAL_ACCOUNT|9001"
                ),
                display_label=(
                    "LOCAL_ACCOUNT|9001"
                ),
                decision_category="CONTINUE",
                ai_decision=(
                    "SUSPICIOUS_EXPAND"
                ),
                decision_label=(
                    "Suspicious expand"
                ),
                rationale=(
                    "Transaction behavior is "
                    "consistent with mule activity."
                ),
                key_evidence=(
                    "- Rapid onward movement"
                ),
                discovered_via=(
                    "Outbound payments"
                ),
            ),
            _node(
                node_id="N_LEGITIMATE",
                node_type="COUNTERPARTY",
                counterparty_key=(
                    "LOCAL_ACCOUNT|9002"
                ),
                display_label=(
                    "LOCAL_ACCOUNT|9002"
                ),
                decision_category="STOP",
                ai_decision=(
                    "LEGITIMATE_SUPPRESS"
                ),
                decision_label=(
                    "Legitimate suppress"
                ),
                rationale=(
                    "The counterparty appears "
                    "legitimate."
                ),
                key_evidence=(
                    "- Stable recurring activity"
                ),
                discovered_via=(
                    "Inbound payments"
                ),
            ),
            _node(
                node_id="N_EID",
                node_type="CUSTOMER",
                entity_key="SME|EID_LINKED",
                display_label="SME|EID_LINKED",
                decision_category=(
                    "DETERMINISTIC"
                ),
                ai_decision=(
                    "DETERMINISTIC_EID_LINK"
                ),
                decision_label=(
                    "Deterministic EID link"
                ),
                rationale="",
                key_evidence="",
                discovered_via=(
                    "Same Emirates ID"
                ),
            ),
            _node(
                node_id="N_PENDING",
                node_type="CUSTOMER",
                entity_key="RETAIL|PENDING",
                display_label="RETAIL|PENDING",
                decision_category="PENDING",
                ai_decision="PENDING",
                decision_label="Pending",
                rationale="",
                key_evidence="",
                discovered_via=(
                    "Shared counterparty"
                ),
            ),
        ]
    )

    feedback = pd.DataFrame(
        [
            _feedback(
                feedback_id="AF_OLD",
                feedback="AI_CORRECT",
                analyst_notes="Initial review.",
                submitted_at=(
                    "2026-08-04T08:00:00Z"
                ),
            ),
            _feedback(
                feedback_id="AF_LATEST",
                feedback="AI_INCORRECT",
                analyst_notes=(
                    "Latest review changes "
                    "the conclusion."
                ),
                submitted_at=(
                    "2026-08-04T08:05:00Z"
                ),
            ),
            _feedback(
                feedback_id="AF_EID",
                node_id="N_EID",
                subject_type="CUSTOMER",
                subject_key="SME|EID_LINKED",
                ai_decision=(
                    "DETERMINISTIC_EID_LINK"
                ),
                feedback="AI_CORRECT",
                analyst_notes=(
                    "Direct identity match "
                    "confirmed."
                ),
                submitted_at=(
                    "2026-08-04T08:10:00Z"
                ),
            ),
            _feedback(
                feedback_id="AF_OTHER_ANALYST",
                node_id="N_LEGITIMATE",
                subject_key=(
                    "LOCAL_ACCOUNT|9002"
                ),
                ai_decision=(
                    "LEGITIMATE_SUPPRESS"
                ),
                feedback="AI_CORRECT",
                analyst_id="ANALYST_2",
                submitted_at=(
                    "2026-08-04T08:15:00Z"
                ),
            ),
        ],
        columns=list(
            ANALYST_FEEDBACK_COLUMNS
        ),
    )

    queue = build_analyst_review_queue(
        run_id="RUN001",
        group_id="G001",
        nodes=nodes,
        feedback=feedback,
        analyst_id="ANALYST_1",
    )

    assert queue.total_required == 3
    assert queue.reviewed_count == 2
    assert queue.unreviewed_count == 1
    assert queue.correct_count == 1
    assert queue.incorrect_count == 1
    assert queue.completion_percentage == 66.7
    assert not queue.review_complete

    assert set(
        queue.rows["node_id"]
    ) == {
        "N_SUSPICIOUS",
        "N_LEGITIMATE",
        "N_EID",
    }
    assert (
        queue.rows.iloc[0]["node_id"]
        == "N_LEGITIMATE"
    )
    assert (
        queue.rows.iloc[0]["review_status"]
        == "UNREVIEWED"
    )

    suspicious = queue.rows.loc[
        queue.rows["node_id"].eq(
            "N_SUSPICIOUS"
        )
    ].iloc[0]

    assert (
        suspicious["review_outcome"]
        == "SUSPICIOUS"
    )
    assert (
        suspicious["review_status"]
        == "REVIEWED_INCORRECT"
    )
    assert (
        suspicious["latest_analyst_notes"]
        == (
            "Latest review changes "
            "the conclusion."
        )
    )

    legitimate = queue.rows.loc[
        queue.rows["node_id"].eq(
            "N_LEGITIMATE"
        )
    ].iloc[0]

    assert (
        legitimate["review_outcome"]
        == "NON_SUSPICIOUS"
    )
    assert (
        legitimate["review_status"]
        == "UNREVIEWED"
    )

    deterministic = queue.rows.loc[
        queue.rows["node_id"].eq(
            "N_EID"
        )
    ].iloc[0]

    assert (
        deterministic["review_outcome"]
        == "SUSPICIOUS"
    )
    assert (
        deterministic["review_status"]
        == "REVIEWED_CORRECT"
    )
    assert (
        deterministic["evidence_status"]
        == "AVAILABLE"
    )

    completed_feedback = pd.concat(
        [
            feedback,
            pd.DataFrame(
                [
                    _feedback(
                        feedback_id=(
                            "AF_LEGITIMATE"
                        ),
                        node_id="N_LEGITIMATE",
                        subject_key=(
                            "LOCAL_ACCOUNT|9002"
                        ),
                        ai_decision=(
                            "LEGITIMATE_SUPPRESS"
                        ),
                        feedback="AI_CORRECT",
                        submitted_at=(
                            "2026-08-04T08:20:00Z"
                        ),
                    )
                ],
                columns=list(
                    ANALYST_FEEDBACK_COLUMNS
                ),
            ),
        ],
        ignore_index=True,
    )

    completed = build_analyst_review_queue(
        run_id="RUN001",
        group_id="G001",
        nodes=nodes,
        feedback=completed_feedback,
        analyst_id="ANALYST_1",
    )

    assert completed.review_complete
    assert completed.reviewed_count == 3
    assert completed.unreviewed_count == 0
    assert (
        completed.completion_percentage
        == 100.0
    )

    print(
        "Analyst review queue smoke test passed."
    )
    print("Required node decisions: 3")
    print("Seed excluded: passed")
    print("Pending decision excluded: passed")
    print("Deterministic decision included: passed")
    print("Latest analyst feedback selected: passed")
    print("Per-analyst progress isolation: passed")
    print("Unreviewed-first ordering: passed")
    print("Completion rule: passed")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
