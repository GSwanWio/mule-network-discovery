"""Controlled expectations for full live synthetic acceptance."""

from __future__ import annotations

from dataclasses import dataclass


CONTINUE_DECISIONS = frozenset({
    "SUSPICIOUS_EXPAND",
    "MULE_LIKE",
})

COUNTERPARTY_STOP_DECISIONS = frozenset({
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
})

CUSTOMER_STOP_DECISIONS = frozenset({
    "EXPOSED_VULNERABLE",
    "LOW_CONCERN",
    "INSUFFICIENT_EVIDENCE",
})

STOP_DECISIONS = (
    COUNTERPARTY_STOP_DECISIONS
    | CUSTOMER_STOP_DECISIONS
)


@dataclass(frozen=True)
class LiveAcceptanceCase:
    """Acceptance requirements for one synthetic scenario."""

    scenario_id: str
    title: str
    max_live_calls: int
    assess_eid_linked_customers: bool
    required_decisions: tuple[str, ...]
    required_any_decision_groups: tuple[
        tuple[str, ...],
        ...,
    ]
    expected_decision_count: int
    minimum_continue_decision_count: int
    minimum_stop_decision_count: int
    expected_termination_reason: str
    expected_group_count: int
    expected_raw_node_count: int
    expected_raw_edge_count: int
    expected_visible_max_depth: int
    minimum_collapsed_customer_count: int
    expected_unchanged_rerun_calls: int
    changed_evidence_supported: bool
    expected_changed_evidence_requeued_calls: int


LIVE_ACCEPTANCE_CASES = (
    LiveAcceptanceCase(
        scenario_id="scenario_1",
        title="Breadth-and-depth suspicious expansion",
        max_live_calls=11,
        assess_eid_linked_customers=True,
        required_decisions=(
            "SUSPICIOUS_EXPAND",
            "MULE_LIKE",
        ),
        required_any_decision_groups=(
            (
                "LEGITIMATE_SUPPRESS",
                "COMMON_PUBLIC_SUPPRESS",
            ),
        ),
        expected_decision_count=11,
        minimum_continue_decision_count=4,
        minimum_stop_decision_count=7,
        expected_termination_reason="FRONTIER_EXHAUSTED",
        expected_group_count=1,
        expected_raw_node_count=108,
        expected_raw_edge_count=109,
        expected_visible_max_depth=4,
        minimum_collapsed_customer_count=100,
        expected_unchanged_rerun_calls=0,
        changed_evidence_supported=False,
        expected_changed_evidence_requeued_calls=0,
    ),
    LiveAcceptanceCase(
        scenario_id="scenario_2",
        title="Common public high-degree suppression",
        max_live_calls=1,
        assess_eid_linked_customers=True,
        required_decisions=(),
        required_any_decision_groups=(
            (
                "LEGITIMATE_SUPPRESS",
                "COMMON_PUBLIC_SUPPRESS",
            ),
        ),
        expected_decision_count=1,
        minimum_continue_decision_count=0,
        minimum_stop_decision_count=1,
        expected_termination_reason="FRONTIER_EXHAUSTED",
        expected_group_count=1,
        expected_raw_node_count=502,
        expected_raw_edge_count=501,
        expected_visible_max_depth=1,
        minimum_collapsed_customer_count=500,
        expected_unchanged_rerun_calls=0,
        changed_evidence_supported=False,
        expected_changed_evidence_requeued_calls=0,
    ),
    LiveAcceptanceCase(
        scenario_id="scenario_3",
        title="Beneficiary-linked customer assessment",
        max_live_calls=2,
        assess_eid_linked_customers=True,
        required_decisions=(
            "MULE_LIKE",
        ),
        required_any_decision_groups=(
            (
                "EXPOSED_VULNERABLE",
                "LOW_CONCERN",
                "INSUFFICIENT_EVIDENCE",
            ),
        ),
        expected_decision_count=2,
        minimum_continue_decision_count=1,
        minimum_stop_decision_count=1,
        expected_termination_reason="FRONTIER_EXHAUSTED",
        expected_group_count=1,
        expected_raw_node_count=3,
        expected_raw_edge_count=2,
        expected_visible_max_depth=1,
        minimum_collapsed_customer_count=0,
        expected_unchanged_rerun_calls=0,
        changed_evidence_supported=False,
        expected_changed_evidence_requeued_calls=0,
    ),
    LiveAcceptanceCase(
        scenario_id="scenario_4",
        title="Deterministic Emirates-ID-only groups",
        max_live_calls=0,
        assess_eid_linked_customers=False,
        required_decisions=(),
        required_any_decision_groups=(),
        expected_decision_count=0,
        minimum_continue_decision_count=0,
        minimum_stop_decision_count=0,
        expected_termination_reason="FRONTIER_EXHAUSTED",
        expected_group_count=2,
        expected_raw_node_count=5,
        expected_raw_edge_count=3,
        expected_visible_max_depth=1,
        minimum_collapsed_customer_count=0,
        expected_unchanged_rerun_calls=0,
        changed_evidence_supported=False,
        expected_changed_evidence_requeued_calls=0,
    ),
    LiveAcceptanceCase(
        scenario_id="scenario_5",
        title="Insufficient counterparty evidence suppression",
        max_live_calls=1,
        assess_eid_linked_customers=True,
        required_decisions=(
            "INSUFFICIENT_EVIDENCE_SUPPRESS",
        ),
        required_any_decision_groups=(),
        expected_decision_count=1,
        minimum_continue_decision_count=0,
        minimum_stop_decision_count=1,
        expected_termination_reason="FRONTIER_EXHAUSTED",
        expected_group_count=1,
        expected_raw_node_count=4,
        expected_raw_edge_count=3,
        expected_visible_max_depth=1,
        minimum_collapsed_customer_count=2,
        expected_unchanged_rerun_calls=0,
        changed_evidence_supported=True,
        expected_changed_evidence_requeued_calls=1,
    ),
)


LIVE_ACCEPTANCE_CASE_BY_ID = {
    case.scenario_id: case
    for case in LIVE_ACCEPTANCE_CASES
}

MAXIMUM_INITIAL_LIVE_CALLS = sum(
    case.max_live_calls
    for case in LIVE_ACCEPTANCE_CASES
)


def get_live_acceptance_case(
    scenario_id: str,
) -> LiveAcceptanceCase:
    """Return one supported live acceptance case."""
    normalized = str(scenario_id).strip().lower()

    try:
        return LIVE_ACCEPTANCE_CASE_BY_ID[
            normalized
        ]
    except KeyError as exc:
        raise ValueError(
            "Unsupported live acceptance scenario: "
            f"{scenario_id}"
        ) from exc
