"""Deterministic analyst-facing narrative for one persisted network."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from network_mule_discovery.analyst_investigation_view import (
    AnalystInvestigationView,
)


CUSTOMER_TYPE = "CUSTOMER"
COUNTERPARTY_TYPE = "COUNTERPARTY"

SUSPICIOUS_CUSTOMER_DECISIONS = frozenset({
    "MULE_LIKE",
})
NON_SUSPICIOUS_CUSTOMER_DECISIONS = frozenset({
    "EXPOSED_VULNERABLE",
    "LOW_CONCERN",
    "INSUFFICIENT_EVIDENCE",
})
SUSPICIOUS_COUNTERPARTY_DECISIONS = frozenset({
    "SUSPICIOUS_EXPAND",
})
LEGITIMATE_COUNTERPARTY_DECISIONS = frozenset({
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
})
UNCERTAIN_COUNTERPARTY_DECISIONS = frozenset({
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
})


@dataclass(frozen=True)
class AnalystNetworkNarrative:
    """Exact counts and prose describing one investigation."""

    linked_customer_count: int
    counterparty_count: int
    likely_mule_customer_count: int
    deterministic_customer_count: int
    non_suspicious_customer_count: int
    suspicious_counterparty_count: int
    legitimate_counterparty_count: int
    uncertain_counterparty_count: int
    summarized_customer_count: int
    visible_node_count: int
    visible_edge_count: int
    root_branch_count: int
    cross_link_count: int
    max_depth: int
    shape_label: str
    headline: str
    paragraphs: tuple[str, ...]


def _normalized_series(
    frame: pd.DataFrame,
    column: str,
) -> pd.Series:
    """Return one upper-case string column or an empty series."""
    if column not in frame.columns:
        return pd.Series(
            "",
            index=frame.index,
            dtype="string",
        )

    return (
        frame[column]
        .astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
    )


def _shape_label(
    *,
    visible_node_count: int,
    visible_edge_count: int,
    seed_count: int,
    root_branch_count: int,
    max_depth: int,
) -> tuple[str, int]:
    """Classify the visible network using transparent graph counts."""
    spanning_edge_count = max(
        0,
        visible_node_count - max(seed_count, 1),
    )
    cross_link_count = max(
        0,
        visible_edge_count - spanning_edge_count,
    )

    if cross_link_count and max_depth >= 2:
        return (
            "multi-layer interconnected network",
            cross_link_count,
        )

    if max_depth >= 3:
        return (
            "multi-layer branching network",
            cross_link_count,
        )

    if max_depth == 2:
        return (
            "two-layer branching network",
            cross_link_count,
        )

    if max_depth == 1 and root_branch_count >= 3:
        return (
            "hub-and-spoke network",
            cross_link_count,
        )

    return (
        "compact linked group",
        cross_link_count,
    )


def build_analyst_network_narrative(
    investigation: AnalystInvestigationView,
) -> AnalystNetworkNarrative:
    """Build a factual analyst narrative from persisted graph results."""
    nodes = investigation.nodes.copy()
    edges = investigation.edges.copy()

    node_types = _normalized_series(
        nodes,
        "node_type",
    )
    categories = _normalized_series(
        nodes,
        "decision_category",
    )
    decisions = _normalized_series(
        nodes,
        "ai_decision",
    )
    seed_flags = (
        nodes["is_seed"].fillna(False).astype(bool)
        if "is_seed" in nodes.columns
        else categories.eq("SEED")
    )
    depths = (
        pd.to_numeric(
            nodes["depth"],
            errors="coerce",
        ).fillna(0).astype(int)
        if "depth" in nodes.columns
        else pd.Series(
            0,
            index=nodes.index,
            dtype="int64",
        )
    )

    customer_mask = node_types.eq(CUSTOMER_TYPE)
    counterparty_mask = node_types.eq(
        COUNTERPARTY_TYPE
    )
    non_seed_customer_mask = (
        customer_mask & ~seed_flags
    )

    likely_mule_customer_count = int(
        (
            non_seed_customer_mask
            & decisions.isin(
                SUSPICIOUS_CUSTOMER_DECISIONS
            )
        ).sum()
    )
    deterministic_customer_count = int(
        (
            non_seed_customer_mask
            & categories.eq("DETERMINISTIC")
        ).sum()
    )
    non_suspicious_customer_count = int(
        (
            non_seed_customer_mask
            & decisions.isin(
                NON_SUSPICIOUS_CUSTOMER_DECISIONS
            )
        ).sum()
    )

    suspicious_counterparty_count = int(
        (
            counterparty_mask
            & decisions.isin(
                SUSPICIOUS_COUNTERPARTY_DECISIONS
            )
        ).sum()
    )
    legitimate_counterparty_count = int(
        (
            counterparty_mask
            & decisions.isin(
                LEGITIMATE_COUNTERPARTY_DECISIONS
            )
        ).sum()
    )
    uncertain_counterparty_count = int(
        (
            counterparty_mask
            & decisions.isin(
                UNCERTAIN_COUNTERPARTY_DECISIONS
            )
        ).sum()
    )

    summarized_customer_count = int(
        investigation.collapsed_customer_count
    )
    visible_linked_customer_count = int(
        non_seed_customer_mask.sum()
    )
    linked_customer_count = (
        visible_linked_customer_count
        + summarized_customer_count
    )
    counterparty_count = int(
        counterparty_mask.sum()
    )
    visible_node_count = len(nodes)
    visible_edge_count = len(edges)
    root_branch_count = int(
        depths.eq(1).sum()
    )

    shape_label, cross_link_count = (
        _shape_label(
            visible_node_count=visible_node_count,
            visible_edge_count=visible_edge_count,
            seed_count=int(seed_flags.sum()),
            root_branch_count=root_branch_count,
            max_depth=int(investigation.max_depth),
        )
    )

    final_mule_count = (
        likely_mule_customer_count
        + deterministic_customer_count
    )

    headline = (
        f"{linked_customer_count} linked customers "
        f"across a {shape_label}"
    )

    paragraphs = (
        (
            f"The confirmed seed is connected to "
            f"{linked_customer_count} other customers "
            f"and {counterparty_count} counterparties."
        ),
        (
            f"The investigation identified "
            f"{final_mule_count} customer"
            f"{'' if final_mule_count == 1 else 's'} "
            f"as mule-related: "
            f"{likely_mule_customer_count} through AI "
            f"assessment and "
            f"{deterministic_customer_count} through "
            f"deterministic identity links. "
            f"{non_suspicious_customer_count} visible "
            f"customer"
            f"{'' if non_suspicious_customer_count == 1 else 's'} "
            f"were assessed as non-mule or potentially exposed."
        ),
        (
            f"The AI expanded "
            f"{suspicious_counterparty_count} suspicious "
            f"counterparty path"
            f"{'' if suspicious_counterparty_count == 1 else 's'} "
            f"and stopped at "
            f"{legitimate_counterparty_count} legitimate or "
            f"common "
            f"{'counterparty' if legitimate_counterparty_count == 1 else 'counterparties'}"
            f"{'.' if uncertain_counterparty_count == 0 else ';'}"
            + (
                ""
                if uncertain_counterparty_count == 0
                else (
                    f" {uncertain_counterparty_count} additional "
                    f"counterparty path"
                    f"{'' if uncertain_counterparty_count == 1 else 's'} "
                    f"stopped because the evidence was insufficient."
                )
            )
        ),
        (
            f"{summarized_customer_count} customer"
            f"{'' if summarized_customer_count == 1 else 's'} "
            f"linked only through stopped counterparty branches "
            f"were summarized rather than drawn individually. "
            f"The visible network reaches "
            f"{investigation.max_depth} level"
            f"{'' if investigation.max_depth == 1 else 's'} "
            f"from the seed and is classified as a "
            f"{shape_label}."
        ),
    )

    return AnalystNetworkNarrative(
        linked_customer_count=linked_customer_count,
        counterparty_count=counterparty_count,
        likely_mule_customer_count=(
            likely_mule_customer_count
        ),
        deterministic_customer_count=(
            deterministic_customer_count
        ),
        non_suspicious_customer_count=(
            non_suspicious_customer_count
        ),
        suspicious_counterparty_count=(
            suspicious_counterparty_count
        ),
        legitimate_counterparty_count=(
            legitimate_counterparty_count
        ),
        uncertain_counterparty_count=(
            uncertain_counterparty_count
        ),
        summarized_customer_count=(
            summarized_customer_count
        ),
        visible_node_count=visible_node_count,
        visible_edge_count=visible_edge_count,
        root_branch_count=root_branch_count,
        cross_link_count=cross_link_count,
        max_depth=int(investigation.max_depth),
        shape_label=shape_label,
        headline=headline,
        paragraphs=paragraphs,
    )
