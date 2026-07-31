"""Validate deterministic breadth-first phase selection."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_orchestrator import (
    select_next_frontier_action,
)


def queue(
    *records: tuple[str, str],
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "action_type": action_type,
                "subject_key": subject_key,
            }
            for action_type, subject_key in records
        ],
        columns=[
            "action_type",
            "subject_key",
        ],
    )


def main() -> None:
    counterparty = select_next_frontier_action(
        actionable_queue=queue(
            ("RUN_COUNTERPARTY_AI", "CP2"),
            ("RUN_COUNTERPARTY_AI", "CP1"),
        ),
        failed_closed_item_count=0,
    )
    assert counterparty.action_type == "RUN_COUNTERPARTY_AI"
    assert counterparty.subject_keys == ("CP1", "CP2")

    customer = select_next_frontier_action(
        actionable_queue=queue(
            ("RUN_CUSTOMER_AI", "RETAIL|R1"),
        ),
        failed_closed_item_count=0,
    )
    assert customer.action_type == "RUN_CUSTOMER_AI"

    discovery = select_next_frontier_action(
        actionable_queue=queue(
            (
                "DISCOVER_CUSTOMER_RELATIONSHIPS",
                "RETAIL|R2",
            ),
        ),
        failed_closed_item_count=0,
    )
    assert discovery.subject_keys == ("RETAIL|R2",)

    termination = select_next_frontier_action(
        actionable_queue=queue(),
        failed_closed_item_count=0,
    )
    assert termination.action_type == "TERMINATE_FRONTIER"

    failed = select_next_frontier_action(
        actionable_queue=queue(),
        failed_closed_item_count=1,
    )
    assert failed.action_type == "FAIL_CLOSED"

    try:
        select_next_frontier_action(
            actionable_queue=queue(
                ("RUN_COUNTERPARTY_AI", "CP1"),
                ("RUN_CUSTOMER_AI", "RETAIL|R1"),
            ),
            failed_closed_item_count=0,
        )
    except Exception as exc:
        assert "mixed breadth-first phases" in str(exc)
    else:
        raise AssertionError(
            "Mixed frontier phases were not rejected."
        )

    print("Daily frontier selection smoke test passed.")
    print("Counterparty phase selection: passed")
    print("Customer phase selection: passed")
    print("Recursive discovery selection: passed")
    print("Direct termination selection: passed")
    print("Failed-closed selection: passed")
    print("Mixed phase rejection: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
