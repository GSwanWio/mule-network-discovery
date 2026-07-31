"""Validate the bounded breadth-first result contract."""

from __future__ import annotations

import sys
from dataclasses import fields
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_orchestrator import (
    DailyBreadthFirstRunResult,
    DailyBreadthFirstSettings,
    DailyBreadthFirstStepResult,
)
from network_mule_discovery.source_contracts import (
    SourceContractError,
)


def main() -> None:
    settings = DailyBreadthFirstSettings(
        max_frontier_steps=10
    )

    assert settings.max_frontier_steps == 10

    for invalid_value in (0, -1, True):
        try:
            DailyBreadthFirstSettings(
                max_frontier_steps=invalid_value
            )
        except SourceContractError:
            pass
        else:
            raise AssertionError(
                "Invalid frontier limit was accepted."
            )

    assert [
        field.name
        for field in fields(
            DailyBreadthFirstStepResult
        )
    ] == [
        "step_number",
        "selection",
        "recursive_counterparty",
        "recursive_customer",
    ]

    assert [
        field.name
        for field in fields(
            DailyBreadthFirstRunResult
        )
    ] == [
        "customer_phase",
        "steps",
        "supplemental_subject_payloads",
        "final_plan",
        "termination_status",
        "termination_reason",
        "recursive_termination",
        "frontier_termination",
    ]

    print(
        "Daily breadth-first contract smoke test passed."
    )
    print("Positive step limit: passed")
    print("Invalid step limits rejected: passed")
    print("Per-step result contract: passed")
    print("Final run result contract: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
