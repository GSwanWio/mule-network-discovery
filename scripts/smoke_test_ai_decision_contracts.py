"""Offline tests for AI schemas and configuration."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pydantic import ValidationError


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.ai_decision_schemas import (
    CounterpartyDecisionAssessment,
    CustomerDecisionAssessment,
)
from network_mule_discovery.openai_config import (
    load_openai_settings,
)


def main() -> None:
    """Validate decision schemas without calling the API."""
    counterparty = CounterpartyDecisionAssessment(
        decision="SUSPICIOUS_EXPAND",
        reason_code="unexplained_shared_activity",
        rationale=(
            "The supplied evidence supports exposing "
            "linked customers for further assessment."
        ),
        key_evidence=[
            "Shared by multiple customers",
            "Repeated transaction evidence",
        ],
        confidence="HIGH",
    )

    assert (
        counterparty.reason_code
        == "UNEXPLAINED_SHARED_ACTIVITY"
    )

    customer = CustomerDecisionAssessment(
        decision="LOW_CONCERN",
        reason_code="limited_supporting_evidence",
        rationale=(
            "The supplied evidence does not support "
            "a mule-like customer classification."
        ),
        key_evidence=[
            "Only one relationship type is present",
        ],
        confidence="MEDIUM",
    )

    assert (
        customer.reason_code
        == "LIMITED_SUPPORTING_EVIDENCE"
    )

    try:
        CounterpartyDecisionAssessment(
            decision="APPROVE_EVERYTHING",
            reason_code="INVALID_DECISION",
            rationale=(
                "This deliberately uses an invalid "
                "decision value for schema testing."
            ),
            key_evidence=["Synthetic test"],
            confidence="HIGH",
        )

    except ValidationError:
        pass

    else:
        raise AssertionError(
            "Invalid counterparty decision was accepted."
        )

    try:
        CustomerDecisionAssessment(
            decision="MULE_LIKE",
            reason_code="VALID_REASON",
            rationale="Too short",
            key_evidence=[],
            confidence="HIGH",
        )

    except ValidationError:
        pass

    else:
        raise AssertionError(
            "Invalid evidence output was accepted."
        )

    settings = load_openai_settings()

    assert settings.api_key
    assert settings.model
    assert settings.prompt_version
    assert settings.timeout_seconds > 0
    assert settings.max_output_tokens > 0

    print(
        "AI decision contracts smoke test passed."
    )

    print(
        "Counterparty decision schema: passed"
    )

    print(
        "Customer decision schema: passed"
    )

    print(
        "Invalid decision rejection: passed"
    )

    print(
        "Invalid evidence rejection: passed"
    )

    print(
        "OPENAI_API_KEY available: passed"
    )

    print(
        f"Configured model: {settings.model}"
    )

    print(
        "Live API calls made: 0"
    )


if __name__ == "__main__":
    main()
