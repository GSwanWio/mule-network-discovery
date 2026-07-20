"""Strict structured outputs for AI network decisions."""

from __future__ import annotations

from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)


ConfidenceLevel = Literal[
    "LOW",
    "MEDIUM",
    "HIGH",
]

CounterpartyDecision = Literal[
    "SUSPICIOUS_EXPAND",
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
]

CustomerDecision = Literal[
    "MULE_LIKE",
    "EXPOSED_VULNERABLE",
    "LOW_CONCERN",
    "INSUFFICIENT_EVIDENCE",
]


class DecisionAssessmentBase(BaseModel):
    """Common fields required for every AI decision."""

    model_config = ConfigDict(
        extra="forbid",
    )

    reason_code: str = Field(
        min_length=3,
        max_length=80,
        pattern=r"^[A-Z0-9_]+$",
    )

    rationale: str = Field(
        min_length=10,
        max_length=1200,
    )

    key_evidence: list[str] = Field(
        min_length=1,
        max_length=8,
    )

    confidence: ConfidenceLevel

    @field_validator(
        "reason_code",
        mode="before",
    )
    @classmethod
    def normalize_reason_code(
        cls,
        value: object,
    ) -> str:
        """Normalize reason codes to stable identifiers."""
        normalized = str(value).strip().upper()

        if not normalized:
            raise ValueError(
                "reason_code cannot be blank."
            )

        return normalized

    @field_validator("rationale")
    @classmethod
    def normalize_rationale(
        cls,
        value: str,
    ) -> str:
        """Remove surrounding whitespace."""
        return value.strip()

    @field_validator("key_evidence")
    @classmethod
    def normalize_key_evidence(
        cls,
        values: list[str],
    ) -> list[str]:
        """Require at least one nonblank evidence item."""
        normalized = [
            str(value).strip()
            for value in values
            if str(value).strip()
        ]

        if not normalized:
            raise ValueError(
                "At least one evidence item is required."
            )

        return normalized


class CounterpartyDecisionAssessment(
    DecisionAssessmentBase
):
    """Final counterparty expansion decision."""

    decision: CounterpartyDecision


class CustomerDecisionAssessment(
    DecisionAssessmentBase
):
    """Final customer mule-likeness decision."""

    decision: CustomerDecision
