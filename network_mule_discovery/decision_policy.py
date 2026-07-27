"""Versioned AI assessment policies used in evidence and audit state."""

from __future__ import annotations


COUNTERPARTY_ASSESSMENT_POLICY_VERSION = (
    "counterparty-assessment-policy-v2"
)


def effective_prompt_version(
    *,
    subject_type: str,
    base_prompt_version: str,
) -> str:
    """Return the auditable prompt version for one subject type."""
    normalized_subject_type = subject_type.strip().upper()

    if normalized_subject_type == "COUNTERPARTY":
        return (
            f"{base_prompt_version}:"
            f"{COUNTERPARTY_ASSESSMENT_POLICY_VERSION}"
        )

    return base_prompt_version
