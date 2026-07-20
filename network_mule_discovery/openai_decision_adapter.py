"""OpenAI-backed final counterparty and customer decisions."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

from openai import OpenAI

from network_mule_discovery.ai_decision_schemas import (
    CounterpartyDecisionAssessment,
    CustomerDecisionAssessment,
)
from network_mule_discovery.openai_config import (
    load_openai_settings,
)


COUNTERPARTY_SYSTEM_PROMPT = """
You are the final counterparty decision engine for a bank
mule-network discovery workflow.

Make the final decision without analyst intervention.

Use only the supplied evidence payload. Do not invent
transactions, identities, ownership, registry information,
customer behavior, or fraud outcomes.

A shared counterparty is candidate evidence only. Do not
approve expansion solely because multiple customers use the
same counterparty.

Decision definitions:

SUSPICIOUS_EXPAND:
The evidence supports treating the external counterparty as
a suspicious relationship and exposing linked customers for
customer-level assessment.

LEGITIMATE_SUPPRESS:
The evidence is more consistent with a plausible legitimate
commercial or personal relationship.

COMMON_PUBLIC_SUPPRESS:
The counterparty appears common, public, institutional, or
otherwise unsuitable for network expansion.

INSUFFICIENT_EVIDENCE_SUPPRESS:
The evidence is insufficient to justify network expansion.

Return a concise evidence-based rationale, a stable uppercase
reason code, one to eight key evidence statements, and a
confidence level.
""".strip()


CUSTOMER_SYSTEM_PROMPT = """
You are the final customer decision engine for a bank
mule-network discovery workflow.

Make the final decision without analyst intervention.

Use only the supplied evidence payload. Do not invent
transactions, identities, account behavior, registry
information, or fraud outcomes.

Decision definitions:

MULE_LIKE:
The evidence supports treating the customer as mule-like.
The customer may become a recursive relationship-expansion
source.

EXPOSED_VULNERABLE:
The customer appears exposed, manipulated, or vulnerable,
but the evidence does not support mule-like expansion.

LOW_CONCERN:
The evidence is more consistent with legitimate or
low-concern activity.

INSUFFICIENT_EVIDENCE:
The evidence is insufficient to classify the customer as
mule-like.

Return a concise evidence-based rationale, a stable uppercase
reason code, one to eight key evidence statements, and a
confidence level.
""".strip()


class OpenAIDecisionError(RuntimeError):
    """A live decision could not be completed safely."""

    def __init__(
        self,
        code: str,
        message: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _stable_id(
    prefix: str,
    *values: object,
) -> str:
    """Create a deterministic identifier."""
    canonical_value = "|".join(
        str(value)
        for value in values
    )

    digest = hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()[:16]

    return f"{prefix}{digest}"


def _extract_refusal(
    response: object,
) -> str | None:
    """Return a structured refusal, when present."""
    for output_item in (
        getattr(response, "output", None)
        or []
    ):
        if (
            getattr(output_item, "type", None)
            != "message"
        ):
            continue

        for content_item in (
            getattr(output_item, "content", None)
            or []
        ):
            if (
                getattr(content_item, "type", None)
                != "refusal"
            ):
                continue

            refusal = str(
                getattr(
                    content_item,
                    "refusal",
                    "",
                )
            ).strip()

            return refusal or "Model refusal"

    return None


def _validate_feature_payload(
    *,
    subject_type: str,
    subject_key: str,
    feature_snapshot_hash: str,
    feature_payload_json: str,
) -> dict[str, object]:
    """Validate the exact payload represented by the hash."""
    calculated_hash = hashlib.sha256(
        feature_payload_json.encode("utf-8")
    ).hexdigest()

    if calculated_hash != feature_snapshot_hash:
        raise OpenAIDecisionError(
            "EVIDENCE_HASH_MISMATCH",
            "The evidence payload does not match "
            "feature_snapshot_hash.",
        )

    try:
        payload = json.loads(
            feature_payload_json
        )

    except json.JSONDecodeError as exc:
        raise OpenAIDecisionError(
            "INVALID_EVIDENCE_JSON",
            "The feature payload is not valid JSON.",
        ) from exc

    if not isinstance(payload, dict):
        raise OpenAIDecisionError(
            "INVALID_EVIDENCE_PAYLOAD",
            "The feature payload must be a JSON object.",
        )

    if payload.get("subject_type") != subject_type:
        raise OpenAIDecisionError(
            "EVIDENCE_SUBJECT_TYPE_MISMATCH",
            "The evidence subject type does not match "
            "the queued subject.",
        )

    if payload.get("subject_key") != subject_key:
        raise OpenAIDecisionError(
            "EVIDENCE_SUBJECT_KEY_MISMATCH",
            "The evidence subject key does not match "
            "the queued subject.",
        )

    relationships = payload.get(
        "relationships"
    )

    if not isinstance(relationships, list):
        raise OpenAIDecisionError(
            "INVALID_RELATIONSHIP_EVIDENCE",
            "The evidence payload must contain a "
            "relationships list.",
        )

    return payload


class OpenAIDecisionAdapter:
    """Make schema-constrained decisions through OpenAI."""

    def __init__(
        self,
        *,
        client: Any,
        model: str,
        prompt_version: str,
        max_output_tokens: int,
    ) -> None:
        self.client = client
        self.model = model
        self.prompt_version = prompt_version
        self.max_output_tokens = (
            max_output_tokens
        )

        self.last_call_metadata: dict[
            str,
            object,
        ] | None = None

    @classmethod
    def from_environment(
        cls,
    ) -> "OpenAIDecisionAdapter":
        """Create an adapter from environment settings."""
        settings = load_openai_settings()

        client = OpenAI(
            api_key=settings.api_key,
            timeout=settings.timeout_seconds,
            max_retries=0,
        )

        return cls(
            client=client,
            model=settings.model,
            prompt_version=(
                settings.prompt_version
            ),
            max_output_tokens=(
                settings.max_output_tokens
            ),
        )

    def decide(
        self,
        *,
        subject_type: str,
        subject_key: str,
        feature_snapshot_hash: str,
        feature_payload_json: str,
        run_date: date,
        round_number: int,
        sequence_number: int,
    ) -> dict[str, str]:
        """Return one final structured decision."""
        self.last_call_metadata = None

        evidence_payload = (
            _validate_feature_payload(
                subject_type=subject_type,
                subject_key=subject_key,
                feature_snapshot_hash=(
                    feature_snapshot_hash
                ),
                feature_payload_json=(
                    feature_payload_json
                ),
            )
        )

        if subject_type == "COUNTERPARTY":
            response_schema = (
                CounterpartyDecisionAssessment
            )

            system_prompt = (
                COUNTERPARTY_SYSTEM_PROMPT
            )

        elif subject_type == "CUSTOMER":
            response_schema = (
                CustomerDecisionAssessment
            )

            system_prompt = (
                CUSTOMER_SYSTEM_PROMPT
            )

        else:
            raise OpenAIDecisionError(
                "UNSUPPORTED_SUBJECT_TYPE",
                f"Unsupported subject type: "
                f"{subject_type}",
            )

        request_payload = {
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "run_date": str(run_date),
            "evidence": evidence_payload,
        }

        try:
            response = self.client.responses.parse(
                model=self.model,
                instructions=system_prompt,
                input=json.dumps(
                    request_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                text_format=response_schema,
                max_output_tokens=(
                    self.max_output_tokens
                ),
            )

        except Exception as exc:
            raise OpenAIDecisionError(
                "AI_API_OR_PARSE_ERROR",
                f"{type(exc).__name__}: {exc}",
            ) from exc

        response_status = getattr(
            response,
            "status",
            None,
        )

        if response_status in {
            "failed",
            "incomplete",
            "cancelled",
        }:
            raise OpenAIDecisionError(
                "AI_INCOMPLETE_RESPONSE",
                f"Response status: {response_status}",
            )

        refusal = _extract_refusal(
            response
        )

        if refusal:
            raise OpenAIDecisionError(
                "AI_REFUSAL",
                refusal,
            )

        parsed = getattr(
            response,
            "output_parsed",
            None,
        )

        if parsed is None:
            raise OpenAIDecisionError(
                "AI_PARSE_FAILURE",
                "The response contained no parsed "
                "structured decision.",
            )

        if not isinstance(
            parsed,
            response_schema,
        ):
            try:
                parsed = (
                    response_schema
                    .model_validate(parsed)
                )

            except Exception as exc:
                raise OpenAIDecisionError(
                    "AI_SCHEMA_FAILURE",
                    "The parsed output did not match "
                    "the required schema.",
                ) from exc

        decision_version = (
            f"{self.prompt_version}:"
            f"{self.model}"
        )

        decision_id = _stable_id(
            "OD",
            subject_type,
            subject_key,
            feature_snapshot_hash,
            parsed.decision,
            decision_version,
        )

        decided_at = datetime.now(
            timezone.utc
        ).isoformat()

        self.last_call_metadata = {
            "response_id": getattr(
                response,
                "id",
                None,
            ),
            "request_id": getattr(
                response,
                "_request_id",
                None,
            ),
            "model": self.model,
            "prompt_version": (
                self.prompt_version
            ),
            "subject_type": subject_type,
            "subject_key": subject_key,
            "round_number": round_number,
            "sequence_number": sequence_number,
            "assessment": (
                parsed.model_dump()
            ),
        }

        return {
            "decision_id": decision_id,
            "subject_type": subject_type,
            "subject_key": subject_key,
            "feature_snapshot_hash": (
                feature_snapshot_hash
            ),
            "decision": parsed.decision,
            "reason_code": (
                parsed.reason_code
            ),
            "decision_version": (
                decision_version
            ),
            "decided_at": decided_at,
            "source": "OPENAI_RESPONSES_API",
        }
