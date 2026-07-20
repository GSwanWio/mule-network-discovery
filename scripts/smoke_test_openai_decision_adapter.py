"""Offline smoke tests for the OpenAI decision adapter."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.ai_decision_schemas import (
    CounterpartyDecisionAssessment,
    CustomerDecisionAssessment,
)
from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionAdapter,
    OpenAIDecisionError,
)


class FakeResponses:
    """Return a configured fake Responses result."""

    def __init__(
        self,
        *,
        parsed: object | None = None,
        refusal: str | None = None,
        status: str = "completed",
        error: Exception | None = None,
    ) -> None:
        self.parsed = parsed
        self.refusal = refusal
        self.status = status
        self.error = error
        self.calls: list[
            dict[str, object]
        ] = []

    def parse(
        self,
        **kwargs: object,
    ) -> object:
        """Return or raise the configured response."""
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        output = []

        if self.refusal is not None:
            output = [
                SimpleNamespace(
                    type="message",
                    content=[
                        SimpleNamespace(
                            type="refusal",
                            refusal=self.refusal,
                        )
                    ],
                )
            ]

        return SimpleNamespace(
            id="resp_test_123",
            _request_id="req_test_123",
            status=self.status,
            output_parsed=self.parsed,
            output=output,
        )


class FakeClient:
    """Expose a fake Responses client."""

    def __init__(
        self,
        responses: FakeResponses,
    ) -> None:
        self.responses = responses


def build_payload(
    *,
    subject_type: str,
    subject_key: str,
) -> tuple[str, str]:
    """Create one canonical evidence payload."""
    payload = {
        "subject_type": subject_type,
        "subject_key": subject_key,
        "nodes": [
            {
                "node_key": (
                    f"{subject_type}|{subject_key}"
                ),
                "node_type": subject_type,
                "node_roles": "TEST_SUBJECT",
            }
        ],
        "relationships": [
            {
                "source_node_key": (
                    "CUSTOMER|RETAIL|TEST001"
                ),
                "target_node_key": (
                    f"{subject_type}|{subject_key}"
                ),
                "edge_type": (
                    "SHARED_EXTERNAL_COUNTERPARTY"
                ),
                "evidence_key": "TEST_EVIDENCE_1",
                "evidence_summary": (
                    "Synthetic shared relationship"
                ),
                "source_event_count": 3,
                "candidate_event_count": 2,
            }
        ],
    }

    payload_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    payload_hash = hashlib.sha256(
        payload_json.encode("utf-8")
    ).hexdigest()

    return payload_json, payload_hash


def assert_error_code(
    callable_object,
    expected_code: str,
) -> None:
    """Assert that an adapter call fails closed."""
    try:
        callable_object()

    except OpenAIDecisionError as exc:
        assert exc.code == expected_code

    else:
        raise AssertionError(
            "Expected OpenAIDecisionError."
        )


def main() -> None:
    """Validate success and fail-closed behavior."""
    counterparty_key = (
        "LOCAL_ACCOUNT|TEST9001"
    )

    (
        counterparty_payload,
        counterparty_hash,
    ) = build_payload(
        subject_type="COUNTERPARTY",
        subject_key=counterparty_key,
    )

    fake_counterparty_responses = (
        FakeResponses(
            parsed=(
                CounterpartyDecisionAssessment(
                    decision=(
                        "SUSPICIOUS_EXPAND"
                    ),
                    reason_code=(
                        "UNEXPLAINED_SHARED_ACTIVITY"
                    ),
                    rationale=(
                        "The evidence supports exposing "
                        "linked customers for assessment."
                    ),
                    key_evidence=[
                        "Three source events",
                        "Two candidate events",
                    ],
                    confidence="HIGH",
                )
            )
        )
    )

    adapter = OpenAIDecisionAdapter(
        client=FakeClient(
            fake_counterparty_responses
        ),
        model="test-model",
        prompt_version="test-v1",
        max_output_tokens=500,
    )

    decision = adapter.decide(
        subject_type="COUNTERPARTY",
        subject_key=counterparty_key,
        feature_snapshot_hash=(
            counterparty_hash
        ),
        feature_payload_json=(
            counterparty_payload
        ),
        run_date=date(2026, 7, 20),
        round_number=1,
        sequence_number=1,
    )

    assert (
        decision["decision"]
        == "SUSPICIOUS_EXPAND"
    )

    assert len(
        fake_counterparty_responses.calls
    ) == 1

    call = (
        fake_counterparty_responses
        .calls[0]
    )

    assert call["model"] == "test-model"
    assert call["text_format"] is (
        CounterpartyDecisionAssessment
    )

    assert (
        adapter.last_call_metadata[
            "response_id"
        ]
        == "resp_test_123"
    )

    assert (
        adapter.last_call_metadata[
            "request_id"
        ]
        == "req_test_123"
    )

    customer_key = "RETAIL|TEST2001"

    (
        customer_payload,
        customer_hash,
    ) = build_payload(
        subject_type="CUSTOMER",
        subject_key=customer_key,
    )

    customer_adapter = (
        OpenAIDecisionAdapter(
            client=FakeClient(
                FakeResponses(
                    parsed=(
                        CustomerDecisionAssessment(
                            decision="LOW_CONCERN",
                            reason_code=(
                                "LIMITED_SUPPORTING_EVIDENCE"
                            ),
                            rationale=(
                                "The evidence does not "
                                "support mule-like activity."
                            ),
                            key_evidence=[
                                "One relationship type"
                            ],
                            confidence="MEDIUM",
                        )
                    )
                )
            ),
            model="test-model",
            prompt_version="test-v1",
            max_output_tokens=500,
        )
    )

    customer_decision = (
        customer_adapter.decide(
            subject_type="CUSTOMER",
            subject_key=customer_key,
            feature_snapshot_hash=(
                customer_hash
            ),
            feature_payload_json=(
                customer_payload
            ),
            run_date=date(2026, 7, 20),
            round_number=1,
            sequence_number=2,
        )
    )

    assert (
        customer_decision["decision"]
        == "LOW_CONCERN"
    )

    refusal_adapter = OpenAIDecisionAdapter(
        client=FakeClient(
            FakeResponses(
                refusal="Synthetic refusal"
            )
        ),
        model="test-model",
        prompt_version="test-v1",
        max_output_tokens=500,
    )

    assert_error_code(
        lambda: refusal_adapter.decide(
            subject_type="COUNTERPARTY",
            subject_key=counterparty_key,
            feature_snapshot_hash=(
                counterparty_hash
            ),
            feature_payload_json=(
                counterparty_payload
            ),
            run_date=date(2026, 7, 20),
            round_number=1,
            sequence_number=1,
        ),
        "AI_REFUSAL",
    )

    incomplete_adapter = (
        OpenAIDecisionAdapter(
            client=FakeClient(
                FakeResponses(
                    status="incomplete"
                )
            ),
            model="test-model",
            prompt_version="test-v1",
            max_output_tokens=500,
        )
    )

    assert_error_code(
        lambda: incomplete_adapter.decide(
            subject_type="COUNTERPARTY",
            subject_key=counterparty_key,
            feature_snapshot_hash=(
                counterparty_hash
            ),
            feature_payload_json=(
                counterparty_payload
            ),
            run_date=date(2026, 7, 20),
            round_number=1,
            sequence_number=1,
        ),
        "AI_INCOMPLETE_RESPONSE",
    )

    error_adapter = OpenAIDecisionAdapter(
        client=FakeClient(
            FakeResponses(
                error=RuntimeError(
                    "Synthetic API failure"
                )
            )
        ),
        model="test-model",
        prompt_version="test-v1",
        max_output_tokens=500,
    )

    assert_error_code(
        lambda: error_adapter.decide(
            subject_type="COUNTERPARTY",
            subject_key=counterparty_key,
            feature_snapshot_hash=(
                counterparty_hash
            ),
            feature_payload_json=(
                counterparty_payload
            ),
            run_date=date(2026, 7, 20),
            round_number=1,
            sequence_number=1,
        ),
        "AI_API_OR_PARSE_ERROR",
    )

    assert_error_code(
        lambda: adapter.decide(
            subject_type="COUNTERPARTY",
            subject_key=counterparty_key,
            feature_snapshot_hash=(
                "0" * 64
            ),
            feature_payload_json=(
                counterparty_payload
            ),
            run_date=date(2026, 7, 20),
            round_number=1,
            sequence_number=1,
        ),
        "EVIDENCE_HASH_MISMATCH",
    )

    print(
        "OpenAI decision adapter offline "
        "smoke test passed."
    )

    print(
        "Counterparty structured output: passed"
    )

    print(
        "Customer structured output: passed"
    )

    print("Refusal handling: passed")
    print("Incomplete response handling: passed")
    print("API failure handling: passed")
    print("Evidence hash protection: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
