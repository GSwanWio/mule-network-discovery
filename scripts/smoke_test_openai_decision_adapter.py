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
        raw_output: str | None = None,
        refusal: str | None = None,
        status: str = "completed",
        incomplete_reason: str | None = None,
        error: Exception | None = None,
    ) -> None:
        self.parsed = parsed
        self.raw_output = raw_output
        self.refusal = refusal
        self.status = status
        self.incomplete_reason = (
            incomplete_reason
        )
        self.error = error
        self.calls: list[
            dict[str, object]
        ] = []

    def create(
        self,
        **kwargs: object,
    ) -> object:
        """Return or raise the configured response."""
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        output = []
        output_text = ""

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

        else:
            if self.raw_output is not None:
                output_text = self.raw_output

            elif self.parsed is not None:
                output_text = (
                    self.parsed.model_dump_json()
                )

            if output_text:
                output = [
                    SimpleNamespace(
                        type="message",
                        content=[
                            SimpleNamespace(
                                type="output_text",
                                text=output_text,
                            )
                        ],
                    )
                ]

        incomplete_details = None

        if self.incomplete_reason:
            incomplete_details = (
                SimpleNamespace(
                    reason=(
                        self.incomplete_reason
                    )
                )
            )

        return SimpleNamespace(
            id="resp_test_123",
            _request_id="req_test_123",
            status=self.status,
            incomplete_details=(
                incomplete_details
            ),
            usage=SimpleNamespace(
                input_tokens=250,
                output_tokens=120,
                output_tokens_details=(
                    SimpleNamespace(
                        reasoning_tokens=40,
                    )
                ),
            ),
            output_text=output_text,
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

    assert call["reasoning"] == {
        "effort": "minimal",
    }

    assert (
        call["text"]["verbosity"]
        == "low"
    )

    format_config = call["text"][
        "format"
    ]

    assert (
        format_config["type"]
        == "json_schema"
    )

    assert (
        format_config["name"]
        == "CounterpartyDecisionAssessment"
    )

    assert format_config["strict"] is True

    assert (
        format_config["schema"]
        == CounterpartyDecisionAssessment
        .model_json_schema()
    )

    assert call["max_output_tokens"] == 500

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
                    status="incomplete",
                    incomplete_reason=(
                        "max_output_tokens"
                    ),
                    raw_output='{"decision":',
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
        "AI_OUTPUT_TRUNCATED",
    )

    try:
        incomplete_adapter.decide(
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

    except OpenAIDecisionError as exc:
        assert exc.response_id == (
            "resp_test_123"
        )
        assert exc.request_id == (
            "req_test_123"
        )
        assert exc.response_status == (
            "incomplete"
        )
        assert exc.incomplete_reason == (
            "max_output_tokens"
        )
        assert exc.input_tokens == "250"
        assert exc.output_tokens == "120"
        assert exc.reasoning_tokens == "40"

    else:
        raise AssertionError(
            "Expected traceable incomplete error."
        )

    malformed_adapter = OpenAIDecisionAdapter(
        client=FakeClient(
            FakeResponses(
                raw_output='{"decision":',
            )
        ),
        model="test-model",
        prompt_version="test-v1",
        max_output_tokens=500,
    )

    assert_error_code(
        lambda: malformed_adapter.decide(
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
        "AI_OUTPUT_TRUNCATED",
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
        "AI_API_ERROR",
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
    print("Response trace metadata: passed")
    print("Completed-output schema handling: passed")
    print("API failure handling: passed")
    print("Evidence hash protection: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
