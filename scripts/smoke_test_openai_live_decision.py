"""Make one controlled live structured counterparty decision."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.openai_decision_adapter import (
    OpenAIDecisionAdapter,
)


VALID_COUNTERPARTY_DECISIONS = {
    "SUSPICIOUS_EXPAND",
    "LEGITIMATE_SUPPRESS",
    "COMMON_PUBLIC_SUPPRESS",
    "INSUFFICIENT_EVIDENCE_SUPPRESS",
}


def main() -> None:
    """Run one live decision using synthetic evidence only."""
    live_test_enabled = os.getenv(
        "ALLOW_LIVE_OPENAI_SMOKE_TEST",
        "",
    ).strip()

    if live_test_enabled != "1":
        raise SystemExit(
            "Live OpenAI smoke test blocked. Set "
            "ALLOW_LIVE_OPENAI_SMOKE_TEST=1 explicitly "
            "to authorize one billable synthetic request."
        )

    subject_key = (
        "LOCAL_ACCOUNT|SYNTHETIC9001"
    )

    payload = {
        "subject_type": "COUNTERPARTY",
        "subject_key": subject_key,
        "nodes": [
            {
                "node_key": (
                    "COUNTERPARTY|"
                    f"{subject_key}"
                ),
                "node_type": "COUNTERPARTY",
                "entity_type": None,
                "entity_id": None,
                "entity_key": None,
                "counterparty_key": subject_key,
                "display_label": (
                    "Synthetic External Account"
                ),
                "node_roles": (
                    "EXTERNAL_COUNTERPARTY_CANDIDATE"
                ),
            }
        ],
        "relationships": [
            {
                "source_node_key": (
                    "CUSTOMER|RETAIL|SYNTHETIC_SEED"
                ),
                "target_node_key": (
                    "COUNTERPARTY|"
                    f"{subject_key}"
                ),
                "edge_type": (
                    "SEED_COUNTERPARTY_EVIDENCE"
                ),
                "evidence_key": (
                    "SYNTHETIC_SOURCE_ACTIVITY"
                ),
                "evidence_summary": (
                    "Five transfers from the synthetic "
                    "seed to the external account"
                ),
                "source_event_count": 5,
                "candidate_event_count": 3,
            },
            {
                "source_node_key": (
                    "COUNTERPARTY|"
                    f"{subject_key}"
                ),
                "target_node_key": (
                    "CUSTOMER|RETAIL|SYNTHETIC_LINKED"
                ),
                "edge_type": (
                    "SHARED_EXTERNAL_COUNTERPARTY"
                ),
                "evidence_key": (
                    "SYNTHETIC_SHARED_ACTIVITY"
                ),
                "evidence_summary": (
                    "The same external account is used "
                    "by another synthetic customer"
                ),
                "source_event_count": 5,
                "candidate_event_count": 3,
            },
        ],
    }

    feature_payload_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    feature_snapshot_hash = hashlib.sha256(
        feature_payload_json.encode("utf-8")
    ).hexdigest()

    adapter = (
        OpenAIDecisionAdapter
        .from_environment()
    )

    decision = adapter.decide(
        subject_type="COUNTERPARTY",
        subject_key=subject_key,
        feature_snapshot_hash=(
            feature_snapshot_hash
        ),
        feature_payload_json=(
            feature_payload_json
        ),
        run_date=date.today(),
        round_number=1,
        sequence_number=1,
    )

    assert (
        decision["decision"]
        in VALID_COUNTERPARTY_DECISIONS
    )

    assert (
        decision["subject_type"]
        == "COUNTERPARTY"
    )

    assert (
        decision["subject_key"]
        == subject_key
    )

    assert (
        decision["feature_snapshot_hash"]
        == feature_snapshot_hash
    )

    assert (
        decision["source"]
        == "OPENAI_RESPONSES_API"
    )

    metadata = (
        adapter.last_call_metadata
        or {}
    )

    assessment = metadata.get(
        "assessment",
        {},
    )

    assert metadata.get("response_id")
    assert assessment.get("rationale")
    assert assessment.get("key_evidence")

    print(
        "OpenAI live structured decision "
        "smoke test passed."
    )

    print(
        f"Model: {metadata.get('model')}"
    )

    print(
        "Response ID present: "
        f"{bool(metadata.get('response_id'))}"
    )

    print(
        "Request ID present: "
        f"{bool(metadata.get('request_id'))}"
    )

    print(
        f"Decision: {decision['decision']}"
    )

    print(
        f"Reason code: "
        f"{decision['reason_code']}"
    )

    print(
        f"Confidence: "
        f"{assessment.get('confidence')}"
    )

    print(
        "Key evidence items: "
        f"{len(assessment.get('key_evidence', []))}"
    )

    print(
        "Synthetic evidence only: passed"
    )


if __name__ == "__main__":
    main()
