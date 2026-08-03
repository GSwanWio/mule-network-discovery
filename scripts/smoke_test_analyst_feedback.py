"""Validate append-only analyst decision feedback."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_feedback import (
    AnalystFeedbackError,
    CsvAnalystFeedbackStore,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        store = CsvAnalystFeedbackStore(
            Path(directory)
        )

        assert store.load().empty

        first = store.submit(
            run_id="RUN001",
            group_id="G001",
            node_id="N_CP_1",
            subject_type="COUNTERPARTY",
            subject_key="LOCAL_ACCOUNT|9901",
            ai_decision="SUSPICIOUS_EXPAND",
            feedback="AI_CORRECT",
            analyst_notes=(
                "Decision matches the network evidence."
            ),
            analyst_id="ANALYST_1",
            submitted_at=(
                "2026-08-03T18:00:00Z"
            ),
        )

        revised = store.submit(
            run_id="RUN001",
            group_id="G001",
            node_id="N_CP_1",
            subject_type="COUNTERPARTY",
            subject_key="LOCAL_ACCOUNT|9901",
            ai_decision="SUSPICIOUS_EXPAND",
            feedback="AI_INCORRECT",
            analyst_notes=(
                "Supporting transaction context "
                "changes the assessment."
            ),
            analyst_id="ANALYST_1",
            submitted_at=(
                "2026-08-03T18:05:00Z"
            ),
        )

        assert (
            first.feedback_id
            != revised.feedback_id
        )

        history = store.load()

        assert len(history) == 2
        assert set(
            history["ai_decision"]
        ) == {"SUSPICIOUS_EXPAND"}

        latest = store.latest_for_node(
            run_id="RUN001",
            group_id="G001",
            node_id="N_CP_1",
        )

        assert latest is not None
        assert (
            latest.feedback
            == "AI_INCORRECT"
        )
        assert (
            latest.ai_decision
            == "SUSPICIOUS_EXPAND"
        )
        assert (
            latest.analyst_notes
            == (
                "Supporting transaction context "
                "changes the assessment."
            )
        )

        try:
            store.submit(
                run_id="RUN001",
                group_id="G001",
                node_id="N_CP_2",
                subject_type="COUNTERPARTY",
                subject_key="LOCAL_ACCOUNT|9902",
                ai_decision="COMMON_PUBLIC_SUPPRESS",
                feedback="OVERRIDE_DECISION",
            )
        except AnalystFeedbackError:
            invalid_feedback_rejected = True
        else:
            invalid_feedback_rejected = False

        assert invalid_feedback_rejected

        print(
            "Analyst feedback persistence "
            "smoke test passed."
        )
        print("Append-only feedback rows: 2")
        print("Revised feedback retained: passed")
        print("Latest node feedback loaded: passed")
        print("AI decision unchanged: passed")
        print("Invalid override rejected: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
