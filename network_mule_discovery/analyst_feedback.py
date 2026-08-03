"""Append-only analyst feedback for persisted AI decisions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ANALYST_FEEDBACK_FILENAME = "analyst_feedback.csv"

ANALYST_FEEDBACK_VALUES = frozenset({
    "AI_CORRECT",
    "AI_INCORRECT",
})

ANALYST_FEEDBACK_COLUMNS = (
    "feedback_id",
    "run_id",
    "group_id",
    "node_id",
    "subject_type",
    "subject_key",
    "ai_decision",
    "feedback",
    "analyst_notes",
    "analyst_id",
    "submitted_at",
)


class AnalystFeedbackError(RuntimeError):
    """Analyst feedback could not be persisted."""


@dataclass(frozen=True)
class AnalystFeedbackSubmission:
    """One immutable analyst review event."""

    feedback_id: str
    run_id: str
    group_id: str
    node_id: str
    subject_type: str
    subject_key: str
    ai_decision: str
    feedback: str
    analyst_notes: str
    analyst_id: str
    submitted_at: str


def _clean_text(value: object) -> str:
    """Return normalized text."""
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _submitted_timestamp(
    value: str | datetime | None,
) -> str:
    """Return a normalized UTC submission timestamp."""
    if value is None:
        resolved = datetime.now(
            timezone.utc
        )

    elif isinstance(value, datetime):
        resolved = value

        if resolved.tzinfo is None:
            resolved = resolved.replace(
                tzinfo=timezone.utc
            )
        else:
            resolved = resolved.astimezone(
                timezone.utc
            )

    else:
        text = _clean_text(value)

        if not text:
            raise AnalystFeedbackError(
                "submitted_at cannot be empty."
            )

        return text

    return (
        resolved.isoformat(
            timespec="microseconds"
        )
        .replace("+00:00", "Z")
    )


class CsvAnalystFeedbackStore:
    """CSV-backed append-only analyst feedback store."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state_directory = Path(
            state_directory
        )
        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.path = (
            self.state_directory
            / ANALYST_FEEDBACK_FILENAME
        )

    def load(self) -> pd.DataFrame:
        """Load all analyst feedback events."""
        if not self.path.exists():
            return pd.DataFrame(
                columns=list(
                    ANALYST_FEEDBACK_COLUMNS
                )
            )

        frame = pd.read_csv(
            self.path,
            dtype="string",
            keep_default_na=False,
        )

        missing_columns = sorted(
            set(ANALYST_FEEDBACK_COLUMNS)
            - set(frame.columns)
        )

        if missing_columns:
            raise AnalystFeedbackError(
                "Analyst feedback state is missing "
                f"columns: {missing_columns}"
            )

        return frame[
            list(ANALYST_FEEDBACK_COLUMNS)
        ].copy()

    def submit(
        self,
        *,
        run_id: str,
        group_id: str,
        node_id: str,
        subject_type: str,
        subject_key: str,
        ai_decision: str,
        feedback: str,
        analyst_notes: str = "",
        analyst_id: str = "UNSPECIFIED",
        submitted_at: str | datetime | None = None,
    ) -> AnalystFeedbackSubmission:
        """Append one immutable analyst feedback event."""
        values = {
            "run_id": _clean_text(run_id),
            "group_id": _clean_text(group_id),
            "node_id": _clean_text(node_id),
            "subject_type": (
                _clean_text(
                    subject_type
                ).upper()
            ),
            "subject_key": _clean_text(
                subject_key
            ),
            "ai_decision": (
                _clean_text(
                    ai_decision
                ).upper()
            ),
            "feedback": (
                _clean_text(
                    feedback
                ).upper()
            ),
            "analyst_notes": _clean_text(
                analyst_notes
            ),
            "analyst_id": (
                _clean_text(analyst_id)
                or "UNSPECIFIED"
            ),
            "submitted_at": (
                _submitted_timestamp(
                    submitted_at
                )
            ),
        }

        required = (
            "run_id",
            "group_id",
            "node_id",
            "subject_type",
            "subject_key",
            "ai_decision",
        )

        missing_values = [
            field
            for field in required
            if not values[field]
        ]

        if missing_values:
            raise AnalystFeedbackError(
                "Analyst feedback is missing values: "
                f"{missing_values}"
            )

        if (
            values["feedback"]
            not in ANALYST_FEEDBACK_VALUES
        ):
            raise AnalystFeedbackError(
                "feedback must be AI_CORRECT or "
                "AI_INCORRECT."
            )

        canonical_key = "|".join(
            [
                values["run_id"],
                values["group_id"],
                values["node_id"],
                values["analyst_id"],
                values["feedback"],
                values["submitted_at"],
            ]
        )
        digest = hashlib.sha256(
            canonical_key.encode("utf-8")
        ).hexdigest()[:20]

        record = {
            "feedback_id": f"AF{digest}",
            **values,
        }

        existing = self.load()
        combined = pd.concat(
            [
                existing,
                pd.DataFrame(
                    [record],
                    columns=list(
                        ANALYST_FEEDBACK_COLUMNS
                    ),
                ),
            ],
            ignore_index=True,
        )

        if combined[
            "feedback_id"
        ].duplicated().any():
            raise AnalystFeedbackError(
                "Duplicate analyst feedback event."
            )

        combined = (
            combined.sort_values(
                by=[
                    "submitted_at",
                    "feedback_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        temporary_path = self.path.with_suffix(
            ".tmp"
        )

        combined.to_csv(
            temporary_path,
            index=False,
            lineterminator="\n",
        )
        temporary_path.replace(self.path)

        return AnalystFeedbackSubmission(
            **record
        )

    def latest_for_node(
        self,
        *,
        run_id: str,
        group_id: str,
        node_id: str,
    ) -> AnalystFeedbackSubmission | None:
        """Return the latest feedback for one node."""
        frame = self.load()

        matches = frame.loc[
            frame["run_id"].eq(
                _clean_text(run_id)
            )
            & frame["group_id"].eq(
                _clean_text(group_id)
            )
            & frame["node_id"].eq(
                _clean_text(node_id)
            )
        ]

        if matches.empty:
            return None

        record = (
            matches.sort_values(
                by=[
                    "submitted_at",
                    "feedback_id",
                ],
                kind="stable",
            )
            .iloc[-1]
            .to_dict()
        )

        return AnalystFeedbackSubmission(
            **{
                column: _clean_text(
                    record[column]
                )
                for column
                in ANALYST_FEEDBACK_COLUMNS
            }
        )
