"""Explicit and auditable technical reprocessing of failed frontier work."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from network_mule_discovery.daily_state import CsvDailyStateStore


TECHNICAL_REPROCESSING_LEDGER_FILENAME = (
    "technical_reprocessing_ledger.csv"
)

TECHNICAL_REPROCESSING_LEDGER_COLUMNS = (
    "requeue_event_id",
    "requeue_request_id",
    "requested_at",
    "requested_by",
    "requeue_reason",
    "queue_item_id",
    "run_date",
    "action_type",
    "subject_type",
    "subject_key",
    "feature_snapshot_hash",
    "prior_queue_status",
    "prior_attempt_count",
    "prior_error_code",
    "prior_error_message",
    "prior_response_id",
    "prior_request_id",
    "resulting_queue_status",
)


class TechnicalReprocessingError(RuntimeError):
    """A technical requeue request violates a safety invariant."""


@dataclass(frozen=True)
class TechnicalRequeueResult:
    """Result from one explicit technical requeue request."""

    requeue_event_id: str
    requeue_request_id: str
    queue_item_id: str
    subject_type: str
    subject_key: str
    prior_attempt_count: int
    resulting_queue_status: str
    applied: bool
    already_recorded: bool


def _require_nonempty(
    *,
    value: object,
    label: str,
) -> str:
    resolved = str(value).strip()

    if not resolved:
        raise TechnicalReprocessingError(
            f"{label} cannot be empty."
        )

    return resolved


def _normalize_requested_at(
    requested_at: datetime | str,
) -> str:
    if isinstance(requested_at, datetime):
        parsed = requested_at
    else:
        raw = _require_nonempty(
            value=requested_at,
            label="requested_at",
        )

        try:
            parsed = datetime.fromisoformat(
                raw.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise TechnicalReprocessingError(
                "requested_at must be a valid ISO-8601 timestamp."
            ) from exc

    if parsed.tzinfo is None:
        raise TechnicalReprocessingError(
            "requested_at must include a timezone."
        )

    return parsed.astimezone(timezone.utc).isoformat()


def _stable_requeue_event_id(
    *,
    requeue_request_id: str,
    queue_item_id: str,
) -> str:
    digest = hashlib.sha256(
        "|".join(
            [
                requeue_request_id,
                queue_item_id,
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"TR{digest}"


class CsvTechnicalReprocessingLedger:
    """CSV-backed audit ledger for explicit technical requeues."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state_directory = Path(state_directory)
        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.path = (
            self.state_directory
            / TECHNICAL_REPROCESSING_LEDGER_FILENAME
        )

    def load(self) -> pd.DataFrame:
        """Load all technical requeue audit rows."""
        if not self.path.exists():
            return pd.DataFrame(
                columns=list(
                    TECHNICAL_REPROCESSING_LEDGER_COLUMNS
                )
            )

        frame = pd.read_csv(
            self.path,
            dtype="string",
            keep_default_na=False,
        )

        for column in TECHNICAL_REPROCESSING_LEDGER_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""

        return frame[
            list(TECHNICAL_REPROCESSING_LEDGER_COLUMNS)
        ]

    def existing_request(
        self,
        *,
        requeue_event_id: str,
    ) -> pd.DataFrame:
        """Return a prior row for one deterministic request."""
        ledger = self.load()

        return ledger.loc[
            ledger["requeue_event_id"].eq(
                requeue_event_id
            )
        ].copy()

    def assert_can_append(
        self,
        *,
        record: dict[str, str],
    ) -> None:
        """Reject conflicting or duplicate requeue attempts."""
        ledger = self.load()

        event_matches = ledger.loc[
            ledger["requeue_event_id"].eq(
                record["requeue_event_id"]
            )
        ]

        if not event_matches.empty:
            comparable_columns = [
                "requeue_request_id",
                "queue_item_id",
                "prior_attempt_count",
            ]

            prior = event_matches.iloc[-1]

            if any(
                str(prior[column])
                != str(record[column])
                for column in comparable_columns
            ):
                raise TechnicalReprocessingError(
                    "A requeue event ID conflicts with an existing "
                    "audit row."
                )

            return

        same_request_id = ledger.loc[
            ledger["requeue_request_id"].eq(
                record["requeue_request_id"]
            )
        ]

        if not same_request_id.empty:
            raise TechnicalReprocessingError(
                "The requeue request ID was already used for another "
                "audit event."
            )

        same_failure_attempt = ledger.loc[
            ledger["queue_item_id"].eq(
                record["queue_item_id"]
            )
            & ledger["prior_attempt_count"].eq(
                record["prior_attempt_count"]
            )
        ]

        if not same_failure_attempt.empty:
            raise TechnicalReprocessingError(
                "The failed queue attempt was already explicitly "
                "requeued."
            )

    def append(
        self,
        *,
        record: dict[str, str],
    ) -> pd.DataFrame:
        """Append one validated audit row idempotently."""
        self.assert_can_append(record=record)

        existing = self.existing_request(
            requeue_event_id=record["requeue_event_id"]
        )

        if not existing.empty:
            return self.load()

        combined = pd.concat(
            [
                self.load(),
                pd.DataFrame(
                    [record],
                    columns=list(
                        TECHNICAL_REPROCESSING_LEDGER_COLUMNS
                    ),
                ),
            ],
            ignore_index=True,
        )

        combined = (
            combined
            .drop_duplicates(
                subset=["requeue_event_id"],
                keep="last",
            )
            .sort_values(
                by=[
                    "requested_at",
                    "requeue_event_id",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

        combined.to_csv(
            self.path,
            index=False,
            lineterminator="\n",
        )

        return self.load()


def requeue_failed_frontier_item(
    *,
    state_directory: Path | str,
    queue_item_id: str,
    requeue_request_id: str,
    requested_at: datetime | str,
    requested_by: str,
    requeue_reason: str,
    expected_attempt_count: int | None = None,
) -> TechnicalRequeueResult:
    """Move exactly one failed-closed frontier item back to READY."""
    resolved_queue_item_id = _require_nonempty(
        value=queue_item_id,
        label="queue_item_id",
    )
    resolved_request_id = _require_nonempty(
        value=requeue_request_id,
        label="requeue_request_id",
    )
    resolved_requested_by = _require_nonempty(
        value=requested_by,
        label="requested_by",
    )
    resolved_reason = _require_nonempty(
        value=requeue_reason,
        label="requeue_reason",
    )
    resolved_requested_at = _normalize_requested_at(
        requested_at
    )

    if (
        expected_attempt_count is not None
        and expected_attempt_count < 1
    ):
        raise TechnicalReprocessingError(
            "expected_attempt_count must be positive."
        )

    requeue_event_id = _stable_requeue_event_id(
        requeue_request_id=resolved_request_id,
        queue_item_id=resolved_queue_item_id,
    )

    ledger = CsvTechnicalReprocessingLedger(
        state_directory
    )
    existing_request = ledger.existing_request(
        requeue_event_id=requeue_event_id
    )

    if not existing_request.empty:
        prior = existing_request.iloc[-1]

        if str(prior["requeue_request_id"]) != resolved_request_id:
            raise TechnicalReprocessingError(
                "The requeue event conflicts with an existing request."
            )

        if str(prior["queue_item_id"]) != resolved_queue_item_id:
            raise TechnicalReprocessingError(
                "The requeue request ID was reused for another item."
            )

        expected_metadata = {
            "requested_at": resolved_requested_at,
            "requested_by": resolved_requested_by,
            "requeue_reason": resolved_reason,
        }

        if any(
            str(prior[column]) != value
            for column, value in expected_metadata.items()
        ):
            raise TechnicalReprocessingError(
                "The repeated requeue request changed its audit "
                "metadata."
            )

        prior_attempt_count = int(
            prior["prior_attempt_count"]
        )

        if (
            expected_attempt_count is not None
            and expected_attempt_count != prior_attempt_count
        ):
            raise TechnicalReprocessingError(
                "The repeated requeue request changed its expected "
                "attempt count."
            )

        return TechnicalRequeueResult(
            requeue_event_id=requeue_event_id,
            requeue_request_id=resolved_request_id,
            queue_item_id=resolved_queue_item_id,
            subject_type=str(prior["subject_type"]),
            subject_key=str(prior["subject_key"]),
            prior_attempt_count=prior_attempt_count,
            resulting_queue_status=str(
                prior["resulting_queue_status"]
            ),
            applied=False,
            already_recorded=True,
        )

    state_store = CsvDailyStateStore(state_directory)
    frontier = state_store.load_frontier_queue()
    matches = frontier.loc[
        frontier["queue_item_id"].eq(
            resolved_queue_item_id
        )
    ]

    if len(matches) != 1:
        raise TechnicalReprocessingError(
            "Exactly one persisted frontier item must match the "
            f"requeue request; found {len(matches)}."
        )

    prior = matches.iloc[0]
    prior_status = str(prior["queue_status"]).strip().upper()

    if prior_status != "FAILED_CLOSED":
        raise TechnicalReprocessingError(
            "Only FAILED_CLOSED frontier items can be explicitly "
            f"requeued; current status is {prior_status or 'EMPTY'}."
        )

    try:
        prior_attempt_count = int(
            str(prior["attempt_count"])
        )
    except ValueError as exc:
        raise TechnicalReprocessingError(
            "The failed frontier item has an invalid attempt_count."
        ) from exc

    if prior_attempt_count < 1:
        raise TechnicalReprocessingError(
            "The failed frontier item has no recorded failed attempt."
        )

    if (
        expected_attempt_count is not None
        and prior_attempt_count != expected_attempt_count
    ):
        raise TechnicalReprocessingError(
            "The failed frontier attempt count changed before the "
            "requeue request was applied."
        )

    prior_error_code = str(prior["last_error_code"]).strip()

    if not prior_error_code:
        raise TechnicalReprocessingError(
            "The failed frontier item has no persisted error code."
        )

    record = {
        "requeue_event_id": requeue_event_id,
        "requeue_request_id": resolved_request_id,
        "requested_at": resolved_requested_at,
        "requested_by": resolved_requested_by,
        "requeue_reason": resolved_reason,
        "queue_item_id": resolved_queue_item_id,
        "run_date": str(prior["run_date"]),
        "action_type": str(prior["action_type"]),
        "subject_type": str(prior["subject_type"]),
        "subject_key": str(prior["subject_key"]),
        "feature_snapshot_hash": str(
            prior["feature_snapshot_hash"]
        ),
        "prior_queue_status": prior_status,
        "prior_attempt_count": str(prior_attempt_count),
        "prior_error_code": prior_error_code,
        "prior_error_message": str(
            prior["last_error_message"]
        ),
        "prior_response_id": str(
            prior["last_response_id"]
        ),
        "prior_request_id": str(
            prior["last_request_id"]
        ),
        "resulting_queue_status": "READY",
    }

    ledger.assert_can_append(record=record)

    updated_frontier = frontier.copy()
    mask = updated_frontier["queue_item_id"].eq(
        resolved_queue_item_id
    )
    updated_frontier.loc[mask, "queue_status"] = "READY"
    state_store.save_frontier_queue(updated_frontier)
    ledger.append(record=record)

    return TechnicalRequeueResult(
        requeue_event_id=requeue_event_id,
        requeue_request_id=resolved_request_id,
        queue_item_id=resolved_queue_item_id,
        subject_type=str(prior["subject_type"]),
        subject_key=str(prior["subject_key"]),
        prior_attempt_count=prior_attempt_count,
        resulting_queue_status="READY",
        applied=True,
        already_recorded=False,
    )
