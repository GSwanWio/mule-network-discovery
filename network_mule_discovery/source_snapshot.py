"""Deterministic hashing for provider-neutral source snapshots."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceContractError,
)
from network_mule_discovery.source_dataset_contracts import (
    validate_source_frame,
)


SOURCE_SNAPSHOT_CONTRACT_VERSION = (
    "source-snapshot-v1"
)


def _parse_snapshot_date(
    value: date | str,
) -> date:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(
            str(value).strip()
        )
    except (TypeError, ValueError) as exc:
        raise SourceContractError(
            "run_date must use ISO format "
            "YYYY-MM-DD."
        ) from exc


def _is_missing(
    value: object,
) -> bool:
    if value is None:
        return True

    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False

    try:
        return bool(missing)
    except (TypeError, ValueError):
        return False


def _canonical_value(
    value: object,
) -> object:
    if _is_missing(value):
        return None

    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise SourceContractError(
                "Source snapshots cannot contain "
                "non-finite decimal values."
            )

        return format(value, "f")

    if isinstance(value, float):
        if not math.isfinite(value):
            raise SourceContractError(
                "Source snapshots cannot contain "
                "infinite numeric values."
            )

        return value

    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(
                nested_value
            )
            for key, nested_value
            in sorted(
                value.items(),
                key=lambda item: str(
                    item[0]
                ),
            )
        }

    if isinstance(value, (list, tuple)):
        return [
            _canonical_value(item)
            for item in value
        ]

    if isinstance(
        value,
        (str, int, bool),
    ):
        return value

    return str(value)


def _canonical_json(
    value: object,
) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _canonical_records(
    frame: pd.DataFrame,
) -> list[dict[str, object]]:
    records = [
        {
            str(column): _canonical_value(
                value
            )
            for column, value
            in record.items()
        }
        for record in frame.to_dict(
            orient="records"
        )
    ]

    return sorted(
        records,
        key=_canonical_json,
    )


def calculate_source_snapshot_hash(
    *,
    dataset_id: str,
    run_date: date | str,
    frames: Mapping[
        str,
        pd.DataFrame,
    ],
) -> str:
    normalized_dataset_id = (
        str(dataset_id).strip()
        if dataset_id is not None
        else ""
    )

    if not normalized_dataset_id:
        raise SourceContractError(
            "dataset_id must be a "
            "nonblank string."
        )

    if not isinstance(frames, Mapping):
        raise SourceContractError(
            "frames must be a "
            "dataset-name mapping."
        )

    expected_names = set(
        SOURCE_DATASET_NAMES
    )

    supplied_names = set(
        frames
    )

    missing_names = sorted(
        expected_names - supplied_names
    )

    unexpected_names = sorted(
        str(name)
        for name in supplied_names
        if name not in expected_names
    )

    if missing_names or unexpected_names:
        raise SourceContractError(
            "Source snapshot datasets do not "
            "match the provider contract. "
            f"Missing datasets: {missing_names}. "
            f"Unexpected datasets: "
            f"{unexpected_names}."
        )

    datasets: dict[
        str,
        dict[str, object],
    ] = {}

    for dataset_name in SOURCE_DATASET_NAMES:
        validated = validate_source_frame(
            dataset_name,
            frames[dataset_name],
        )

        datasets[dataset_name] = {
            "columns": list(
                validated.columns
            ),
            "records": (
                _canonical_records(
                    validated
                )
            ),
        }

    payload = {
        "snapshot_contract_version": (
            SOURCE_SNAPSHOT_CONTRACT_VERSION
        ),
        "dataset_id": (
            normalized_dataset_id
        ),
        "run_date": (
            _parse_snapshot_date(
                run_date
            ).isoformat()
        ),
        "datasets": datasets,
    }

    return hashlib.sha256(
        _canonical_json(payload).encode(
            "utf-8"
        )
    ).hexdigest()


def verify_source_bundle_snapshot(
    bundle: DiscoverySourceBundle,
) -> str:
    if not isinstance(
        bundle,
        DiscoverySourceBundle,
    ):
        raise SourceContractError(
            "bundle must be a "
            "DiscoverySourceBundle."
        )

    calculated_hash = (
        calculate_source_snapshot_hash(
            dataset_id=(
                bundle.metadata.dataset_id
            ),
            run_date=(
                bundle.metadata.run_date
            ),
            frames=bundle.as_mapping(),
        )
    )

    if (
        calculated_hash
        != bundle.metadata.source_snapshot_hash
    ):
        raise SourceContractError(
            "source_snapshot_hash does not "
            "match the validated source bundle."
        )

    return calculated_hash
