"""Smoke test for the production-path source preflight."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_orchestrator import (
    run_source_preflight,
)
from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    DiscoverySourceBundle,
    SourceContractError,
    SourceLoadRequest,
    SourceMetadata,
)
from network_mule_discovery.source_dataset_contracts import (
    SOURCE_DATASET_CONTRACTS,
)
from network_mule_discovery.source_snapshot import (
    calculate_source_snapshot_hash,
)


def _frames() -> dict[str, pd.DataFrame]:
    result = {}

    for dataset_name in SOURCE_DATASET_NAMES:
        contract = SOURCE_DATASET_CONTRACTS[
            dataset_name
        ]
        rows = [
            {
                column: (
                    f"{dataset_name}|{column}|{row_number}"
                )
                for column in contract.columns
            }
            for row_number in (1, 2)
        ]
        result[dataset_name] = pd.DataFrame(
            rows,
            columns=list(contract.columns)[::-1],
        )

    return result


def _bundle(
    request: SourceLoadRequest,
    *,
    metadata_overrides: dict[str, object] | None = None,
    invalid_hash: bool = False,
    invalid_schema: bool = False,
) -> DiscoverySourceBundle:
    frames = _frames()

    if invalid_schema:
        column = SOURCE_DATASET_CONTRACTS[
            "customer_identity"
        ].columns[0]
        frames["customer_identity"] = (
            frames["customer_identity"].drop(
                columns=[column]
            )
        )

    values = {
        "provider_name": "synthetic",
        "dataset_id": request.dataset_id,
        "run_date": request.run_date,
        "state_namespace": request.state_namespace,
    }
    values.update(metadata_overrides or {})

    if invalid_hash or invalid_schema:
        snapshot_hash = "0" * 64
    else:
        snapshot_hash = calculate_source_snapshot_hash(
            dataset_id=str(values["dataset_id"]),
            run_date=values["run_date"],
            frames=frames,
        )

    return DiscoverySourceBundle(
        metadata=SourceMetadata(
            provider_name=str(values["provider_name"]),
            dataset_id=str(values["dataset_id"]),
            run_date=values["run_date"],
            state_namespace=str(
                values["state_namespace"]
            ),
            source_manifest={"contract_version": "1"},
            source_snapshot_hash=snapshot_hash,
        ),
        **frames,
    )


class SmokeProvider:
    def __init__(
        self,
        *,
        metadata_overrides: dict[str, object] | None = None,
        invalid_hash: bool = False,
        invalid_schema: bool = False,
    ) -> None:
        self.metadata_overrides = metadata_overrides
        self.invalid_hash = invalid_hash
        self.invalid_schema = invalid_schema
        self.load_count = 0
        self.loaded_request: SourceLoadRequest | None = None

    @property
    def provider_name(self) -> str:
        return "synthetic"

    def load(
        self,
        request: SourceLoadRequest,
    ) -> DiscoverySourceBundle:
        self.load_count += 1
        self.loaded_request = request

        return _bundle(
            request,
            metadata_overrides=self.metadata_overrides,
            invalid_hash=self.invalid_hash,
            invalid_schema=self.invalid_schema,
        )


class InvalidProvider:
    provider_name = "invalid"


def _expect_error(
    action: Callable[[], object],
    message: str,
) -> None:
    try:
        action()
    except SourceContractError as exc:
        assert message in str(exc)
        return

    raise AssertionError(
        "Invalid source preflight was accepted."
    )


def _request() -> SourceLoadRequest:
    return SourceLoadRequest.create(
        dataset_id="scenario_1",
        run_date="2026-07-30",
        state_namespace="preflight-smoke",
    )


def main() -> None:
    raw_request = SourceLoadRequest(
        dataset_id=" scenario_1 ",
        run_date="2026-07-30",
        state_namespace=" preflight-smoke ",
    )
    provider = SmokeProvider()

    result = run_source_preflight(
        source_provider=provider,
        source_request=raw_request,
    )

    assert provider.load_count == 1
    assert provider.loaded_request == _request()
    assert result.source_row_counts == tuple(
        (dataset_name, 2)
        for dataset_name in SOURCE_DATASET_NAMES
    )
    assert (
        result.source_snapshot_hash
        == result.source_bundle.metadata.source_snapshot_hash
    )

    for dataset_name, frame in (
        result.source_bundle.as_mapping().items()
    ):
        assert tuple(frame.columns) == (
            SOURCE_DATASET_CONTRACTS[
                dataset_name
            ].columns
        )

    mismatch_cases = (
        ("provider_name", "databricks"),
        ("dataset_id", "scenario_2"),
        ("run_date", "2026-07-31"),
        ("state_namespace", "other"),
    )

    for field_name, value in mismatch_cases:
        _expect_error(
            lambda field_name=field_name, value=value: (
                run_source_preflight(
                    source_provider=SmokeProvider(
                        metadata_overrides={
                            field_name: value
                        }
                    ),
                    source_request=_request(),
                )
            ),
            f"{field_name} does not match",
        )

    _expect_error(
        lambda: run_source_preflight(
            source_provider=SmokeProvider(
                invalid_hash=True
            ),
            source_request=_request(),
        ),
        "source_snapshot_hash does not match",
    )
    _expect_error(
        lambda: run_source_preflight(
            source_provider=SmokeProvider(
                invalid_schema=True
            ),
            source_request=_request(),
        ),
        "customer_identity does not match",
    )
    _expect_error(
        lambda: run_source_preflight(
            source_provider=InvalidProvider(),
            source_request=_request(),
        ),
        "must implement DiscoverySourceProvider",
    )
    _expect_error(
        lambda: run_source_preflight(
            source_provider=SmokeProvider(),
            source_request="not-a-request",
        ),
        "must be a SourceLoadRequest",
    )

    print(
        "Daily orchestrator source preflight "
        "smoke test passed."
    )
    print("Provider load count: 1")
    print("Request normalization: passed")
    print("Nine-dataset validation: passed")
    print("Snapshot verification: passed")
    print("Stable row counts: passed")
    print("Metadata identity checks: passed")
    print("Invalid-schema rejection: passed")
    print("Invalid-provider rejection: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
