"""Smoke validation for Section 2 counterparty discovery."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_outputs import (
    COUNTERPARTY_OUTPUT_FILENAMES,
)
from network_mule_discovery.counterparty_runner import (
    run_counterparty_candidate_discovery,
)


RUN_DATE = "2026-07-16"
OUTPUT_DIRECTORY = PROJECT_ROOT / "data/demo/output"


def build_data_source() -> CsvCounterpartyNetworkDataSource:
    """Construct the demo counterparty data source."""
    return CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_pool.csv"
        ),
        customer_identity_path=(
            PROJECT_ROOT
            / "data/demo/customer_identity.csv"
        ),
        seed_mule_events_path=(
            PROJECT_ROOT
            / "data/demo/seed_mule_events.csv"
        ),
        counterparty_events_path=(
            PROJECT_ROOT
            / "data/demo/counterparty_events.csv"
        ),
        output_directory=OUTPUT_DIRECTORY,
    )


def main() -> None:
    """Run all Section 2 smoke assertions."""
    first_result = (
        run_counterparty_candidate_discovery(
            data_source=build_data_source(),
            run_date=RUN_DATE,
            output_directory=OUTPUT_DIRECTORY,
            persist_outputs=True,
        )
    )

    assert len(first_result.seed_cutoffs) == 2
    assert len(first_result.seed_transfer_events) == 4
    assert len(first_result.seed_counterparties) == 4

    assert (
        len(first_result.candidate_customer_links)
        == 4
    )

    assert (
        len(first_result.candidate_counterparties)
        == 4
    )

    assert (
        len(first_result.beneficiary_seed_links)
        == 2
    )

    assert "INT-OUT-POST-FRC" not in set(
        first_result.seed_transfer_events[
            "event_id"
        ]
    )

    assert first_result.candidate_customer_links[
        "relationship_id"
    ].is_unique

    assert first_result.beneficiary_seed_links[
        "relationship_id"
    ].is_unique

    assert not first_result.candidate_customer_links[
        "expansion_allowed_flag"
    ].any()

    assert not first_result.beneficiary_seed_links[
        "expansion_allowed_flag"
    ].any()

    expected_shared_candidates = {
        "RETAIL|R3001",
        "RETAIL|R3002",
        "SME|B3002",
        "RETAIL|R3004",
    }

    assert set(
        first_result.candidate_customer_links[
            "candidate_entity_key"
        ]
    ) == expected_shared_candidates

    expected_beneficiary_candidates = {
        "RETAIL|R3003",
        "SME|B3003",
    }

    assert set(
        first_result.beneficiary_seed_links[
            "candidate_entity_key"
        ]
    ) == expected_beneficiary_candidates

    for filename in (
        COUNTERPARTY_OUTPUT_FILENAMES.values()
    ):
        path = OUTPUT_DIRECTORY / filename

        assert path.exists(), (
            f"Missing counterparty output: {path}"
        )

        persisted_frame = pd.read_csv(path)

        assert persisted_frame is not None

    second_result = (
        run_counterparty_candidate_discovery(
            data_source=build_data_source(),
            run_date=RUN_DATE,
            output_directory=OUTPUT_DIRECTORY,
            persist_outputs=False,
        )
    )

    for attribute_name in (
        COUNTERPARTY_OUTPUT_FILENAMES
    ):
        first_frame = getattr(
            first_result,
            attribute_name,
        )

        second_frame = getattr(
            second_result,
            attribute_name,
        )

        assert_frame_equal(
            first_frame,
            second_frame,
            check_dtype=True,
        )

    print(
        "Counterparty discovery smoke test passed."
    )
    print(
        "Shared-counterparty links: "
        f"{len(first_result.candidate_customer_links)}"
    )
    print(
        "Beneficiary-to-seed links: "
        f"{len(first_result.beneficiary_seed_links)}"
    )
    print("Expansion allowed: false")
    print("Deterministic rerun: passed")


if __name__ == "__main__":
    main()
