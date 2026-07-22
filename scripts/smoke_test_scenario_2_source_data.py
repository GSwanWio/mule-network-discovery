"""Validate Scenario 2 production-shaped synthetic sources."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.scenario_2_synthetic_data import (
    COMMON_PUBLIC_COUNTERPARTY_ACCOUNT,
    NON_SEED_CUSTOMER_COUNT,
    PAYMENTS_PER_CUSTOMER,
    RUN_DATE,
    SEED_CUSTOMER_ID,
    TOTAL_CUSTOMER_COUNT,
    TOTAL_RECURRING_PAYMENT_COUNT,
    generate_scenario_2_source_data,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_2_SOURCE_CONTRACTS,
    SCENARIO_2_SOURCE_FILENAMES,
)


FORBIDDEN_RUNTIME_COLUMNS = {
    "ai_decision",
    "decision",
    "expected_decision",
    "expected_outcome",
    "fraud_flag",
    "legitimate_flag",
    "risk_flag",
    "suspicious_flag",
}


def file_hash(path: Path) -> str:
    """Return the SHA-256 digest of one file."""
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def normalize_eid(value: object) -> str:
    """Normalize an Emirates ID for uniqueness checks."""
    return "".join(
        character
        for character in str(value).upper().strip()
        if character.isalnum()
    )


def read_sources(
    directory: Path,
) -> dict[str, pd.DataFrame]:
    """Read every generated source file as strings."""
    return {
        contract.filename: pd.read_csv(
            directory / contract.filename,
            dtype="string",
            keep_default_na=False,
        )
        for contract in SCENARIO_2_SOURCE_CONTRACTS
    }


def validate_contracts(
    frames: dict[str, pd.DataFrame],
) -> None:
    """Validate columns, required values, and unique keys."""
    for contract in SCENARIO_2_SOURCE_CONTRACTS:
        frame = frames[contract.filename]
        assert tuple(frame.columns) == contract.columns

        for column in contract.required_nonblank:
            assert (
                frame[column]
                .astype("string")
                .str.strip()
                .ne("")
                .all()
            ), (
                f"{contract.filename}.{column} "
                "contains blanks."
            )

        if contract.unique_keys:
            assert not frame.duplicated(
                subset=list(contract.unique_keys)
            ).any(), (
                f"{contract.filename} contains "
                "duplicate contract keys."
            )

        forbidden_columns = (
            set(frame.columns)
            & FORBIDDEN_RUNTIME_COLUMNS
        )

        assert not forbidden_columns, (
            f"{contract.filename} leaks outcomes: "
            f"{sorted(forbidden_columns)}"
        )


def validate_no_rapid_drain(
    inwards: pd.DataFrame,
    outwards: pd.DataFrame,
) -> None:
    """Confirm recurring service payments are not rapid drains."""
    inward_times = (
        inwards.assign(
            event_timestamp=pd.to_datetime(
                inwards["transaction_timestamp"],
                errors="raise",
            )
        )
        .sort_values(
            by=[
                "customer_id",
                "event_timestamp",
            ],
            kind="stable",
        )
        .groupby(
            "customer_id",
            sort=False,
        )["event_timestamp"]
        .apply(list)
        .to_dict()
    )

    outward_times = (
        outwards.assign(
            event_timestamp=pd.to_datetime(
                outwards["transaction_timestamp"],
                errors="raise",
            )
        )
        .sort_values(
            by=[
                "customer_id",
                "event_timestamp",
            ],
            kind="stable",
        )
        .groupby(
            "customer_id",
            sort=False,
        )["event_timestamp"]
        .apply(list)
        .to_dict()
    )

    assert set(inward_times) == set(outward_times)

    for customer_id in sorted(outward_times):
        customer_inwards = inward_times[customer_id]
        customer_outwards = outward_times[customer_id]

        assert len(customer_inwards) == PAYMENTS_PER_CUSTOMER
        assert len(customer_outwards) == PAYMENTS_PER_CUSTOMER

        for inward_timestamp, outward_timestamp in zip(
            customer_inwards,
            customer_outwards,
            strict=True,
        ):
            delta = outward_timestamp - inward_timestamp
            assert delta >= pd.Timedelta(days=6)


def main() -> None:
    """Validate deterministic generation and scenario structure."""
    with TemporaryDirectory() as first_directory:
        with TemporaryDirectory() as second_directory:
            first_path = Path(first_directory)
            second_path = Path(second_directory)

            first_manifest = (
                generate_scenario_2_source_data(
                    first_path
                )
            )
            second_manifest = (
                generate_scenario_2_source_data(
                    second_path
                )
            )

            assert (
                first_manifest["row_counts"]
                == second_manifest["row_counts"]
            )

            expected_files = {
                *SCENARIO_2_SOURCE_FILENAMES,
                "source_manifest.json",
            }

            assert {
                path.name
                for path in first_path.iterdir()
                if path.is_file()
            } == expected_files

            for filename in expected_files:
                assert file_hash(
                    first_path / filename
                ) == file_hash(
                    second_path / filename
                )

            frames = read_sources(first_path)
            validate_contracts(frames)

            seeds = frames["seed_mule_pool.csv"]
            identities = frames["customer_identity.csv"]
            accounts = frames["customer_account_master.csv"]
            inwards = frames["local_inward_payments.csv"]
            outwards = frames["local_outward_payments.csv"]
            retail_beneficiaries = frames[
                "retail_beneficiary_master.csv"
            ]
            sme_beneficiaries = frames[
                "sme_beneficiary_master.csv"
            ]

            assert len(seeds) == 1
            assert len(identities) == TOTAL_CUSTOMER_COUNT
            assert len(accounts) == TOTAL_CUSTOMER_COUNT
            assert len(inwards) == TOTAL_RECURRING_PAYMENT_COUNT
            assert len(outwards) == TOTAL_RECURRING_PAYMENT_COUNT
            assert len(retail_beneficiaries) == 451
            assert len(sme_beneficiaries) == 50

            assert seeds.iloc[0][
                "seed_customer_id"
            ] == SEED_CUSTOMER_ID

            assert set(
                identities["entity_type"]
            ) == {"RETAIL", "SME"}

            normalized_eids = {
                normalize_eid(value)
                for value in identities[
                    "emirates_id_number"
                ]
            }

            assert len(normalized_eids) == TOTAL_CUSTOMER_COUNT

            known_customers = set(
                identities["customer_id"]
            )

            assert set(
                accounts["customer_id"]
            ) == known_customers
            assert set(
                inwards["customer_id"]
            ) == known_customers
            assert set(
                outwards["customer_id"]
            ) == known_customers

            assert outwards[
                "beneficiary_account_number"
            ].eq(
                COMMON_PUBLIC_COUNTERPARTY_ACCOUNT
            ).all()

            linked_customers = set(
                outwards["customer_id"]
            )

            assert len(linked_customers) == TOTAL_CUSTOMER_COUNT
            assert (
                len(
                    linked_customers
                    - {SEED_CUSTOMER_ID}
                )
                == NON_SEED_CUSTOMER_COUNT
            )

            customer_payment_counts = (
                outwards.groupby(
                    "customer_id",
                    sort=False,
                )
                .size()
            )

            assert customer_payment_counts.eq(
                PAYMENTS_PER_CUSTOMER
            ).all()

            assert set(
                outwards["payment_purpose_key"]
            ) == {"UTILITY_PAYMENT"}
            assert set(
                outwards["payment_purpose_name"]
            ) == {"Utility Bill Payment"}

            event_timestamps = pd.to_datetime(
                outwards["transaction_timestamp"],
                errors="raise",
            )

            assert (
                event_timestamps.max()
                - event_timestamps.min()
                >= pd.Timedelta(days=120)
            )

            customer_amounts = (
                outwards.assign(
                    amount=pd.to_numeric(
                        outwards["source_amount"],
                        errors="raise",
                    )
                )
                .groupby(
                    "customer_id",
                    sort=False,
                )["amount"]
                .sum()
                .sort_values(
                    ascending=False,
                    kind="stable",
                )
            )

            top_customer_share = float(
                customer_amounts.iloc[0]
                / customer_amounts.sum()
            )

            top_three_share = float(
                customer_amounts.iloc[:3].sum()
                / customer_amounts.sum()
            )

            assert top_customer_share < 0.01
            assert top_three_share < 0.03

            beneficiaries = pd.concat(
                [
                    retail_beneficiaries,
                    sme_beneficiaries.rename(
                        columns={
                            "business_id": "customer_id"
                        }
                    ),
                ],
                ignore_index=True,
            )

            beneficiary_created = pd.to_datetime(
                beneficiaries[
                    "beneficiary_created_date"
                ],
                errors="raise",
            )

            beneficiary_age_days = (
                pd.Timestamp(RUN_DATE)
                - beneficiary_created
            ).dt.days

            assert beneficiary_age_days.min() >= 365
            assert not beneficiary_age_days.lt(90).any()

            validate_no_rapid_drain(
                inwards,
                outwards,
            )

            manifest = json.loads(
                (
                    first_path
                    / "source_manifest.json"
                ).read_text(encoding="utf-8")
            )

            assert manifest["source_only"] is True
            assert manifest[
                "contains_prebuilt_groups"
            ] is False
            assert manifest[
                "contains_prebuilt_nodes"
            ] is False
            assert manifest[
                "contains_prebuilt_edges"
            ] is False
            assert manifest[
                "contains_ai_decisions"
            ] is False

            design_reference = manifest[
                "design_reference"
            ]

            assert design_reference[
                "non_seed_customer_count"
            ] == NON_SEED_CUSTOMER_COUNT
            assert design_reference[
                "total_recurring_payment_count"
            ] == TOTAL_RECURRING_PAYMENT_COUNT

    print("Scenario 2 synthetic source smoke test passed.")
    print("Deterministic rerun: passed")
    print("Source contract columns: passed")
    print("Required values and keys: passed")
    print("Unique normalized EIDs: passed")
    print(
        "Non-seed linked customers: "
        f"{NON_SEED_CUSTOMER_COUNT}"
    )
    print(
        "Total linked customers: "
        f"{TOTAL_CUSTOMER_COUNT}"
    )
    print(
        "Recurring outward payments: "
        f"{TOTAL_RECURRING_PAYMENT_COUNT}"
    )
    print("Low customer concentration: passed")
    print("Established beneficiary relationships: passed")
    print("Rapid-drain relationships: 0")
    print("Prebuilt groups/nodes/edges: 0")
    print("AI decisions supplied by data pack: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
