"""Validate Scenario 1 production-shaped synthetic sources."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.scenario_1_synthetic_data import (
    LEGITIMATE_COUNTERPARTY_ACCOUNT,
    RISK_COUNTERPARTY_1_ACCOUNT,
    RISK_COUNTERPARTY_2_ACCOUNT,
    generate_scenario_1_source_data,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_1_SOURCE_CONTRACTS,
    SCENARIO_1_SOURCE_FILENAMES,
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
        for contract in SCENARIO_1_SOURCE_CONTRACTS
    }


def normalize_eid(value: object) -> str:
    """Match the intended production EID normalization."""
    return "".join(
        character
        for character in str(value).upper().strip()
        if character.isalnum()
    )


def validate_contracts(
    frames: dict[str, pd.DataFrame],
) -> None:
    """Validate columns, required values, and unique keys."""
    for contract in SCENARIO_1_SOURCE_CONTRACTS:
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
                subset=list(
                    contract.unique_keys
                )
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


def main() -> None:
    """Validate deterministic generation and scenario structure."""
    with TemporaryDirectory() as first_directory:
        with TemporaryDirectory() as second_directory:
            first_path = Path(first_directory)
            second_path = Path(second_directory)

            first_manifest = (
                generate_scenario_1_source_data(
                    first_path
                )
            )

            second_manifest = (
                generate_scenario_1_source_data(
                    second_path
                )
            )

            assert (
                first_manifest["row_counts"]
                == second_manifest["row_counts"]
            )

            expected_files = {
                *SCENARIO_1_SOURCE_FILENAMES,
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

            identities = frames[
                "customer_identity.csv"
            ]

            accounts = frames[
                "customer_account_master.csv"
            ]

            inwards = frames[
                "local_inward_payments.csv"
            ]

            outwards = frames[
                "local_outward_payments.csv"
            ]

            retail_beneficiaries = frames[
                "retail_beneficiary_master.csv"
            ]

            sme_beneficiaries = frames[
                "sme_beneficiary_master.csv"
            ]

            assert len(identities) == 105
            assert len(accounts) == 105
            assert len(inwards) >= 180
            assert len(outwards) >= 340

            assert set(
                identities["entity_type"]
            ) == {
                "RETAIL",
                "SME",
            }

            seed_and_sme = identities.loc[
                identities["customer_id"].isin(
                    ["R1001", "B2001"]
                ),
                [
                    "customer_id",
                    "emirates_id_number",
                ],
            ]

            normalized_eids = {
                normalize_eid(value)
                for value in seed_and_sme[
                    "emirates_id_number"
                ]
            }

            assert len(normalized_eids) == 1
            assert next(
                iter(normalized_eids)
            ) == "784198810000013"

            known_customers = set(
                identities["customer_id"]
            )

            assert set(
                accounts["customer_id"]
            ) == known_customers

            assert set(
                inwards["customer_id"]
            ).issubset(known_customers)

            assert set(
                outwards["customer_id"]
            ).issubset(known_customers)

            assert inwards["status"].eq(
                "COMPLETED"
            ).all()

            assert outwards["status"].eq(
                "COMPLETED"
            ).all()

            assert inwards["direction"].eq(
                "INWARD"
            ).all()

            assert outwards["direction"].eq(
                "OUTWARD"
            ).all()

            beneficiary_ids = set(
                retail_beneficiaries[
                    "beneficiary_id"
                ]
            ) | set(
                sme_beneficiaries[
                    "beneficiary_id"
                ]
            )

            assert set(
                outwards["beneficiary_id"]
            ).issubset(
                beneficiary_ids
            )

            first_risk_events = outwards.loc[
                outwards[
                    "beneficiary_account_number"
                ].eq(
                    RISK_COUNTERPARTY_1_ACCOUNT
                )
            ]

            assert set(
                first_risk_events["customer_id"]
            ) == {
                "R1001",
                "R1002",
                "R1003",
                "B2002",
                "R1004",
            }

            legitimate_events = outwards.loc[
                outwards[
                    "beneficiary_account_number"
                ].eq(
                    LEGITIMATE_COUNTERPARTY_ACCOUNT
                )
            ]

            assert (
                legitimate_events[
                    "customer_id"
                ].nunique()
                >= 95
            )

            assert len(
                legitimate_events
            ) >= 280

            legitimate_dates = pd.to_datetime(
                legitimate_events[
                    "transaction_timestamp"
                ],
                errors="raise",
            )

            assert (
                legitimate_dates.max()
                - legitimate_dates.min()
            ).days >= 180

            second_risk_events = outwards.loc[
                outwards[
                    "beneficiary_account_number"
                ].eq(
                    RISK_COUNTERPARTY_2_ACCOUNT
                )
            ]

            assert set(
                second_risk_events["customer_id"]
            ) == {
                "R1002",
                "R1005",
                "R1006",
                "R1007",
            }

            r1002_inwards = inwards.loc[
                inwards["customer_id"].eq(
                    "R1002"
                )
            ]

            assert len(r1002_inwards) == 11
            assert (
                pd.to_numeric(
                    r1002_inwards[
                        "target_amount"
                    ]
                ).sum()
                >= 85000
            )

            r1003_salary = inwards.loc[
                inwards["customer_id"].eq(
                    "R1003"
                )
                & inwards[
                    "payment_purpose_key"
                ].eq("SALARY")
            ]

            assert len(r1003_salary) == 12

            r1004_risk_events = (
                first_risk_events.loc[
                    first_risk_events[
                        "customer_id"
                    ].eq("R1004")
                ]
            )

            assert len(
                r1004_risk_events
            ) == 1

            assert first_manifest[
                "contains_ai_decisions"
            ] is False

            assert first_manifest[
                "contains_prebuilt_groups"
            ] is False

            assert first_manifest[
                "contains_prebuilt_nodes"
            ] is False

            assert first_manifest[
                "contains_prebuilt_edges"
            ] is False

            manifest_from_disk = json.loads(
                (
                    first_path
                    / "source_manifest.json"
                ).read_text(
                    encoding="utf-8"
                )
            )

            assert (
                manifest_from_disk
                == first_manifest
            )

            for filename, frame in frames.items():
                assert_frame_equal(
                    frame,
                    read_sources(
                        second_path
                    )[filename],
                    check_dtype=True,
                )

    print(
        "Scenario 1 synthetic source smoke test passed."
    )
    print("Deterministic rerun: passed")
    print("Source contract columns: passed")
    print("Required values and keys: passed")
    print("Normalized EID link: passed")
    print("First risky counterparty customers: 5")
    print(
        "Legitimate counterparty customers: "
        f"{legitimate_events['customer_id'].nunique()}"
    )
    print(
        "Legitimate counterparty transactions: "
        f"{len(legitimate_events)}"
    )
    print("Second risky counterparty customers: 4")
    print("Prebuilt groups/nodes/edges: 0")
    print("AI decisions supplied by data pack: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
