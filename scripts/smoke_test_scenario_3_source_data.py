"""Validate Scenario 3 synthetic source generation without live AI."""

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

from network_mule_discovery.scenario_3_synthetic_data import (
    ADD_ONLY_CUSTOMER_ID,
    PAYMENT_BACKED_CUSTOMER_ID,
    PAYMENT_PAIR_COUNT,
    RUN_DATE,
    SEED_CUSTOMER_ID,
    generate_scenario_3_source_data,
)
from network_mule_discovery.schemas import (
    normalize_emirates_id,
)
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_3_SOURCE_CONTRACTS,
    SCENARIO_3_SOURCE_FILENAMES,
)


FORBIDDEN_RUNTIME_COLUMNS = {
    "decision",
    "expected_decision",
    "risk_flag",
    "group_id",
    "node_id",
    "edge_id",
}


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def read_sources(directory: Path) -> dict[str, pd.DataFrame]:
    return {
        filename: pd.read_csv(
            directory / filename,
            dtype="string",
            keep_default_na=False,
        )
        for filename in SCENARIO_3_SOURCE_FILENAMES
    }


def validate_contracts(
    frames: dict[str, pd.DataFrame],
) -> None:
    for contract in SCENARIO_3_SOURCE_CONTRACTS:
        frame = frames[contract.filename]
        assert tuple(frame.columns) == contract.columns

        for column in contract.required_nonblank:
            assert frame[column].astype("string").str.strip().ne("").all()

        assert not frame.duplicated(
            subset=list(contract.unique_keys)
        ).any()
        assert not (
            set(frame.columns) & FORBIDDEN_RUNTIME_COLUMNS
        )


def main() -> None:
    with TemporaryDirectory() as first_directory:
        with TemporaryDirectory() as second_directory:
            first_path = Path(first_directory)
            second_path = Path(second_directory)

            first_manifest = generate_scenario_3_source_data(
                first_path
            )
            second_manifest = generate_scenario_3_source_data(
                second_path
            )

            assert first_manifest["row_counts"] == second_manifest["row_counts"]

            expected_files = {
                *SCENARIO_3_SOURCE_FILENAMES,
                "source_manifest.json",
            }
            assert {
                path.name
                for path in first_path.iterdir()
                if path.is_file()
            } == expected_files

            for filename in expected_files:
                assert file_hash(first_path / filename) == file_hash(
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
            assert len(identities) == 3
            assert len(accounts) == 3
            assert len(inwards) == PAYMENT_PAIR_COUNT
            assert len(outwards) == PAYMENT_PAIR_COUNT
            assert len(retail_beneficiaries) == 1
            assert len(sme_beneficiaries) == 1

            assert seeds.iloc[0]["seed_customer_id"] == SEED_CUSTOMER_ID
            seed_account_number = seeds.iloc[0]["seed_account_number"]

            beneficiaries = pd.concat(
                [
                    retail_beneficiaries.assign(
                        owner_customer_id=retail_beneficiaries[
                            "customer_id"
                        ]
                    ),
                    sme_beneficiaries.assign(
                        owner_customer_id=sme_beneficiaries[
                            "business_id"
                        ]
                    ),
                ],
                ignore_index=True,
            )
            assert set(beneficiaries["owner_customer_id"]) == {
                PAYMENT_BACKED_CUSTOMER_ID,
                ADD_ONLY_CUSTOMER_ID,
            }
            assert beneficiaries[
                "beneficiary_account_number"
            ].eq(seed_account_number).all()

            assert set(outwards["customer_id"]) == {
                PAYMENT_BACKED_CUSTOMER_ID
            }
            assert outwards[
                "beneficiary_account_number"
            ].eq(seed_account_number).all()
            assert set(inwards["customer_id"]) == {
                PAYMENT_BACKED_CUSTOMER_ID
            }

            inward_times = pd.to_datetime(
                inwards["transaction_timestamp"],
                errors="raise",
            ).sort_values().reset_index(drop=True)
            outward_times = pd.to_datetime(
                outwards["transaction_timestamp"],
                errors="raise",
            ).sort_values().reset_index(drop=True)
            deltas = outward_times - inward_times
            assert deltas.max() < pd.Timedelta(hours=2)

            inward_amount = pd.to_numeric(
                inwards["source_amount"],
                errors="raise",
            ).sum()
            outward_amount = pd.to_numeric(
                outwards["source_amount"],
                errors="raise",
            ).sum()
            assert outward_amount / inward_amount == 0.9

            normalized_eids = {
                normalize_emirates_id(value)
                for value in identities[
                    "emirates_id_number"
                ]
            }
            assert len(normalized_eids) == 3

            manifest = json.loads(
                (
                    first_path / "source_manifest.json"
                ).read_text(encoding="utf-8")
            )
            assert manifest["source_only"] is True
            assert manifest["contains_prebuilt_groups"] is False
            assert manifest["contains_prebuilt_nodes"] is False
            assert manifest["contains_prebuilt_edges"] is False
            assert manifest["contains_ai_decisions"] is False
            assert manifest["run_date"] == str(RUN_DATE)

    print("Scenario 3 synthetic source smoke test passed.")
    print("Deterministic rerun: passed")
    print("Source contract columns: passed")
    print("Unique normalized EIDs: passed")
    print("Confirmed seed accounts: 1")
    print("Beneficiary-to-seed relationships: 2")
    print(f"Payment-backed transfers: {PAYMENT_PAIR_COUNT}")
    print("Add-only beneficiary relationships: 1")
    print("Rapid-drain payment timing: passed")
    print("Prebuilt groups/nodes/edges: 0")
    print("AI decisions supplied by data pack: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
