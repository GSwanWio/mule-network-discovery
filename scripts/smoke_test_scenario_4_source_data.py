"""Validate Scenario 4 synthetic source generation without live AI."""

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

from network_mule_discovery.scenario_4_synthetic_data import (
    GROUP_A_SEED_ID,
    GROUP_A_SME_IDS,
    GROUP_B_RETAIL_ID,
    GROUP_B_SEED_ID,
    RUN_DATE,
    generate_scenario_4_source_data,
)
from network_mule_discovery.schemas import normalize_emirates_id
from network_mule_discovery.synthetic_source_contracts import (
    SCENARIO_4_SOURCE_CONTRACTS,
    SCENARIO_4_SOURCE_FILENAMES,
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
        for filename in SCENARIO_4_SOURCE_FILENAMES
    }


def validate_contracts(
    frames: dict[str, pd.DataFrame],
) -> None:
    for contract in SCENARIO_4_SOURCE_CONTRACTS:
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

            first_manifest = generate_scenario_4_source_data(
                first_path
            )
            second_manifest = generate_scenario_4_source_data(
                second_path
            )

            assert first_manifest["row_counts"] == second_manifest["row_counts"]

            expected_files = {
                *SCENARIO_4_SOURCE_FILENAMES,
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

            assert len(seeds) == 2
            assert len(identities) == 5
            assert len(accounts) == 5
            assert inwards.empty
            assert outwards.empty
            assert retail_beneficiaries.empty
            assert sme_beneficiaries.empty

            assert set(seeds["seed_customer_id"]) == {
                GROUP_A_SEED_ID,
                GROUP_B_SEED_ID,
            }
            assert set(seeds["seed_entity_type"]) == {
                "RETAIL",
                "SME",
            }

            normalized_by_customer = {
                row.customer_id: normalize_emirates_id(
                    row.emirates_id_number
                )
                for row in identities.itertuples(index=False)
            }
            assert len(set(normalized_by_customer.values())) == 2
            assert len({
                normalized_by_customer[GROUP_A_SEED_ID],
                *(
                    normalized_by_customer[customer_id]
                    for customer_id in GROUP_A_SME_IDS
                ),
            }) == 1
            assert (
                normalized_by_customer[GROUP_B_SEED_ID]
                == normalized_by_customer[GROUP_B_RETAIL_ID]
            )
            assert (
                normalized_by_customer[GROUP_A_SEED_ID]
                != normalized_by_customer[GROUP_B_SEED_ID]
            )

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

    print("Scenario 4 synthetic source smoke test passed.")
    print("Deterministic rerun: passed")
    print("Source contract columns: passed")
    print("Confirmed seeds: 2")
    print("Customer entities: 5")
    print("Distinct normalized EIDs: 2")
    print("Expected EID-only group sizes: 3 and 2")
    print("Payment events: 0")
    print("Beneficiary relationships: 0")
    print("Prebuilt groups/nodes/edges: 0")
    print("AI decisions supplied by data pack: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
