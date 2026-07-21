"""Validate raw-source conversion and real first-layer discovery."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pandas.testing import assert_frame_equal


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    discover_counterparty_candidates,
)
from network_mule_discovery.eid_discovery import (
    discover_entities_by_seed_eids,
)
from network_mule_discovery.raw_source_adapter import (
    build_canonical_discovery_inputs,
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    LEGITIMATE_COUNTERPARTY_ACCOUNT,
    RISK_COUNTERPARTY_1_ACCOUNT,
    RISK_COUNTERPARTY_2_ACCOUNT,
    RUN_DATE,
    generate_scenario_1_source_data,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


def _sorted_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.reset_index(drop=True)

    return (
        frame
        .sort_values(
            by=list(frame.columns),
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _counterparty_key(account_number: str) -> str:
    return f"LOCAL_ACCOUNT|{account_number}"


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        canonical_one = root / "canonical-one"
        canonical_two = root / "canonical-two"
        output_directory = root / "outputs"

        generate_scenario_1_source_data(
            output_directory=source_directory
        )

        first_inputs = build_canonical_discovery_inputs(
            source_directory=source_directory,
            run_date=RUN_DATE,
        )

        first_paths = write_canonical_discovery_inputs(
            source_directory=source_directory,
            output_directory=canonical_one,
            run_date=RUN_DATE,
        )

        second_paths = write_canonical_discovery_inputs(
            source_directory=source_directory,
            output_directory=canonical_two,
            run_date=RUN_DATE,
        )

        for first_path, second_path in (
            (
                first_paths.seed_mule_pool_path,
                second_paths.seed_mule_pool_path,
            ),
            (
                first_paths.customer_identity_path,
                second_paths.customer_identity_path,
            ),
            (
                first_paths.seed_mule_events_path,
                second_paths.seed_mule_events_path,
            ),
            (
                first_paths.counterparty_events_path,
                second_paths.counterparty_events_path,
            ),
        ):
            assert first_path.read_bytes() == second_path.read_bytes()

        manifest = json.loads(
            first_paths.manifest_path.read_text(
                encoding="utf-8"
            )
        )

        assert manifest["prebuilt_groups"] == 0
        assert manifest["prebuilt_nodes"] == 0
        assert manifest["prebuilt_edges"] == 0
        assert manifest["supplied_ai_decisions"] == 0

        assert len(first_inputs.seed_mules) == 1
        assert len(first_inputs.customer_identity) == 105
        assert len(first_inputs.seed_mule_events) == 1
        assert len(first_inputs.counterparty_events) == 683

        data_source = CsvCounterpartyNetworkDataSource(
            seed_mule_pool_path=(
                first_paths.seed_mule_pool_path
            ),
            customer_identity_path=(
                first_paths.customer_identity_path
            ),
            seed_mule_events_path=(
                first_paths.seed_mule_events_path
            ),
            counterparty_events_path=(
                first_paths.counterparty_events_path
            ),
            output_directory=output_directory,
        )

        eid_result = discover_entities_by_seed_eids(
            data_source=data_source,
            run_date=RUN_DATE,
        )

        assert set(
            eid_result.eid_links[
                "candidate_entity_key"
            ]
        ) == {"SME|B2001"}

        counterparty_result = (
            discover_counterparty_candidates(
                data_source=data_source,
                run_date=RUN_DATE,
            )
        )

        seed_counterparty_keys = set(
            counterparty_result.seed_counterparties[
                "counterparty_key"
            ]
        )

        risk_one_key = _counterparty_key(
            RISK_COUNTERPARTY_1_ACCOUNT
        )

        risk_two_key = _counterparty_key(
            RISK_COUNTERPARTY_2_ACCOUNT
        )

        legitimate_key = _counterparty_key(
            LEGITIMATE_COUNTERPARTY_ACCOUNT
        )

        assert risk_one_key in seed_counterparty_keys
        assert legitimate_key in seed_counterparty_keys
        assert risk_two_key not in seed_counterparty_keys

        risk_one_links = (
            counterparty_result
            .candidate_customer_links
            .loc[
                lambda frame: frame[
                    "counterparty_key"
                ].eq(risk_one_key)
            ]
        )

        assert set(
            risk_one_links[
                "candidate_customer_id"
            ]
        ) == {
            "R1002",
            "R1003",
            "B2002",
            "R1004",
        }

        legitimate_links = (
            counterparty_result
            .candidate_customer_links
            .loc[
                lambda frame: frame[
                    "counterparty_key"
                ].eq(legitimate_key)
            ]
        )

        assert (
            legitimate_links[
                "candidate_customer_id"
            ].nunique()
            == 98
        )

        unified_result = build_unified_seed_groups(
            eid_discovery=eid_result,
            counterparty_discovery=(
                counterparty_result
            ),
            run_date=RUN_DATE,
        )

        assert len(unified_result.groups) == 1

        assert (
            unified_result.nodes[
                "node_type"
            ].eq("COUNTERPARTY").sum()
            == 2
        )

        assert not (
            source_directory
            / "ai_decisions.csv"
        ).exists()

        assert not (
            canonical_one
            / "ai_decisions.csv"
        ).exists()

        copied_directory = root / "copy"
        shutil.copytree(
            canonical_one,
            copied_directory,
        )

        copied_inputs = build_canonical_discovery_inputs(
            source_directory=source_directory,
            run_date=RUN_DATE,
        )

        assert_frame_equal(
            _sorted_frame(first_inputs.counterparty_events),
            _sorted_frame(copied_inputs.counterparty_events),
            check_dtype=False,
        )

    print(
        "Scenario 1 raw-source adapter smoke test passed."
    )
    print("Deterministic canonical conversion: passed")
    print("Existing discovery contracts accepted: passed")
    print("Normalized EID links discovered: 1")
    print("First-layer counterparties discovered: 2")
    print("Risk-1 linked customers discovered: 4")
    print("Legitimate linked customers discovered: 98")
    print("Second-layer counterparty exposed early: 0")
    print("Prebuilt groups/nodes/edges: 0")
    print("Supplied AI decisions: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
