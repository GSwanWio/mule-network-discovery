"""Validate neutral behavioral evidence for Scenario 1."""

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

from network_mule_discovery.behavioral_features import (
    build_behavioral_features,
)
from network_mule_discovery.counterparty_data_sources import (
    CsvCounterpartyNetworkDataSource,
)
from network_mule_discovery.counterparty_discovery import (
    discover_counterparty_candidates,
)
from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_1_synthetic_data import RUN_DATE


RISK_ONE_KEY = "LOCAL_ACCOUNT|990100000001"
LEGITIMATE_KEY = "LOCAL_ACCOUNT|880100000001"
SECOND_LAYER_KEY = "LOCAL_ACCOUNT|990200000001"


def main() -> None:
    source_directory = (
        PROJECT_ROOT / "data" / "synthetic" / "scenario_1"
    )

    with TemporaryDirectory() as directory:
        root = Path(directory)
        copied_sources = root / "sources"
        shutil.copytree(source_directory, copied_sources)

        canonical_directory = root / "canonical"
        output_directory = root / "discovery"

        paths = write_canonical_discovery_inputs(
            source_directory=copied_sources,
            output_directory=canonical_directory,
            run_date=RUN_DATE,
        )

        canonical_events = pd.read_csv(
            paths.counterparty_events_path,
            dtype="string",
            keep_default_na=False,
        )
        canonical_events["event_timestamp"] = pd.to_datetime(
            canonical_events["event_timestamp"],
            errors="raise",
        )

        assert canonical_events["event_timestamp"].max() < (
            pd.Timestamp(RUN_DATE) + pd.Timedelta(days=1)
        )
        assert "LOC-OUT-0000046" not in set(
            canonical_events["transfer_id"]
        )

        data_source = CsvCounterpartyNetworkDataSource(
            seed_mule_pool_path=paths.seed_mule_pool_path,
            customer_identity_path=paths.customer_identity_path,
            seed_mule_events_path=paths.seed_mule_events_path,
            counterparty_events_path=paths.counterparty_events_path,
            output_directory=output_directory,
        )

        discovery = discover_counterparty_candidates(
            data_source=data_source,
            run_date=RUN_DATE,
        )

        frontier_keys = sorted(
            discovery.seed_counterparties[
                "counterparty_key"
            ].drop_duplicates()
        )

        assert frontier_keys == [
            LEGITIMATE_KEY,
            RISK_ONE_KEY,
        ]
        assert SECOND_LAYER_KEY not in frontier_keys

        first_result = build_behavioral_features(
            source_directory=copied_sources,
            counterparty_keys=frontier_keys,
            run_date=RUN_DATE,
        )
        second_result = build_behavioral_features(
            source_directory=copied_sources,
            counterparty_keys=reversed(frontier_keys),
            run_date=RUN_DATE,
        )

        assert_frame_equal(
            first_result.counterparty_profiles,
            second_result.counterparty_profiles,
            check_dtype=False,
        )
        assert_frame_equal(
            first_result.counterparty_customer_profiles,
            second_result.counterparty_customer_profiles,
            check_dtype=False,
        )
        assert_frame_equal(
            first_result.counterparty_payloads,
            second_result.counterparty_payloads,
            check_dtype=False,
        )

        profiles = first_result.counterparty_profiles.set_index(
            "counterparty_key"
        )

        assert set(profiles.index) == {
            RISK_ONE_KEY,
            LEGITIMATE_KEY,
        }

        risk = profiles.loc[RISK_ONE_KEY]
        legitimate = profiles.loc[LEGITIMATE_KEY]

        assert int(risk["transfer_event_count"]) == 19
        assert int(risk["distinct_customer_count"]) == 5
        assert int(legitimate["transfer_event_count"]) == 329
        assert int(legitimate["distinct_customer_count"]) == 99

        assert float(risk["top_3_customer_amount_share"]) > 0.8
        assert float(legitimate["top_3_customer_amount_share"]) < 0.2
        assert float(risk["beneficiary_created_last_30d_share"]) >= 0.8
        assert float(legitimate["beneficiary_created_last_30d_share"]) == 0.0
        assert int(legitimate["active_day_count"]) > int(
            risk["active_day_count"]
        )
        assert int(legitimate["recurring_3_month_customer_count"]) > int(
            risk["recurring_3_month_customer_count"]
        )

        relationships = (
            first_result.counterparty_customer_profiles
        )
        assert len(relationships) == 104

        r1002 = relationships.loc[
            relationships["customer_id"].eq("R1002")
            & relationships["counterparty_key"].eq(RISK_ONE_KEY)
        ].iloc[0]
        r1003 = relationships.loc[
            relationships["customer_id"].eq("R1003")
            & relationships["counterparty_key"].eq(RISK_ONE_KEY)
        ].iloc[0]
        b2002 = relationships.loc[
            relationships["customer_id"].eq("B2002")
            & relationships["counterparty_key"].eq(RISK_ONE_KEY)
        ].iloc[0]
        r1004 = relationships.loc[
            relationships["customer_id"].eq("R1004")
            & relationships["counterparty_key"].eq(RISK_ONE_KEY)
        ].iloc[0]

        assert float(r1002["flow_through_ratio_30d"]) > 1.0
        assert float(r1002["rapid_outward_share_2h_30d"]) > 0.6
        assert int(r1002["distinct_inward_source_count_30d"]) == 11
        assert float(r1003["flow_through_ratio_365d"]) < 0.1
        assert float(r1003["rapid_outward_share_2h_30d"]) == 0.0
        assert float(b2002["flow_through_ratio_365d"]) < 0.1
        assert int(b2002["account_tenure_days"]) > 365
        assert int(r1004["inward_event_count_365d"]) == 1
        assert int(r1004["outward_event_count_365d"]) == 1

        forbidden_payload_keys = {
            "expected_decision",
            "scenario_name",
            "risk_label",
            "legitimate_flag",
            "fraud_flag",
        }

        for payload_json in first_result.counterparty_payloads[
            "feature_payload_json"
        ]:
            payload = json.loads(payload_json)
            assert forbidden_payload_keys.isdisjoint(payload.keys())
            assert payload["subject_type"] == "COUNTERPARTY"
            assert payload["subject_key"] in frontier_keys
            assert payload["aggregate_behavior"]
            assert payload["linked_customer_distribution"]
            assert payload["highest_value_linked_customers"]

    print("Scenario 1 behavioral feature smoke test passed.")
    print("Future-dated source events excluded: passed")
    print("Deterministic feature rerun: passed")
    print("First-layer counterparty profiles: 2")
    print("Counterparty-customer profiles: 104")
    print("Broad legitimate customer population: 99")
    print("Concentrated risky customer population: 5")
    print("Rapid-drain customer evidence: passed")
    print("Stable-history customer evidence: passed")
    print("Sparse-history customer evidence: passed")
    print("Scenario labels in runtime payloads: 0")
    print("Expected decisions in runtime payloads: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
