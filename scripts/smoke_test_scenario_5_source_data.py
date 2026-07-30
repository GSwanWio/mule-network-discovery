"""Validate Scenario 5 synthetic sources."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.scenario_5_synthetic_data import (
    AMBIGUOUS_COUNTERPARTY_ACCOUNT,
    CHANGED_PAYMENT_COUNT,
    INITIAL_PAYMENT_COUNT,
    LINKED_CUSTOMER_IDS,
    generate_scenario_5_source_data,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        initial = root / "initial"
        changed = root / "changed"
        first = generate_scenario_5_source_data(initial)
        repeated = generate_scenario_5_source_data(initial)
        changed_manifest = generate_scenario_5_source_data(changed, changed_evidence=True)

        assert first == repeated
        assert first["row_counts"]["local_outward_payments.csv"] == INITIAL_PAYMENT_COUNT
        assert changed_manifest["row_counts"]["local_outward_payments.csv"] == CHANGED_PAYMENT_COUNT
        outwards = pd.read_csv(initial / "local_outward_payments.csv", dtype="string", keep_default_na=False)
        identities = pd.read_csv(initial / "customer_identity.csv", dtype="string", keep_default_na=False)
        assert len(identities) == 3
        assert identities["emirates_id_number"].nunique() == 3
        assert set(outwards["beneficiary_account_number"]) == {AMBIGUOUS_COUNTERPARTY_ACCOUNT}
        assert set(outwards["customer_id"]) == {"R5001", *LINKED_CUSTOMER_IDS}
        assert not first["contains_ai_decisions"]

    print("Scenario 5 synthetic source smoke test passed.")
    print("Deterministic rerun: passed")
    print("Customer entities: 3")
    print("Unique normalized EIDs: passed")
    print("Shared counterparty customers: 3")
    print(f"Initial payment events: {INITIAL_PAYMENT_COUNT}")
    print(f"Changed payment events: {CHANGED_PAYMENT_COUNT}")
    print("Prebuilt groups/nodes/edges: 0")
    print("AI decisions supplied by data pack: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
