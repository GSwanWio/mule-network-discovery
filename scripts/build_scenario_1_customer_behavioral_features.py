"""Build Scenario 1 customer evidence after counterparty decisions."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_PAYLOAD_FILENAME,
)
from network_mule_discovery.customer_behavioral_features import (
    write_customer_behavioral_features,
)
from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
    build_incremental_daily_plan,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)


def main() -> None:
    source_directory = (
        PROJECT_ROOT
        / "data"
        / "synthetic"
        / "scenario_1"
    )
    runtime_directory = source_directory / "runtime"
    first_layer_directory = runtime_directory / "first_layer"
    feature_directory = runtime_directory / "features"
    state_directory = runtime_directory / "live_ai_state"
    output_directory = runtime_directory / "customer_features"

    unified_result = load_unified_result(
        first_layer_directory
    )
    counterparty_payloads = (
        load_supplemental_subject_payloads(
            feature_directory
            / COUNTERPARTY_PAYLOAD_FILENAME
        )
    )
    state_store = CsvDailyStateStore(
        state_directory
    )
    state_store.save_network_state(
        network=unified_result,
        run_date=RUN_DATE,
    )

    plan = build_incremental_daily_plan(
        state_store=state_store,
        run_date=RUN_DATE,
        supplemental_subject_payloads=(
            counterparty_payloads
        ),
    )

    unresolved_counterparties = (
        plan.actionable_queue.loc[
            plan.actionable_queue[
                "action_type"
            ].eq("RUN_COUNTERPARTY_AI")
        ]
    )

    if not unresolved_counterparties.empty:
        raise SystemExit(
            "Customer features cannot be built until every "
            "counterparty decision is complete."
        )

    customer_queue = plan.actionable_queue.loc[
        plan.actionable_queue[
            "action_type"
        ].eq("RUN_CUSTOMER_AI")
    ]

    customer_keys = sorted(
        customer_queue["subject_key"]
        .astype("string")
        .unique()
    )

    if not customer_keys:
        raise SystemExit(
            "No customer AI subjects are ready."
        )

    result = write_customer_behavioral_features(
        source_directory=source_directory,
        customer_keys=customer_keys,
        projection=plan.projection,
        output_directory=output_directory,
        run_date=RUN_DATE,
    )

    print(
        "Scenario 1 customer behavioral features built."
    )
    print(f"Output directory: {output_directory}")
    print(
        "Customer profiles: "
        f"{len(result.customer_profiles)}"
    )
    print(
        "Customer-counterparty profiles: "
        f"{len(result.customer_counterparty_profiles)}"
    )
    print(
        "Customer payloads: "
        f"{len(result.customer_payloads)}"
    )
    print("Scenario labels included: 0")
    print("Expected customer decisions included: 0")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
