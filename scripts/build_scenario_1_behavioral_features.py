"""Build neutral first-frontier behavior features for Scenario 1."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.behavioral_features import (
    write_behavioral_features,
)
from network_mule_discovery.scenario_1_synthetic_data import RUN_DATE


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build neutral behavior features for the observed "
            "Scenario 1 counterparty frontier."
        )
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT / "data" / "synthetic" / "scenario_1"
        ),
    )
    parser.add_argument(
        "--first-layer-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_1"
            / "runtime"
            / "first_layer"
        ),
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_1"
            / "runtime"
            / "features"
        ),
    )
    parser.add_argument("--run-date", default=str(RUN_DATE))
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    frontier_path = arguments.first_layer_directory / "seed_counterparties.csv"

    if not frontier_path.is_file():
        raise SystemExit(
            "First-layer frontier is missing. Run "
            "scripts/run_scenario_1_first_layer_discovery.py first."
        )

    frontier = pd.read_csv(
        frontier_path,
        dtype="string",
        keep_default_na=False,
    )
    counterparty_keys = sorted(
        frontier["counterparty_key"].drop_duplicates().tolist()
    )

    result = write_behavioral_features(
        source_directory=arguments.source_directory,
        counterparty_keys=counterparty_keys,
        output_directory=arguments.output_directory,
        run_date=arguments.run_date,
    )

    print("Scenario 1 behavioral features built.")
    print(f"Output directory: {arguments.output_directory}")
    print(f"Counterparty profiles: {len(result.counterparty_profiles)}")
    print(
        "Counterparty-customer profiles: "
        f"{len(result.counterparty_customer_profiles)}"
    )
    print(f"Counterparty payloads: {len(result.counterparty_payloads)}")
    print("Scenario labels included: 0")
    print("Expected decisions included: 0")
    print("AI decisions made: 0")


if __name__ == "__main__":
    main()
