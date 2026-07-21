"""Build canonical discovery inputs from Scenario 1 raw sources."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.raw_source_adapter import (
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert production-shaped Scenario 1 CSVs into "
            "canonical discovery inputs."
        )
    )

    parser.add_argument(
        "--source-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_1"
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
            / "canonical"
        ),
    )

    parser.add_argument(
        "--run-date",
        default=str(RUN_DATE),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    paths = write_canonical_discovery_inputs(
        source_directory=arguments.source_directory,
        output_directory=arguments.output_directory,
        run_date=arguments.run_date,
    )

    manifest = json.loads(
        paths.manifest_path.read_text(
            encoding="utf-8"
        )
    )

    print("Scenario 1 canonical inputs built.")
    print(f"Output directory: {paths.output_directory}")

    for filename, row_count in manifest[
        "row_counts"
    ].items():
        print(f"{filename}: {row_count} rows")

    print("Prebuilt groups: 0")
    print("Prebuilt nodes: 0")
    print("Prebuilt edges: 0")
    print("Supplied AI decisions: 0")


if __name__ == "__main__":
    main()
