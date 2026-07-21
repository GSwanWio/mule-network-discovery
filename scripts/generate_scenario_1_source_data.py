"""Generate Scenario 1 synthetic source CSVs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)


DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "synthetic"
    / "scenario_1"
)


def parse_arguments() -> argparse.Namespace:
    """Parse generator arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate deterministic production-shaped "
            "Scenario 1 source CSVs."
        )
    )

    parser.add_argument(
        "--output-directory",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
    )

    return parser.parse_args()


def main() -> None:
    """Generate and summarize the source pack."""
    arguments = parse_arguments()

    manifest = generate_scenario_1_source_data(
        arguments.output_directory
    )

    print("Scenario 1 synthetic sources generated.")
    print(
        f"Output directory: "
        f"{arguments.output_directory.resolve()}"
    )

    for filename, row_count in (
        manifest["row_counts"].items()
    ):
        print(
            f"{filename}: {row_count} rows"
        )

    print("Prebuilt groups: 0")
    print("Prebuilt nodes: 0")
    print("Prebuilt edges: 0")
    print("AI decisions: 0")


if __name__ == "__main__":
    main()
