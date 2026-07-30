"""Generate committed Scenario 3 synthetic source fixtures."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.scenario_3_synthetic_data import (
    generate_scenario_3_source_data,
)


def main() -> None:
    output_directory = (
        PROJECT_ROOT
        / "data"
        / "synthetic"
        / "scenario_3"
    )
    manifest = generate_scenario_3_source_data(
        output_directory
    )

    print("Scenario 3 synthetic sources generated.")
    print(f"Output directory: {output_directory}")

    for filename, row_count in sorted(
        manifest["row_counts"].items()
    ):
        print(f"{filename}: {row_count} rows")

    print("Prebuilt groups: 0")
    print("Prebuilt nodes: 0")
    print("Prebuilt edges: 0")
    print("AI decisions: 0")


if __name__ == "__main__":
    main()
