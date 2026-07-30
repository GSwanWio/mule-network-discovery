"""Generate committed Scenario 5 synthetic source fixtures."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from network_mule_discovery.scenario_5_synthetic_data import (
    generate_scenario_5_source_data,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=PROJECT_ROOT / "data" / "synthetic" / "scenario_5",
    )
    parser.add_argument("--changed-evidence", action="store_true")
    args = parser.parse_args()

    manifest = generate_scenario_5_source_data(
        args.output_directory,
        changed_evidence=args.changed_evidence,
    )
    print("Scenario 5 synthetic sources generated.")
    print(f"Output directory: {args.output_directory}")
    for filename, count in sorted(manifest["row_counts"].items()):
        print(f"{filename}: {count} rows")
    print("Prebuilt groups: 0")
    print("Prebuilt nodes: 0")
    print("Prebuilt edges: 0")
    print("AI decisions: 0")


if __name__ == "__main__":
    main()
