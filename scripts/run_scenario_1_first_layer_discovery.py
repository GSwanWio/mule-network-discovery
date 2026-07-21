"""Run the real first-layer discovery pipeline on Scenario 1 sources."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


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
    write_canonical_discovery_inputs,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    RUN_DATE,
)
from network_mule_discovery.unified_group_builder import (
    build_unified_seed_groups,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run real EID and first-layer counterparty discovery "
            "against Scenario 1 raw source CSVs."
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
        "--work-directory",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "synthetic"
            / "scenario_1"
            / "runtime"
        ),
    )

    parser.add_argument(
        "--run-date",
        default=str(RUN_DATE),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    canonical_directory = (
        arguments.work_directory
        / "canonical"
    )

    output_directory = (
        arguments.work_directory
        / "first_layer"
    )

    paths = write_canonical_discovery_inputs(
        source_directory=arguments.source_directory,
        output_directory=canonical_directory,
        run_date=arguments.run_date,
    )

    data_source = CsvCounterpartyNetworkDataSource(
        seed_mule_pool_path=(
            paths.seed_mule_pool_path
        ),
        customer_identity_path=(
            paths.customer_identity_path
        ),
        seed_mule_events_path=(
            paths.seed_mule_events_path
        ),
        counterparty_events_path=(
            paths.counterparty_events_path
        ),
        output_directory=output_directory,
    )

    eid_result = discover_entities_by_seed_eids(
        data_source=data_source,
        run_date=arguments.run_date,
    )

    counterparty_result = (
        discover_counterparty_candidates(
            data_source=data_source,
            run_date=arguments.run_date,
        )
    )

    unified_result = build_unified_seed_groups(
        eid_discovery=eid_result,
        counterparty_discovery=(
            counterparty_result
        ),
        run_date=arguments.run_date,
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    eid_result.eid_links.to_csv(
        output_directory / "eid_links.csv",
        index=False,
    )

    counterparty_result.seed_counterparties.to_csv(
        output_directory / "seed_counterparties.csv",
        index=False,
    )

    counterparty_result.candidate_customer_links.to_csv(
        output_directory / "candidate_customer_links.csv",
        index=False,
    )

    counterparty_result.candidate_counterparties.to_csv(
        output_directory / "candidate_counterparties.csv",
        index=False,
    )

    counterparty_result.beneficiary_seed_links.to_csv(
        output_directory / "beneficiary_seed_links.csv",
        index=False,
    )

    unified_result.groups.to_csv(
        output_directory / "unified_groups.csv",
        index=False,
    )

    unified_result.nodes.to_csv(
        output_directory / "unified_nodes.csv",
        index=False,
    )

    unified_result.edges.to_csv(
        output_directory / "unified_edges.csv",
        index=False,
    )

    print("Scenario 1 first-layer discovery completed.")
    print(f"Output directory: {output_directory}")
    print(f"EID links: {len(eid_result.eid_links)}")
    print(
        "Seed counterparties: "
        f"{len(counterparty_result.seed_counterparties)}"
    )
    print(
        "Candidate customer links: "
        f"{len(counterparty_result.candidate_customer_links)}"
    )
    print(
        "Beneficiary-to-seed links: "
        f"{len(counterparty_result.beneficiary_seed_links)}"
    )
    print(f"Unified groups: {len(unified_result.groups)}")
    print(f"Unified nodes: {len(unified_result.nodes)}")
    print(f"Unified edges: {len(unified_result.edges)}")
    print("AI decisions made: 0")


if __name__ == "__main__":
    main()
