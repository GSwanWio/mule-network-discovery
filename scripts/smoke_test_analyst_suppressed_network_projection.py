"""Validate collapsed display of a suppressed public hub."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = ROOT / "scripts"

for path in (
    ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from network_mule_discovery.analyst_network_projection import (
    build_analyst_network_display_projection,
)
from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_PAYLOAD_FILENAME,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
    run_counterparty_ai_frontier,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    NON_SEED_CUSTOMER_COUNT,
    RUN_DATE,
    generate_scenario_2_source_data,
)
from smoke_test_scenario_2_live_suppression import (
    CommonPublicAdapter,
)


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        work_directory = root / "runtime"
        evidence_directory = (
            work_directory
            / "hub_discovery_evidence"
        )

        generate_scenario_2_source_data(
            source_directory
        )

        subprocess.run(
            [
                sys.executable,
                str(
                    SCRIPTS_DIRECTORY
                    / "run_scenario_2_hub_discovery_evidence.py"
                ),
                "--source-directory",
                str(source_directory),
                "--work-directory",
                str(work_directory),
                "--run-date",
                str(RUN_DATE),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        unified_result = load_unified_result(
            evidence_directory
        )
        supplemental_payloads = (
            load_supplemental_subject_payloads(
                evidence_directory
                / "features"
                / COUNTERPARTY_PAYLOAD_FILENAME
            )
        )
        adapter = CommonPublicAdapter()

        result = run_counterparty_ai_frontier(
            unified_result=unified_result,
            supplemental_subject_payloads=(
                supplemental_payloads
            ),
            state_directory=(
                root / "suppressed_state"
            ),
            run_date=RUN_DATE,
            settings=DailyAiSettings(
                live_ai_enabled=True,
                daily_call_limit=10,
                run_call_limit=1,
            ),
            reset_state=True,
            adapter_factory=lambda: adapter,
        )

        raw_projection = (
            result.controlled_run
            .final_plan
            .projection
        )
        display_projection = (
            build_analyst_network_display_projection(
                nodes=raw_projection.nodes,
                edges=raw_projection.edges,
            )
        )

        assert len(raw_projection.nodes) == 502
        assert len(raw_projection.edges) == 501
        assert (
            display_projection.hidden_node_count
            == NON_SEED_CUSTOMER_COUNT
        )
        assert (
            display_projection.hidden_edge_count
            == NON_SEED_CUSTOMER_COUNT
        )
        assert len(
            display_projection.nodes
        ) == 2
        assert len(
            display_projection.edges
        ) == 1
        assert len(
            display_projection
            .collapsed_counterparties
        ) == 1

        summary = (
            display_projection
            .collapsed_counterparties
            .iloc[0]
        )

        assert (
            summary[
                "collapsed_customer_count"
            ]
            == NON_SEED_CUSTOMER_COUNT
        )
        assert (
            summary[
                "visible_linked_customer_count"
            ]
            == 0
        )
        assert display_projection.nodes[
            "collapsed_customer_count"
        ].max() == NON_SEED_CUSTOMER_COUNT
        assert len(raw_projection.nodes) == 502
        assert len(raw_projection.edges) == 501

        print(
            "Analyst suppressed-network projection "
            "smoke test passed."
        )
        print("AI decision: COMMON_PUBLIC_SUPPRESS")
        print(
            "Observed customer relationships: "
            f"{NON_SEED_CUSTOMER_COUNT}"
        )
        print(
            "Customer nodes displayed: 0"
        )
        print(
            "Collapsed customer summary: "
            f"{NON_SEED_CUSTOMER_COUNT}"
        )
        print("Raw audit graph preserved: 502/501")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
