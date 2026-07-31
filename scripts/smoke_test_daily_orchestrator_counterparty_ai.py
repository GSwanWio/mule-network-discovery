"""Test the production-path counterparty AI phase offline."""

from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = ROOT / "scripts"

for path in (ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from smoke_test_daily_orchestrator_initial_discovery import (
    ScenarioOneProvider,
)
from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.daily_orchestrator import (
    run_counterparty_ai_phase,
    run_initial_discovery,
    run_source_preflight,
)
from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)
from network_mule_discovery.source_contracts import (
    SourceLoadRequest,
)


def forbidden_factory() -> object:
    raise AssertionError(
        "Planning-only orchestration created a live adapter."
    )


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        manifest = generate_scenario_1_source_data(
            source_directory
        )

        request = SourceLoadRequest.create(
            dataset_id="scenario_1",
            run_date=manifest["run_date"],
            state_namespace=(
                "counterparty-orchestrator-smoke"
            ),
        )

        provider = ScenarioOneProvider(
            source_directory=source_directory,
            source_manifest=manifest,
        )

        preflight = run_source_preflight(
            source_provider=provider,
            source_request=request,
        )

        initial_discovery = run_initial_discovery(
            source_preflight=preflight,
        )

        result = run_counterparty_ai_phase(
            initial_discovery=initial_discovery,
            state_directory=state_directory,
            settings=DailyAiSettings(
                live_ai_enabled=False,
                daily_call_limit=10,
                run_call_limit=10,
            ),
            reset_state=True,
            adapter_factory=forbidden_factory,
        )

        graph_nodes = (
            initial_discovery
            .unified_groups
            .nodes
        )
        expected_counterparties = (
            graph_nodes.loc[
                graph_nodes["node_type"]
                .astype("string")
                .str.strip()
                .str.upper()
                .eq("COUNTERPARTY"),
                "counterparty_key",
            ]
            .astype("string")
            .str.strip()
            .replace("", None)
            .dropna()
            .nunique()
        )

        assert provider.load_count == 1
        assert result.initial_discovery is initial_discovery
        assert len(result.counterparty_payloads) == (
            expected_counterparties
        )
        assert set(
            result.counterparty_payloads[
                "subject_type"
            ]
        ) == {"COUNTERPARTY"}
        assert result.counterparty_frontier is not None

        state_files = [
            path
            for path in state_directory.rglob("*")
            if path.is_file()
        ]

        assert state_files

        print(
            "Daily orchestrator counterparty AI "
            "smoke test passed."
        )
        print("Provider load count: 1")
        print(
            "Counterparty payloads: "
            f"{len(result.counterparty_payloads)}"
        )
        print(
            "Persisted state files: "
            f"{len(state_files)}"
        )
        print("Planning-only adapter calls: 0")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
