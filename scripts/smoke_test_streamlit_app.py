"""Smoke test for the analyst-first Streamlit interface."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"
APP_PATH = PROJECT_ROOT / "app/streamlit_app.py"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from network_mule_discovery.consolidated_state import (
    ConsolidatedStateStore,
)
from network_mule_discovery.synthetic_scenario_registry import (
    create_synthetic_source_provider,
)
from smoke_test_analyst_application_runs import (
    build_run,
)


def main() -> None:
    """Validate the analyst-first application shell."""
    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        state_directory = root / "state"

        provider = create_synthetic_source_provider(
            scenario_id="scenario_1",
            output_directory=source_directory,
        )
        state_store = ConsolidatedStateStore(
            state_directory
        )

        historical_run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="streamlit-history",
            run_status="STOPPED",
            termination_status="STOPPED",
            termination_reason=(
                "MAX_FRONTIER_STEPS_REACHED"
            ),
        )
        current_run_id = build_run(
            provider=provider,
            state_store=state_store,
            state_namespace="streamlit-current",
            run_status="RUNNING",
        )

        with (
            patch.dict(
                os.environ,
                {
                    "MULE_NETWORK_STATE_DIRECTORY": (
                        str(state_directory)
                    )
                },
                clear=False,
            ),
            patch(
                "streamlit_cytoscape.streamlit_cytoscape",
                return_value=None,
            ) as graph_component,
        ):
            app = AppTest.from_file(
                str(APP_PATH),
                default_timeout=20,
            )
            app.run()

        assert not app.exception
        assert len(app.title) == 1
        assert (
            app.title[0].value
            == "Mule Network Investigation"
        )

        assert len(app.selectbox) == 1
        assert str(
            app.selectbox[0].value
        ).startswith(current_run_id)

        assert len(app.metric) == 5
        assert len(app.dataframe) == 0
        assert len(app.checkbox) == 0

        assert graph_component.call_count == 1

        graph_arguments = (
            graph_component.call_args.kwargs
        )

        assert (
            graph_arguments["layout"]["name"]
            == "breadthfirst"
        )
        assert graph_arguments["height"] == 680
        assert graph_arguments["node_actions"] == []
        assert graph_arguments["edge_actions"] == []
        assert len(graph_arguments["events"]) == 1

        app_source = APP_PATH.read_text(
            encoding="utf-8"
        )

        assert "st.graphviz_chart" not in app_source
        assert "Show full audit graph" not in app_source
        assert "st.dataframe" not in app_source
        assert "st.tabs" not in app_source
        assert (
            "build_analyst_investigation_view"
            in app_source
        )
        assert (
            "selected_investigation_node_id"
            in app_source
        )
        assert (
            "Only AI-approved expansion paths"
            in app_source
        )
        assert (
            historical_run_id
            != current_run_id
        )

        print(
            "Analyst-first Streamlit interface "
            "smoke test passed."
        )
        print("Investigation selector: passed")
        print("Interactive graph rendered: passed")
        print("Breadth-first layout: passed")
        print("Focused node review card: passed")
        print("Raw dataframes displayed: 0")
        print("Full audit graph exposed: no")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
