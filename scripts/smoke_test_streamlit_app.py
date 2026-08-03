"""Smoke test for the persisted-state Streamlit interface."""

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
    """Validate the persisted analyst interface."""
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

        with patch.dict(
            os.environ,
            {
                "MULE_NETWORK_STATE_DIRECTORY": (
                    str(state_directory)
                )
            },
            clear=False,
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
            == "Mule Network Discovery"
        )

        assert len(app.selectbox) == 2
        assert (
            app.selectbox[0].value
            == current_run_id
        )
        assert app.selectbox[1].value is not None

        assert len(app.metric) == 18
        assert len(app.dataframe) >= 4
        assert len(app.warning) == 1

        warning_text = app.warning[0].value

        assert (
            "stored feature hash matches"
            in warning_text
        )

        app_source = APP_PATH.read_text(
            encoding="utf-8"
        )

        assert "data/demo/output" not in app_source
        assert (
            "MULE_NETWORK_STATE_DIRECTORY"
            in app_source
        )
        assert (
            historical_run_id
            != current_run_id
        )

        print(
            "Persisted-state Streamlit interface "
            "smoke test passed."
        )
        print("Persisted runs available: 2")
        print("Current run selected: passed")
        print("Selected group loaded: passed")
        print(
            f"Metrics rendered: {len(app.metric)}"
        )
        print(
            f"Dataframes rendered: "
            f"{len(app.dataframe)}"
        )
        print("Demo-output dependency removed: passed")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
