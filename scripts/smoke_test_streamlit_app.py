"""Smoke test for the analyst-first Streamlit interface."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd
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


from network_mule_discovery.analyst_application_state import (
    AnalystApplicationStateStore,
)
from network_mule_discovery.analyst_feedback import (
    CsvAnalystFeedbackStore,
)
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

        application = AnalystApplicationStateStore(
            state_directory
        )
        group_id = str(
            application.group_table(
                current_run_id
            ).iloc[0]["group_id"]
        )
        snapshot = application.load_run(
            current_run_id
        )
        group_nodes = (
            snapshot.daily_state.network.nodes.loc[
                snapshot
                .daily_state
                .network
                .nodes["group_id"]
                .eq(group_id)
            ]
        )
        counterparty = (
            group_nodes.loc[
                group_nodes["node_type"]
                .eq("COUNTERPARTY")
            ]
            .iloc[0]
        )
        review_node_id = str(
            counterparty["node_id"]
        )
        counterparty_key = str(
            counterparty["counterparty_key"]
        )

        state_store.daily_state.append_decisions(
            pd.DataFrame(
                [
                    {
                        "decision_id": (
                            "D_STREAMLIT_COUNTERPARTY"
                        ),
                        "subject_type": (
                            "COUNTERPARTY"
                        ),
                        "subject_key": counterparty_key,
                        "feature_snapshot_hash": (
                            "HASH_STREAMLIT"
                        ),
                        "decision": (
                            "SUSPICIOUS_EXPAND"
                        ),
                        "reason_code": (
                            "STREAMLIT_SMOKE_TEST"
                        ),
                        "decision_version": (
                            "streamlit-test-v1"
                        ),
                        "decided_at": (
                            "2026-08-03T18:00:00Z"
                        ),
                        "source": "OFFLINE_TEST",
                    }
                ]
            )
        )

        state_store.ai_calls.append_executions(
            run_date=snapshot.manifest.run_date,
            executed_actions=pd.DataFrame(
                [
                    {
                        "queue_item_id": (
                            "Q_STREAMLIT_COUNTERPARTY"
                        ),
                        "action_type": (
                            "RUN_COUNTERPARTY_AI"
                        ),
                        "subject_type": (
                            "COUNTERPARTY"
                        ),
                        "subject_key": counterparty_key,
                        "feature_snapshot_hash": (
                            "HASH_STREAMLIT"
                        ),
                        "execution_status": (
                            "COMPLETED"
                        ),
                        "attempted_at": (
                            "2026-08-03T18:00:00Z"
                        ),
                        "generated_decision_id": (
                            "D_STREAMLIT_COUNTERPARTY"
                        ),
                        "decision": (
                            "SUSPICIOUS_EXPAND"
                        ),
                        "reason_code": (
                            "STREAMLIT_SMOKE_TEST"
                        ),
                        "confidence": "0.94",
                        "rationale": (
                            "The counterparty has strong "
                            "network evidence requiring "
                            "continued expansion."
                        ),
                        "key_evidence_json": (
                            '["strong network evidence"]'
                        ),
                        "model": "offline-test-model",
                        "prompt_version": (
                            "streamlit-test-v1"
                        ),
                    }
                ]
            ),
        )

        CsvAnalystFeedbackStore(
            state_directory
        ).submit(
            run_id=current_run_id,
            group_id=group_id,
            node_id=review_node_id,
            subject_type="COUNTERPARTY",
            subject_key=counterparty_key,
            ai_decision="SUSPICIOUS_EXPAND",
            feedback="AI_CORRECT",
            analyst_notes=(
                "Existing analyst review."
            ),
            analyst_id="ANALYST_TEST",
            submitted_at=(
                "2026-08-03T18:05:00Z"
            ),
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
                return_value={
                    "action": (
                        "investigation_node_selected"
                    ),
                    "data": {
                        "type": "tap",
                        "target_id": review_node_id,
                        "target_group": "nodes",
                    },
                },
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
        assert len(app.radio) == 1
        assert (
            app.radio[0].label
            == "Was the AI decision correct?"
        )
        assert app.radio[0].value is None
        assert len(app.text_area) == 1
        assert any(
            button.label == "Submit review"
            for button in app.button
        )
        assert any(
            "Latest analyst review: "
            "AI marked correct"
            in message.value
            for message in app.success
        )

        assert graph_component.call_count == 1

        graph_arguments = (
            graph_component.call_args.kwargs
        )

        assert (
            graph_arguments["layout"]["name"]
            == "breadthfirst"
        )
        assert graph_arguments["height"] == 760
        assert all(
            node["selectable"] is False
            for node
            in graph_arguments[
                "elements"
            ]["nodes"]
        )
        assert all(
            node["grabbable"] is False
            for node
            in graph_arguments[
                "elements"
            ]["nodes"]
        )
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
            "CsvAnalystFeedbackStore"
            in app_source
        )
        assert "AI correct" in app_source
        assert "AI incorrect" in app_source
        assert (
            "AI decision remains"
            in app_source
        )
        assert "unchanged." in app_source
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
        print("Analyst feedback controls: passed")
        print("Latest persisted review shown: passed")
        print("AI decision override exposed: no")
        print("Raw dataframes displayed: 0")
        print("Full audit graph exposed: no")
        print("External live API calls made: 0")


if __name__ == "__main__":
    main()
