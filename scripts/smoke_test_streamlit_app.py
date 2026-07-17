"""Smoke test for the decision-aware Streamlit interface."""

from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "app/streamlit_app.py"


def main() -> None:
    """Validate the decision-aware interface."""
    app = AppTest.from_file(
        str(APP_PATH),
        default_timeout=15,
    )

    app.run()

    assert not app.exception

    assert len(app.title) == 1
    assert (
        app.title[0].value
        == "Mule Network Discovery"
    )

    assert len(app.selectbox) == 1
    assert len(app.metric) == 12
    assert len(app.dataframe) >= 5
    assert len(app.warning) == 1

    warning_text = app.warning[0].value

    assert (
        "stored feature hash matches"
        in warning_text
    )

    assert app.selectbox[0].value is not None

    print(
        "Decision-aware Streamlit interface "
        "smoke test passed."
    )
    print(
        f"Selected group: "
        f"{app.selectbox[0].value}"
    )
    print(
        f"Metrics rendered: {len(app.metric)}"
    )
    print(
        f"Dataframes rendered: "
        f"{len(app.dataframe)}"
    )
    print(
        "Incremental-decision warning: passed"
    )


if __name__ == "__main__":
    main()
