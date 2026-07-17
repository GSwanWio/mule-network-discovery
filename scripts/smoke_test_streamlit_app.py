"""Smoke test for the unified Streamlit interface."""

from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "app/streamlit_app.py"


def main() -> None:
    """Validate the initial unified interface."""
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
    assert len(app.metric) == 10
    assert len(app.dataframe) >= 4
    assert len(app.warning) == 1

    warning_text = app.warning[0].value

    assert (
        "Counterparty branches remain blocked"
        in warning_text
    )

    assert (
        app.selectbox[0].value is not None
    )

    print(
        "Unified Streamlit interface smoke "
        "test passed."
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
        "Blocked-counterparty warning: passed"
    )


if __name__ == "__main__":
    main()
