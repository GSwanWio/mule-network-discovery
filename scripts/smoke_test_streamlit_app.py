"""Smoke test for the basic Streamlit interface."""

from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "app/streamlit_app.py"


def main() -> None:
    """Run the interface and validate its initial state."""
    app = AppTest.from_file(
        str(APP_PATH),
        default_timeout=15,
    )

    app.run()

    assert not app.exception
    assert len(app.title) == 1
    assert app.title[0].value == "Mule Network Discovery"
    assert len(app.metric) == 9
    assert len(app.dataframe) == 3

    print("Streamlit interface smoke test passed.")
    print(f"Metrics rendered: {len(app.metric)}")
    print(f"Dataframes rendered: {len(app.dataframe)}")


if __name__ == "__main__":
    main()
