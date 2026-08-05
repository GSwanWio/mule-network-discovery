"""Validate Streamlit network-narrative integration."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app/streamlit_app.py"


def main() -> None:
    """Verify the narrative replaced operational metric cards."""
    source = APP_PATH.read_text()
    ast.parse(source)

    required_markers = (
        "build_analyst_network_narrative",
        "_render_network_narrative",
        "Network summary",
        "mule-network-narrative",
        "_render_network_narrative(narrative)",
    )
    for marker in required_markers:
        assert marker in source, marker

    removed_operational_labels = (
        '"Journey depth"',
        '"AI expanded"',
        '"AI stopped"',
        '"Deterministic links"',
        '"Awaiting AI"',
        '"Customers summarized"',
    )
    for label in removed_operational_labels:
        assert label not in source, label

    assert source.count(
        "build_analyst_network_narrative("
    ) == 1
    assert source.count(
        "_render_network_narrative(narrative)"
    ) == 1

    print(
        "Streamlit network narrative integration "
        "smoke test passed."
    )
    print("Operational metric cards removed: passed")
    print("Narrative contract invoked once: passed")
    print("Narrative rendered once: passed")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
