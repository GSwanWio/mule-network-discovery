"""Validate strongest-evidence bullets and Streamlit integration."""

from __future__ import annotations

import ast
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.analyst_evidence_presentation import (
    strongest_evidence_items,
)


APP_PATH = ROOT / "app/streamlit_app.py"


def main() -> None:
    """Validate evidence normalization and UI integration."""
    assert strongest_evidence_items(None) == ()
    assert strongest_evidence_items("") == ()
    assert strongest_evidence_items(
        "Single strongest fact"
    ) == (
        "Single strongest fact",
    )
    assert strongest_evidence_items(
        "• First fact\n• Second fact"
    ) == (
        "First fact",
        "Second fact",
    )
    assert strongest_evidence_items(
        "- First fact\n* Second fact\n\n  • Third fact"
    ) == (
        "First fact",
        "Second fact",
        "Third fact",
    )

    source = APP_PATH.read_text()
    ast.parse(source)

    required_markers = (
        "strongest_evidence_items",
        "evidence_items = strongest_evidence_items(",
        '"\\n".join(',
        'f"- {item}"',
    )
    for marker in required_markers:
        assert marker in source, marker

    assert "st.markdown(key_evidence)" not in source
    assert source.count(
        "evidence_items = strongest_evidence_items("
    ) == 1

    print(
        "Analyst strongest-evidence presentation "
        "smoke test passed."
    )
    print("Single evidence item: passed")
    print("Persisted bullet markers removed: passed")
    print("Evidence order preserved: passed")
    print("Separate Markdown bullets rendered: passed")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
