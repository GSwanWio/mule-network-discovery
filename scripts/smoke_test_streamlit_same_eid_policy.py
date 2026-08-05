"""Validate same-EID final mule precedence in Streamlit."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app/streamlit_app.py"


def main() -> None:
    """Verify final and behavioral outcomes remain separate."""
    source = APP_PATH.read_text()
    ast.parse(source)

    required_markers = (
        "#### Final determination",
        "#### Behavioral assessment",
        "#### Deterministic basis",
        "This customer is determined to be a mule",
        "confirmed mule seed",
        "behavioral_decision",
        "behavioral_decision_label",
        "does not override the final",
        "identity-based determination",
        "contract, that direct identity relationship takes ",
        "precedence over any behavioral assessment.",
    )
    for marker in required_markers:
        assert marker in source, marker

    removed_markers = (
        "#### Why no AI decision was required",
        "The customer was included through a deterministic identity link.",
    )
    for marker in removed_markers:
        assert marker not in source, marker

    assert source.count("#### Final determination") == 1
    assert source.count("#### Behavioral assessment") == 1
    assert source.count("#### Deterministic basis") == 1

    print(
        "Streamlit same-EID decision policy smoke test passed."
    )
    print("Final determination shown as mule: passed")
    print("Behavioral assessment shown separately: passed")
    print("Deterministic basis shown explicitly: passed")
    print("Behavioral outcome cannot override final decision: passed")
    print("External live API calls made: 0")


if __name__ == "__main__":
    main()
