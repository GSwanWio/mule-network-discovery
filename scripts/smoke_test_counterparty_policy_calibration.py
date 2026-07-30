"""Validate calibrated counterparty policy versioning and requeue hash."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (PROJECT_ROOT, SCRIPTS_DIRECTORY):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.behavioral_features import (
    COUNTERPARTY_PAYLOAD_FILENAME,
)
from network_mule_discovery.decision_engine import (
    build_subject_snapshots,
)
from network_mule_discovery.decision_policy import (
    COUNTERPARTY_ASSESSMENT_POLICY_VERSION,
    effective_prompt_version,
)
from network_mule_discovery.frontier_ai import (
    load_supplemental_subject_payloads,
    load_unified_result,
)
from network_mule_discovery.openai_decision_adapter import (
    COUNTERPARTY_SYSTEM_PROMPT,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    RUN_DATE,
    generate_scenario_5_source_data,
)


def main() -> None:
    """Validate policy text, evidence version and material hash change."""
    assert (
        COUNTERPARTY_ASSESSMENT_POLICY_VERSION
        == "counterparty-assessment-policy-v2"
    )
    assert (
        effective_prompt_version(
            subject_type="COUNTERPARTY",
            base_prompt_version="mule-network-v3",
        )
        == (
            "mule-network-v3:"
            "counterparty-assessment-policy-v2"
        )
    )
    assert (
        effective_prompt_version(
            subject_type="CUSTOMER",
            base_prompt_version="mule-network-v3",
        )
        == "mule-network-v3"
    )

    required_prompt_statements = (
        "never sufficient by itself for SUSPICIOUS_EXPAND",
        "at least two independent corroborating evidence categories",
        "Absence of recurrence is not evidence of mule aggregation",
        "Sparse or one-off evidence must not receive HIGH confidence",
    )
    normalized_prompt = " ".join(
        COUNTERPARTY_SYSTEM_PROMPT.split()
    )

    for statement in required_prompt_statements:
        assert statement in normalized_prompt

    with TemporaryDirectory() as directory:
        root = Path(directory)
        source_directory = root / "source"
        work_directory = root / "work"

        generate_scenario_5_source_data(
            source_directory
        )

        subprocess.run(
            [
                sys.executable,
                str(
                    SCRIPTS_DIRECTORY
                    / "run_scenario_5_counterparty_evidence.py"
                ),
                "--source-directory",
                str(source_directory),
                "--work-directory",
                str(work_directory),
                "--run-date",
                str(RUN_DATE),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        evidence_directory = (
            work_directory
            / "counterparty_evidence"
        )
        unified_result = load_unified_result(
            evidence_directory
        )
        payload_path = (
            evidence_directory
            / "features"
            / COUNTERPARTY_PAYLOAD_FILENAME
        )
        current_payloads = (
            load_supplemental_subject_payloads(
                payload_path
            )
        )

        current_payload = json.loads(
            current_payloads.iloc[0][
                "feature_payload_json"
            ]
        )
        assert (
            current_payload[
                "counterparty_assessment_policy_version"
            ]
            == COUNTERPARTY_ASSESSMENT_POLICY_VERSION
        )

        current_snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=unified_result.edges,
            supplemental_subject_payloads=(
                current_payloads
            ),
        )
        current_counterparty = current_snapshots.loc[
            current_snapshots["subject_type"].eq(
                "COUNTERPARTY"
            )
        ]
        assert len(current_counterparty) == 1

        previous_payload = dict(current_payload)
        previous_payload[
            "counterparty_assessment_policy_version"
        ] = "counterparty-assessment-policy-v1"

        previous_payloads = pd.DataFrame(
            [
                {
                    "subject_type": "COUNTERPARTY",
                    "subject_key": current_payloads.iloc[0][
                        "subject_key"
                    ],
                    "feature_payload_json": json.dumps(
                        previous_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            ]
        )

        previous_snapshots = build_subject_snapshots(
            nodes=unified_result.nodes,
            edges=unified_result.edges,
            supplemental_subject_payloads=(
                previous_payloads
            ),
        )
        previous_counterparty = previous_snapshots.loc[
            previous_snapshots["subject_type"].eq(
                "COUNTERPARTY"
            )
        ]
        assert len(previous_counterparty) == 1
        assert (
            current_counterparty.iloc[0][
                "feature_snapshot_hash"
            ]
            != previous_counterparty.iloc[0][
                "feature_snapshot_hash"
            ]
        )

    print("Counterparty policy calibration smoke test passed.")
    print(
        "Policy version: "
        f"{COUNTERPARTY_ASSESSMENT_POLICY_VERSION}"
    )
    print("Shared-usage-only expansion prohibited: passed")
    print("Independent corroboration requirement: passed")
    print("Sparse-evidence confidence calibration: passed")
    print("Policy version changed feature hash: passed")
    print("Customer prompt version compatibility: passed")
    print("Live API calls made: 0")


if __name__ == "__main__":
    main()
