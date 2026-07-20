"""Validate missing-state and initialized-state preflight."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = PROJECT_ROOT / "scripts"

for path in (
    PROJECT_ROOT,
    SCRIPTS_DIRECTORY,
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from network_mule_discovery.daily_state import (
    CsvDailyStateStore,
)
from network_mule_discovery.daily_state_preflight import (
    DailyStatePreflightError,
    validate_daily_state_preflight,
)
from smoke_test_daily_incremental_state import (
    DAY_ONE,
    run_day_one,
)


def main() -> None:
    """Validate both preflight outcomes."""
    with TemporaryDirectory() as directory:
        root = Path(directory)

        missing_directory = (
            root / "missing-state"
        )

        missing_directory.mkdir(
            parents=True
        )

        try:
            validate_daily_state_preflight(
                missing_directory
            )

        except DailyStatePreflightError as exc:
            message = str(exc)

            assert (
                "network_state_groups.csv"
                in message
            )

            assert (
                "network_state_nodes.csv"
                in message
            )

            assert (
                "network_state_edges.csv"
                in message
            )

        else:
            raise AssertionError(
                "Missing daily state passed preflight."
            )

        environment = os.environ.copy()

        environment.pop(
            "MULE_NETWORK_ENABLE_LIVE_AI",
            None,
        )

        missing_cli = subprocess.run(
            [
                sys.executable,
                str(
                    PROJECT_ROOT
                    / "scripts"
                    / "run_daily_ai.py"
                ),
                "--state-directory",
                str(missing_directory),
                "--run-date",
                "2026-07-20",
                "--preflight-only",
            ],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

        missing_output = (
            missing_cli.stdout
            + missing_cli.stderr
        )

        assert missing_cli.returncode != 0

        assert (
            "Daily state preflight failed"
            in missing_output
        )

        assert "Traceback" not in missing_output

        valid_directory = (
            root / "valid-state"
        )

        state_store = CsvDailyStateStore(
            valid_directory
        )

        state_store.commit_recursive_result(
            result=run_day_one(),
            run_date=DAY_ONE,
        )

        preflight = (
            validate_daily_state_preflight(
                valid_directory
            )
        )

        assert preflight.group_count > 0
        assert preflight.node_count > 0
        assert preflight.edge_count > 0

        valid_cli = subprocess.run(
            [
                sys.executable,
                str(
                    PROJECT_ROOT
                    / "scripts"
                    / "run_daily_ai.py"
                ),
                "--state-directory",
                str(valid_directory),
                "--run-date",
                "2026-07-20",
                "--preflight-only",
            ],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

        valid_output = (
            valid_cli.stdout
            + valid_cli.stderr
        )

        assert valid_cli.returncode == 0

        assert (
            "Daily state preflight passed."
            in valid_output
        )

        assert (
            "Planning executed: False"
            in valid_output
        )

        assert (
            "Live AI calls executed: 0"
            in valid_output
        )

    print(
        "Daily state preflight smoke test passed."
    )
    print(
        "Missing-state traceback suppressed: passed"
    )
    print(
        "Missing required files reported: passed"
    )
    print(
        "Initialized network state accepted: passed"
    )
    print(
        "Preflight-only planning actions: 0"
    )
    print(
        "Preflight-only live API calls: 0"
    )


if __name__ == "__main__":
    main()
