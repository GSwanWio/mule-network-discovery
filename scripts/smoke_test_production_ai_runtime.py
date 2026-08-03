"""Validate production live-AI runtime preflight offline."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from network_mule_discovery.daily_ai_runner import (
    DailyAiSettings,
)
from network_mule_discovery.production_ai_runtime import (
    PRODUCTION_AI_RUNTIME_FILENAME,
    JsonProductionAiRuntimeStore,
    ProductionAiRuntimeError,
    build_production_ai_runtime,
)


def main() -> None:
    environment = {
        "OPENAI_API_KEY": "synthetic-test-key",
        "OPENAI_MODEL": "synthetic-model-v1",
        "OPENAI_PROMPT_VERSION": (
            "synthetic-prompt-v1"
        ),
        "OPENAI_TIMEOUT_SECONDS": "30",
        "OPENAI_MAX_OUTPUT_TOKENS": "2500",
    }

    with (
        patch.dict(
            "os.environ",
            environment,
            clear=False,
        ),
        TemporaryDirectory() as directory,
    ):
        settings = DailyAiSettings(
            live_ai_enabled=True,
            daily_call_limit=10,
            run_call_limit=3,
        )

        runtime = build_production_ai_runtime(
            settings
        )

        assert runtime.live_ai_enabled
        assert runtime.daily_call_limit == 10
        assert runtime.run_call_limit == 3
        assert runtime.model == "synthetic-model-v1"
        assert (
            runtime.prompt_version
            == "synthetic-prompt-v1"
        )
        assert runtime.timeout_seconds == 30
        assert runtime.max_output_tokens == 2500
        assert runtime.sdk_package == "openai"
        assert runtime.sdk_version

        store = JsonProductionAiRuntimeStore(
            directory
        )
        path = store.save(runtime)
        loaded = store.load()

        assert loaded == runtime
        assert (
            path.name
            == PRODUCTION_AI_RUNTIME_FILENAME
        )
        assert not path.with_suffix(
            ".json.tmp"
        ).exists()

        persisted = json.loads(
            path.read_text(
                encoding="utf-8"
            )
        )
        assert "api_key" not in persisted
        assert (
            "synthetic-test-key"
            not in path.read_text(
                encoding="utf-8"
            )
        )

        try:
            build_production_ai_runtime(
                DailyAiSettings(
                    live_ai_enabled=False,
                    daily_call_limit=10,
                    run_call_limit=3,
                )
            )
        except ProductionAiRuntimeError as exc:
            assert (
                "explicitly enabled"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Disabled live AI passed production preflight."
            )

        try:
            build_production_ai_runtime(
                DailyAiSettings(
                    live_ai_enabled=True,
                    daily_call_limit=2,
                    run_call_limit=3,
                )
            )
        except ProductionAiRuntimeError as exc:
            assert (
                "cannot exceed"
                in str(exc)
            )
        else:
            raise AssertionError(
                "Unsafe run budget passed preflight."
            )

        print(
            "Production AI runtime smoke test passed."
        )
        print("Live-AI enablement gate: passed")
        print("Daily and run budgets: passed")
        print("Model and prompt identity: passed")
        print("Timeout and output limit: passed")
        print("OpenAI SDK identity: passed")
        print("API key persisted: 0")
        print("Atomic runtime persistence: passed")
        print("Live API calls made: 0")


if __name__ == "__main__":
    main()
