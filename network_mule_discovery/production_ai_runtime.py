"""Validated and auditable production live-AI runtime identity."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Protocol

from network_mule_discovery.openai_config import (
    load_openai_settings,
)


PRODUCTION_AI_RUNTIME_FILENAME = (
    "production_ai_runtime.json"
)
PRODUCTION_AI_RUNTIME_VERSION = (
    "production-ai-runtime-v1"
)
PRODUCTION_AI_STARTUP_FAILURE_FILENAME = (
    "production_ai_startup_failure.json"
)
PRODUCTION_AI_STARTUP_FAILURE_VERSION = (
    "production-ai-startup-failure-v1"
)


class DailyAiSettingsLike(Protocol):
    """Settings required by production runtime validation."""

    live_ai_enabled: bool
    daily_call_limit: int
    run_call_limit: int


class ProductionAiRuntimeError(RuntimeError):
    """Production live-AI configuration is not safe to run."""


@dataclass(frozen=True)
class ProductionAiRuntime:
    """Non-secret configuration identity for one live-AI run."""

    runtime_version: str
    live_ai_enabled: bool
    daily_call_limit: int
    run_call_limit: int
    model: str
    prompt_version: str
    timeout_seconds: float
    max_output_tokens: int
    sdk_package: str
    sdk_version: str

    def __post_init__(self) -> None:
        if (
            self.runtime_version
            != PRODUCTION_AI_RUNTIME_VERSION
        ):
            raise ProductionAiRuntimeError(
                "Unsupported production AI runtime version."
            )

        if not self.live_ai_enabled:
            raise ProductionAiRuntimeError(
                "Production live AI must be explicitly enabled."
            )

        if self.daily_call_limit <= 0:
            raise ProductionAiRuntimeError(
                "daily_call_limit must be positive."
            )

        if self.run_call_limit <= 0:
            raise ProductionAiRuntimeError(
                "run_call_limit must be positive."
            )

        if self.run_call_limit > self.daily_call_limit:
            raise ProductionAiRuntimeError(
                "run_call_limit cannot exceed "
                "daily_call_limit."
            )

        for field_name in (
            "model",
            "prompt_version",
            "sdk_package",
            "sdk_version",
        ):
            if not str(
                getattr(self, field_name)
            ).strip():
                raise ProductionAiRuntimeError(
                    f"{field_name} must be nonblank."
                )

        if self.timeout_seconds <= 0:
            raise ProductionAiRuntimeError(
                "timeout_seconds must be positive."
            )

        if self.max_output_tokens <= 0:
            raise ProductionAiRuntimeError(
                "max_output_tokens must be positive."
            )

    def to_record(self) -> dict[str, object]:
        """Return a JSON-safe record containing no secret."""
        return asdict(self)

    @classmethod
    def from_record(
        cls,
        record: dict[str, object],
    ) -> ProductionAiRuntime:
        """Rebuild one persisted runtime identity."""
        if not isinstance(record, dict):
            raise ProductionAiRuntimeError(
                "Production AI runtime must be a JSON object."
            )

        required_fields = {
            "runtime_version",
            "live_ai_enabled",
            "daily_call_limit",
            "run_call_limit",
            "model",
            "prompt_version",
            "timeout_seconds",
            "max_output_tokens",
            "sdk_package",
            "sdk_version",
        }

        missing_fields = sorted(
            required_fields - set(record)
        )

        if missing_fields:
            raise ProductionAiRuntimeError(
                "Production AI runtime is missing fields: "
                f"{missing_fields}"
            )

        return cls(
            runtime_version=str(
                record["runtime_version"]
            ),
            live_ai_enabled=bool(
                record["live_ai_enabled"]
            ),
            daily_call_limit=int(
                record["daily_call_limit"]
            ),
            run_call_limit=int(
                record["run_call_limit"]
            ),
            model=str(record["model"]),
            prompt_version=str(
                record["prompt_version"]
            ),
            timeout_seconds=float(
                record["timeout_seconds"]
            ),
            max_output_tokens=int(
                record["max_output_tokens"]
            ),
            sdk_package=str(
                record["sdk_package"]
            ),
            sdk_version=str(
                record["sdk_version"]
            ),
        )


def build_production_ai_runtime(
    daily_settings: DailyAiSettingsLike,
) -> ProductionAiRuntime:
    """Validate live-AI settings without making an API call."""
    required_attributes = (
        "live_ai_enabled",
        "daily_call_limit",
        "run_call_limit",
    )
    missing_attributes = [
        attribute
        for attribute in required_attributes
        if not hasattr(
            daily_settings,
            attribute,
        )
    ]

    if missing_attributes:
        raise ProductionAiRuntimeError(
            "daily_settings is missing attributes: "
            f"{missing_attributes}"
        )

    try:
        openai_settings = load_openai_settings()
    except Exception as exc:
        raise ProductionAiRuntimeError(
            "OpenAI runtime configuration failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    try:
        sdk_version = version("openai")
    except PackageNotFoundError as exc:
        raise ProductionAiRuntimeError(
            "The openai package is not installed."
        ) from exc

    return ProductionAiRuntime(
        runtime_version=(
            PRODUCTION_AI_RUNTIME_VERSION
        ),
        live_ai_enabled=(
            daily_settings.live_ai_enabled
        ),
        daily_call_limit=(
            daily_settings.daily_call_limit
        ),
        run_call_limit=(
            daily_settings.run_call_limit
        ),
        model=openai_settings.model,
        prompt_version=(
            openai_settings.prompt_version
        ),
        timeout_seconds=(
            openai_settings.timeout_seconds
        ),
        max_output_tokens=(
            openai_settings.max_output_tokens
        ),
        sdk_package="openai",
        sdk_version=sdk_version,
    )


class ProductionAiStartupError(RuntimeError):
    """The default production adapter could not start safely."""

    code = "PRODUCTION_AI_STARTUP_FAILED"


@dataclass(frozen=True)
class ProductionAiStartupFailure:
    """Non-secret audit record for a startup failure."""

    failure_version: str
    error_code: str
    error_type: str
    error_message: str
    failed_at: str

    def __post_init__(self) -> None:
        if (
            self.failure_version
            != PRODUCTION_AI_STARTUP_FAILURE_VERSION
        ):
            raise ProductionAiRuntimeError(
                "Unsupported startup-failure version."
            )

        for field_name in (
            "error_code",
            "error_type",
            "error_message",
            "failed_at",
        ):
            if not str(
                getattr(self, field_name)
            ).strip():
                raise ProductionAiRuntimeError(
                    f"{field_name} must be nonblank."
                )

    def to_record(self) -> dict[str, object]:
        """Return a JSON-safe non-secret record."""
        return asdict(self)

    @classmethod
    def from_record(
        cls,
        record: dict[str, object],
    ) -> "ProductionAiStartupFailure":
        """Rebuild one persisted failure record."""
        if not isinstance(record, dict):
            raise ProductionAiRuntimeError(
                "Startup failure must be a JSON object."
            )

        required_fields = {
            "failure_version",
            "error_code",
            "error_type",
            "error_message",
            "failed_at",
        }
        missing_fields = sorted(
            required_fields - set(record)
        )

        if missing_fields:
            raise ProductionAiRuntimeError(
                "Startup failure is missing fields: "
                f"{missing_fields}"
            )

        return cls(
            failure_version=str(
                record["failure_version"]
            ),
            error_code=str(record["error_code"]),
            error_type=str(record["error_type"]),
            error_message=str(
                record["error_message"]
            ),
            failed_at=str(record["failed_at"]),
        )


def build_production_ai_startup_failure(
    exc: Exception,
) -> ProductionAiStartupFailure:
    """Build a redacted audit record from one startup error."""
    message = (
        f"{type(exc).__name__}: {exc}"
    )[:1000]

    api_key = os.getenv(
        "OPENAI_API_KEY",
        "",
    ).strip()

    if api_key:
        message = message.replace(
            api_key,
            "[REDACTED]",
        )

    return ProductionAiStartupFailure(
        failure_version=(
            PRODUCTION_AI_STARTUP_FAILURE_VERSION
        ),
        error_code=(
            ProductionAiStartupError.code
        ),
        error_type=type(exc).__name__,
        error_message=message,
        failed_at=datetime.now(
            timezone.utc
        ).isoformat(),
    )


class JsonProductionAiStartupFailureStore:
    """Atomic persistence for production startup failures."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state_directory = Path(
            state_directory
        )
        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.path = (
            self.state_directory
            / PRODUCTION_AI_STARTUP_FAILURE_FILENAME
        )

    def save(
        self,
        failure: ProductionAiStartupFailure,
    ) -> Path:
        """Persist one validated startup failure."""
        if not isinstance(
            failure,
            ProductionAiStartupFailure,
        ):
            raise ProductionAiRuntimeError(
                "failure must be "
                "ProductionAiStartupFailure."
            )

        temporary = self.path.with_suffix(
            ".json.tmp"
        )
        temporary.write_text(
            json.dumps(
                failure.to_record(),
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.path)

        return self.path

    def load(
        self,
    ) -> ProductionAiStartupFailure:
        """Load and validate the persisted startup failure."""
        if not self.path.is_file():
            raise FileNotFoundError(
                "Missing production AI startup failure: "
                f"{self.path}"
            )

        try:
            record = json.loads(
                self.path.read_text(
                    encoding="utf-8"
                )
            )
        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:
            raise ProductionAiRuntimeError(
                "Production AI startup failure "
                "could not be read."
            ) from exc

        return (
            ProductionAiStartupFailure
            .from_record(record)
        )


class JsonProductionAiRuntimeStore:
    """Atomic persistence for the non-secret runtime identity."""

    def __init__(
        self,
        state_directory: Path | str,
    ) -> None:
        self.state_directory = Path(
            state_directory
        )
        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.path = (
            self.state_directory
            / PRODUCTION_AI_RUNTIME_FILENAME
        )

    def save(
        self,
        runtime: ProductionAiRuntime,
    ) -> Path:
        """Persist one validated runtime identity."""
        if not isinstance(
            runtime,
            ProductionAiRuntime,
        ):
            raise ProductionAiRuntimeError(
                "runtime must be ProductionAiRuntime."
            )

        temporary = self.path.with_suffix(
            ".json.tmp"
        )
        temporary.write_text(
            json.dumps(
                runtime.to_record(),
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.path)

        return self.path

    def load(self) -> ProductionAiRuntime:
        """Load and validate the persisted runtime identity."""
        if not self.path.is_file():
            raise FileNotFoundError(
                "Missing production AI runtime: "
                f"{self.path}"
            )

        try:
            record = json.loads(
                self.path.read_text(
                    encoding="utf-8"
                )
            )
        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:
            raise ProductionAiRuntimeError(
                "Production AI runtime could not be read."
            ) from exc

        return ProductionAiRuntime.from_record(
            record
        )
