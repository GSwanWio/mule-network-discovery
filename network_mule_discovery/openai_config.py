"""Runtime configuration for OpenAI decision adapters."""

from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_OPENAI_MODEL = "gpt-5-mini"
DEFAULT_PROMPT_VERSION = "mule-network-v3"
DEFAULT_TIMEOUT_SECONDS = 45.0
DEFAULT_MAX_OUTPUT_TOKENS = 4000


@dataclass(frozen=True)
class OpenAISettings:
    """Validated OpenAI runtime configuration."""

    api_key: str
    model: str
    prompt_version: str
    timeout_seconds: float
    max_output_tokens: int


def load_openai_settings() -> OpenAISettings:
    """Load OpenAI settings from environment variables."""
    api_key = os.getenv(
        "OPENAI_API_KEY",
        "",
    ).strip()

    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not configured."
        )

    model = os.getenv(
        "OPENAI_MODEL",
        DEFAULT_OPENAI_MODEL,
    ).strip()

    prompt_version = os.getenv(
        "OPENAI_PROMPT_VERSION",
        DEFAULT_PROMPT_VERSION,
    ).strip()

    timeout_seconds = float(
        os.getenv(
            "OPENAI_TIMEOUT_SECONDS",
            str(DEFAULT_TIMEOUT_SECONDS),
        )
    )

    max_output_tokens = int(
        os.getenv(
            "OPENAI_MAX_OUTPUT_TOKENS",
            str(DEFAULT_MAX_OUTPUT_TOKENS),
        )
    )

    if not model:
        raise RuntimeError(
            "OPENAI_MODEL cannot be blank."
        )

    if not prompt_version:
        raise RuntimeError(
            "OPENAI_PROMPT_VERSION cannot be blank."
        )

    if timeout_seconds <= 0:
        raise RuntimeError(
            "OPENAI_TIMEOUT_SECONDS must be positive."
        )

    if max_output_tokens <= 0:
        raise RuntimeError(
            "OPENAI_MAX_OUTPUT_TOKENS must be positive."
        )

    return OpenAISettings(
        api_key=api_key,
        model=model,
        prompt_version=prompt_version,
        timeout_seconds=timeout_seconds,
        max_output_tokens=max_output_tokens,
    )
