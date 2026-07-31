"""Deterministic selection of supported synthetic source scenarios."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from network_mule_discovery.scenario_1_synthetic_data import (
    generate_scenario_1_source_data,
)
from network_mule_discovery.scenario_2_synthetic_data import (
    generate_scenario_2_source_data,
)
from network_mule_discovery.scenario_3_synthetic_data import (
    generate_scenario_3_source_data,
)
from network_mule_discovery.scenario_4_synthetic_data import (
    generate_scenario_4_source_data,
)
from network_mule_discovery.scenario_5_synthetic_data import (
    generate_scenario_5_source_data,
)
from network_mule_discovery.source_contracts import (
    SOURCE_DATASET_NAMES,
    SourceContractError,
)
from network_mule_discovery.synthetic_source_provider import (
    SyntheticSourceProvider,
)


ScenarioGenerator = Callable[
    [Path | str],
    dict[str, object],
]

SUPPORTED_SYNTHETIC_SCENARIOS = (
    "scenario_1",
    "scenario_2",
    "scenario_3",
    "scenario_4",
    "scenario_5",
)

_STANDARD_GENERATORS: dict[
    str,
    ScenarioGenerator,
] = {
    "scenario_1": generate_scenario_1_source_data,
    "scenario_2": generate_scenario_2_source_data,
    "scenario_3": generate_scenario_3_source_data,
    "scenario_4": generate_scenario_4_source_data,
}


def normalize_synthetic_scenario_id(
    scenario_id: object,
) -> str:
    """Return one supported normalized scenario identifier."""
    normalized = str(scenario_id).strip().lower()

    if normalized not in SUPPORTED_SYNTHETIC_SCENARIOS:
        raise SourceContractError(
            "Unsupported synthetic scenario: "
            f"{scenario_id}. Supported scenarios: "
            f"{list(SUPPORTED_SYNTHETIC_SCENARIOS)}"
        )

    return normalized


def _clear_previous_snapshot(
    output_directory: Path,
) -> None:
    """Remove known files from an earlier generated snapshot."""
    for dataset_name in SOURCE_DATASET_NAMES:
        (
            output_directory
            / f"{dataset_name}.csv"
        ).unlink(missing_ok=True)

    (
        output_directory
        / "source_manifest.json"
    ).unlink(missing_ok=True)


def generate_synthetic_scenario(
    *,
    scenario_id: str,
    output_directory: Path | str,
    changed_evidence: bool = False,
) -> dict[str, object]:
    """Generate one selected source-only synthetic scenario."""
    normalized_scenario_id = (
        normalize_synthetic_scenario_id(
            scenario_id
        )
    )

    if (
        changed_evidence
        and normalized_scenario_id != "scenario_5"
    ):
        raise SourceContractError(
            "changed_evidence is supported only "
            "for scenario_5."
        )

    resolved_output_directory = Path(
        output_directory
    )
    resolved_output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    _clear_previous_snapshot(
        resolved_output_directory
    )

    if normalized_scenario_id == "scenario_5":
        manifest = generate_scenario_5_source_data(
            resolved_output_directory,
            changed_evidence=changed_evidence,
        )
    else:
        generator = _STANDARD_GENERATORS[
            normalized_scenario_id
        ]
        manifest = generator(
            resolved_output_directory
        )

    if not isinstance(manifest, Mapping):
        raise SourceContractError(
            "Synthetic scenario generator must "
            "return a manifest mapping."
        )

    return dict(manifest)


def create_synthetic_source_provider(
    *,
    scenario_id: str,
    output_directory: Path | str,
    changed_evidence: bool = False,
) -> SyntheticSourceProvider:
    """Generate one scenario and return its source provider."""
    manifest = generate_synthetic_scenario(
        scenario_id=scenario_id,
        output_directory=output_directory,
        changed_evidence=changed_evidence,
    )

    return SyntheticSourceProvider(
        source_directory=output_directory,
        source_manifest=manifest,
    )
