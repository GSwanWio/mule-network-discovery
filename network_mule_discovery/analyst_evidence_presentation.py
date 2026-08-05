"""Analyst-facing strongest-evidence presentation helpers."""

from __future__ import annotations


BULLET_PREFIXES = ("•", "-", "*")


def strongest_evidence_items(
    value: object,
) -> tuple[str, ...]:
    """Return clean evidence items in their persisted order."""
    if value is None:
        return ()

    raw_value = str(value).strip()
    if not raw_value:
        return ()

    items: list[str] = []
    for raw_line in raw_value.splitlines():
        item = raw_line.strip()
        while item.startswith(BULLET_PREFIXES):
            item = item[1:].strip()

        if item:
            items.append(item)

    if items:
        return tuple(items)

    return (raw_value,)
