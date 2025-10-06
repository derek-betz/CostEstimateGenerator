"""
Shared metadata for project-level inputs surfaced in the GUI.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

# Keep tuple structure to preserve order for UI display
DISTRICT_CHOICES: Tuple[Tuple[int, str], ...] = (
    (1, "CRAWFORDSVILLE"),
    (2, "FORT WAYNE"),
    (3, "GREENFIELD"),
    (4, "LAPORTE"),
    (5, "SEYMOUR"),
    (6, "VINCENNES"),
)

DISTRICT_REGION_MAP = {name: number for number, name in DISTRICT_CHOICES}
REGION_DISTRICT_MAP = {number: name for number, name in DISTRICT_CHOICES}


def district_display_strings() -> List[str]:
    """Return formatted strings like ``\"1 - CRAWFORDSVILLE\"`` for UI dropdowns."""

    return [f"{number} - {name}" for number, name in DISTRICT_CHOICES]


def normalize_district(value: str) -> Optional[str]:
    """
    Normalize a district string/number into the canonical district name.

    Accepts inputs in the form \"1\", \"1 - Crawfordsville\", or \"Crawfordsville\".
    Returns ``None`` if the value cannot be mapped.
    """

    if not value:
        return None

    candidate = value.strip()
    if not candidate:
        return None

    # Fast path: formatted display string "1 - NAME"
    if "-" in candidate:
        first, *_rest = candidate.split("-", 1)
        first = first.strip()
    else:
        first = candidate

    if first.isdigit():
        number = int(first)
        name = REGION_DISTRICT_MAP.get(number)
        if name:
            return name

    canonical = candidate.upper()
    if canonical in DISTRICT_REGION_MAP:
        return canonical

    compressed = canonical.replace(" ", "")
    for name in DISTRICT_REGION_MAP:
        if compressed == name.replace(" ", ""):
            return name
    return None


def district_to_region(value: str) -> Optional[int]:
    """Convert a district string/number into its region id."""

    name = normalize_district(value)
    if not name:
        return None
    return DISTRICT_REGION_MAP.get(name)


def iter_region_map_rows() -> Iterable[tuple[str, int]]:
    """Yield ``(DISTRICT, REGION)`` rows suitable for a DataFrame constructor."""

    for number, name in DISTRICT_CHOICES:
        yield name, number

