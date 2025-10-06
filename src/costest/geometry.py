"""Geometry parsing helpers for pay-item descriptions.

Supported patterns:
- Rectangles: "9' x 6'", "12 IN x 18 IN", "9 FT X 6 FT"
- Circles: "\u00d8 42 IN", "DIAMETER 36\"", "DIA 3 FT"
- Minimum area descriptors: "MIN AREA 8.5 SFT"

All results are returned in square feet for downstream comparisons.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Optional

RECT_PATTERN = re.compile(
    r"(?P<a>\d+(?:\.\d+)?)\s*(?P<a_unit>FT|FEET|FOOT|F|'|IN|INCH|INCHES|\")?\s*[x\u00d7X]\s*(?P<b>\d+(?:\.\d+)?)\s*(?P<b_unit>FT|FEET|FOOT|F|'|IN|INCH|INCHES|\")?",
    re.IGNORECASE,
)

CIRCLE_PATTERN = re.compile(
    r"(?:\u00d8|DIAM(?:ETER)?|DIA)\s*(?P<diameter>\d+(?:\.\d+)?)\s*(?P<unit>FT|FEET|FOOT|F|'|IN|INCH|INCHES|\")",
    re.IGNORECASE,
)

MIN_AREA_PATTERN = re.compile(
    r"MIN\s+AREA\s+(?P<area>\d+(?:\.\d+)?)\s*(?:SQ\s*FT|SFT|SF|FT\^?2|FT2)",
    re.IGNORECASE,
)


@dataclass
class GeometryInfo:
    shape: str
    area_sqft: float
    source_text: str
    dimensions: str | None = None


_IN_UNITS = {"IN", "INCH", "INCHES", '"'}


def _length_to_feet(value: float, unit: str | None) -> float:
    unit = (unit or "").strip().upper()
    if unit in _IN_UNITS:
        return value / 12.0
    return value


def _parse_rectangle(match: re.Match[str], description: str) -> GeometryInfo:
    a_val = float(match.group("a"))
    b_val = float(match.group("b"))
    a_unit = match.group("a_unit")
    b_unit = match.group("b_unit")
    a_ft = _length_to_feet(a_val, a_unit)
    b_ft = _length_to_feet(b_val, b_unit)
    area = a_ft * b_ft
    dims = f"{a_ft:.4g} ft x {b_ft:.4g} ft"
    return GeometryInfo(shape="rectangle", area_sqft=area, source_text=description, dimensions=dims)


def _parse_circle(match: re.Match[str], description: str) -> GeometryInfo:
    diameter = float(match.group("diameter"))
    unit = match.group("unit")
    diameter_ft = _length_to_feet(diameter, unit)
    radius_ft = diameter_ft / 2.0
    area = math.pi * radius_ft * radius_ft
    dims = f"diameter {diameter_ft:.4g} ft"
    return GeometryInfo(shape="circle", area_sqft=area, source_text=description, dimensions=dims)


def _parse_min_area(match: re.Match[str], description: str) -> GeometryInfo:
    area = float(match.group("area"))
    return GeometryInfo(shape="min_area", area_sqft=area, source_text=description, dimensions=None)


def parse_geometry(description: str) -> Optional[GeometryInfo]:
    """Extract geometric information from a pay-item description."""
    if not description:
        return None

    text = description.strip()
    if not text:
        return None

    rect_match = RECT_PATTERN.search(text)
    if rect_match:
        return _parse_rectangle(rect_match, text)

    circle_match = CIRCLE_PATTERN.search(text)
    if circle_match:
        return _parse_circle(circle_match, text)

    min_area_match = MIN_AREA_PATTERN.search(text)
    if min_area_match:
        return _parse_min_area(min_area_match, text)

    return None
