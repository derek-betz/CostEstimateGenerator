"""
Parse processed design memo summaries to extract recommended unit prices.

This module inspects the structured JSON outputs produced by the memo parser
and derives pay-item level price guidance when the memo text explicitly calls
out a unit price (e.g. "unit price of $2.22 per SYS").  The extracted guidance
is cached in-memory so downstream pricing routines can apply memo-directed
estimates without reparsing files on every lookup.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .bidtabs_io import normalize_item_code

DEFAULT_PROCESSED_DIRECTORY = Path("references/memos/processed")

PRICE_PATTERN = re.compile(
    r"unit\s+price(?:\s+of|\s+as)?\s*\$?\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
CODE_PATTERN = re.compile(r"\b\d{3}-\d{5,6}[A-Za-z]?\b")
UNIT_PATTERN = re.compile(r"\bper\s+([A-Za-z\/\-]{1,15})", re.IGNORECASE)


# Expand window size so codes in memo tables that appear a few hundred
# characters away from the price reference are still captured.
WINDOW_RADIUS = 600


@dataclass(frozen=True)
class MemoPriceGuidance:
    """Structured memo guidance tying a pay item to a recommended price."""

    memo_id: str
    price: float
    unit: Optional[str]
    context: str
    effective_date: Optional[str]
    extracted_at: Optional[str]
    source_path: Optional[Path]


def lookup_memo_price(item_code: str, processed_dir: Path | None = None) -> Optional[MemoPriceGuidance]:
    """
    Return memo price guidance for ``item_code`` if available.

    Parameters
    ----------
    item_code:
        Pay item code to look up (with or without hyphen).
    processed_dir:
        Optional override directory containing processed memo JSON payloads.

    Returns
    -------
    MemoPriceGuidance | None
        Extracted memo guidance or ``None`` when no price recommendation exists.
    """

    normalized = normalize_item_code(item_code)
    if not normalized:
        return None
    guidance = _load_guidance_cache(processed_dir)
    return guidance.get(normalized)


@lru_cache(maxsize=None)
def _load_guidance_cache(processed_dir: Path | None) -> Dict[str, MemoPriceGuidance]:
    directory = processed_dir or DEFAULT_PROCESSED_DIRECTORY
    try:
        directory = directory.resolve()
    except OSError:
        return {}
    if not directory.exists():
        return {}

    guidance_map: Dict[str, MemoPriceGuidance] = {}
    for json_path in sorted(directory.glob("*.json")):
        try:
            with json_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            continue
        memo_id = _coerce_str(payload.get("metadata", {}).get("memo_id")) or json_path.stem
        extracted_at = _coerce_str(payload.get("metadata", {}).get("extracted_at"))
        effective_date = _coerce_str(payload.get("metadata", {}).get("effective_date"))
        text = _collect_text_segments(payload)
        if not text:
            continue
        for normalized_code, price_entry in _extract_guidance_entries(
            text,
            memo_id=memo_id,
            effective_date=effective_date,
            extracted_at=extracted_at,
            source_path=json_path,
        ):
            if math.isnan(price_entry.price) or price_entry.price <= 0:
                continue
            existing = guidance_map.get(normalized_code)
            if existing is None or _is_candidate_newer(price_entry, existing):
                guidance_map[normalized_code] = price_entry

    return guidance_map


def _extract_guidance_entries(
    text: str,
    *,
    memo_id: str,
    effective_date: Optional[str],
    extracted_at: Optional[str],
    source_path: Path,
) -> Iterable[Tuple[str, MemoPriceGuidance]]:
    if not text:
        return []

    results: List[Tuple[str, MemoPriceGuidance]] = []
    for match in PRICE_PATTERN.finditer(text):
        raw_value = match.group("value")
        if not raw_value:
            continue
        try:
            price_value = float(raw_value.replace(",", ""))
        except ValueError:
            continue

        window = text[max(0, match.start() - WINDOW_RADIUS) : match.end() + WINDOW_RADIUS]
        codes = {normalize_item_code(code) for code in CODE_PATTERN.findall(window)}
        codes.discard("")
        if not codes:
            continue

        unit_match = UNIT_PATTERN.search(window)
        unit = unit_match.group(1).upper() if unit_match else None
        cleaned_context = " ".join(window.split())
        for code in codes:
            results.append(
                (
                    code,
                    MemoPriceGuidance(
                        memo_id=memo_id,
                        price=price_value,
                        unit=unit,
                        context=cleaned_context[:300],
                        effective_date=effective_date,
                        extracted_at=extracted_at,
                        source_path=source_path,
                    ),
                )
            )
    return results


def _collect_text_segments(payload: dict) -> str:
    snippets: List[str] = []
    metadata = payload.get("metadata") or {}
    highlights = payload.get("highlights") or {}

    for key in ("title", "memo_id", "effective_date"):
        value = _coerce_str(metadata.get(key))
        if value:
            snippets.append(value)

    pay_items = highlights.get("pay_items")
    if isinstance(pay_items, list) and pay_items:
        snippets.append(" ".join(str(item) for item in pay_items if item))

    for snippet in payload.get("snippets") or []:
        value = _coerce_str(snippet)
        if value:
            snippets.append(value)

    return "\n".join(snippets)


def _coerce_str(value) -> Optional[str]:
    if isinstance(value, str):
        return value.strip()
    return None


def _is_candidate_newer(candidate: MemoPriceGuidance, existing: MemoPriceGuidance) -> bool:
    cand_priority = _guidance_priority(candidate)
    exist_priority = _guidance_priority(existing)
    return cand_priority > exist_priority


def _guidance_priority(entry: MemoPriceGuidance) -> Tuple[datetime, bool]:
    if entry.effective_date:
        parsed = _try_parse_date(entry.effective_date)
        if parsed:
            return parsed, True
    if entry.extracted_at:
        parsed = _try_parse_extracted(entry.extracted_at)
        if parsed:
            return parsed, False
    return datetime.min, False


def _try_parse_date(value: str) -> Optional[datetime]:
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _try_parse_extracted(value: str) -> Optional[datetime]:
    value = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


__all__ = ["MemoPriceGuidance", "lookup_memo_price"]
