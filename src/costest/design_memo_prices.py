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
from . import reference_data
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .bidtabs_io import normalize_item_code
from . import reference_data

DEFAULT_PROCESSED_DIRECTORY = Path("references/memos/processed")

PRICE_PATTERN = re.compile(
    r"(?:unit\s+price|price)\s*(?:of|as|is)?\s*\$?\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
FALLBACK_DOLLAR_PATTERN = re.compile(r"\$\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)")
CODE_PATTERN = re.compile(r"\b\d{3}-\d{5,6}[A-Za-z]?\b")
UNIT_PATTERN = re.compile(r"\bper\s+([A-Za-z\/\-]{1,15})", re.IGNORECASE)
UNIT_TOKENS = {
    # Common INDOT units (uppercased)
    "EA",
    "EACH",
    "LFT",
    "FT",
    "SFT",
    "SQFT",
    "SYD",
    "SYS",
    "LS",
    "LUMP",
    "TON",
    "TONS",
    "CY",
    "CYS",
    "YD",
    "LF",
    "SF",
}

# When scanning memo text, use a wider window to associate pay-item descriptions.
DESCRIPTION_WINDOW = 600


# Expand window size so codes in memo tables that appear a few hundred
# characters away from the price reference are still captured.
WINDOW_RADIUS = 900


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
    confidence: Optional[float] = 1.0

MANUAL_GUIDANCE_OVERRIDES: Dict[str, MemoPriceGuidance] = {
    "629-000149": MemoPriceGuidance(
        memo_id="dm-2025-07-20topsoil-20management",
        price=2.22,
        unit="SYS",
        context=(
            "DM 25-07 Topsoil Management: For estimating purposes, a unit price of $2.22 per SYS "
            "should be used until a bid history is established."
        ),
        effective_date="September 1, 2025",
        extracted_at="2025-10-20T21:54:31-0600",
        source_path=Path("references/memos/digests/dm-2025-07-20topsoil-20management.md"),
    ),
    "629-000150": MemoPriceGuidance(
        memo_id="dm-2025-07-20topsoil-20management",
        price=1.00,
        unit="DOL",
        context="DM 25-07 Topsoil Management: Topsoil Amendment Budget is set at $1.00 per DOL.",
        effective_date="September 1, 2025",
        extracted_at="2025-10-20T21:54:31-0600",
        source_path=Path("references/memos/digests/dm-2025-07-20topsoil-20management.md"),
    ),
}

@dataclass(frozen=True)
class CodeMetadata:
    """Holds memo-specific context for a pay item code."""

    positions: Tuple[int, ...]
    unit: Optional[str]
    description: Optional[str]
    keywords: Tuple[str, ...]


@dataclass(frozen=True)
class GuidanceMatch:
    """Captures a guidance candidate alongside scoring metadata."""

    guidance: MemoPriceGuidance
    distance: int
    code_unit: Optional[str]
    keyword_pre: int
    keyword_post: int
    desc_pre: bool
    desc_post: bool


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
    override = MANUAL_GUIDANCE_OVERRIDES.get(normalized)
    if override is not None:
        return override
    guidance = _load_guidance_cache(processed_dir)
    entry = guidance.get(normalized)
    # If unit mismatches catalog, reduce confidence or drop (leave caller to decide)
    if entry is not None:
        try:
            catalog = reference_data.load_payitem_catalog()
            expected_unit = str((catalog.get(normalized, {}) or {}).get("unit", "")).strip().upper()
            if expected_unit and entry.unit and expected_unit != entry.unit.upper():
                # degrade confidence but keep guidance for caller to evaluate
                conf = 0.6 if (entry.confidence or 1.0) > 0.6 else (entry.confidence or 0.6)
                return MemoPriceGuidance(
                    memo_id=entry.memo_id,
                    price=entry.price,
                    unit=entry.unit,
                    context=entry.context,
                    effective_date=entry.effective_date,
                    extracted_at=entry.extracted_at,
                    source_path=entry.source_path,
                    confidence=conf,
                )
        except Exception:
            pass
    return entry


@lru_cache(maxsize=None)
def _load_guidance_cache(processed_dir: Path | None) -> Dict[str, MemoPriceGuidance]:
    directory = processed_dir or DEFAULT_PROCESSED_DIRECTORY
    try:
        directory = directory.resolve()
    except OSError:
        return {}
    if not directory.exists():
        return {}

    guidance_map: Dict[str, GuidanceMatch] = {}
    for json_path in sorted(directory.glob("*.json")):
        try:
            with json_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            continue
        memo_id = _coerce_str(payload.get("metadata", {}).get("memo_id")) or json_path.stem
        extracted_at = _coerce_str(payload.get("metadata", {}).get("extracted_at"))
        effective_date = _coerce_str(payload.get("metadata", {}).get("effective_date"))
        # Try robust extraction in this order:
        # 1) Full source PDF text (best chance to see price-code proximity)
        # 2) Aggregated processed JSON snippets (fast path)

        texts_to_scan: List[Tuple[str, Path]] = []
        # Collect PDF text if available
        src_pdf = _coerce_str(payload.get("metadata", {}).get("source_pdf"))
        if src_pdf:
            pdf_path = Path(src_pdf)
            if pdf_path.exists():
                try:
                    pdf_text = _extract_pdf_text(pdf_path)
                    if pdf_text:
                        texts_to_scan.append((pdf_text, pdf_path))
                except Exception:
                    pass

        # Always include processed snippets as a secondary source
        processed_text = _collect_text_segments(payload)
        if processed_text:
            texts_to_scan.append((processed_text, json_path))

        if not texts_to_scan:
            continue

        found_any = False
        for text, src in texts_to_scan:
            for normalized_code, candidate in _extract_guidance_entries(
                text,
                memo_id=memo_id,
                effective_date=effective_date,
                extracted_at=extracted_at,
                source_path=src,
            ):
                if math.isnan(candidate.guidance.price) or candidate.guidance.price <= 0:
                    continue
                existing = guidance_map.get(normalized_code)
                if existing is None or _prefer_candidate(candidate, existing):
                    guidance_map[normalized_code] = candidate
                    found_any = True
            # If we found guidance from the stronger source (PDF), we can skip fallback
            if found_any:
                break

    return {code: match.guidance for code, match in guidance_map.items()}


def _extract_guidance_entries(
    text: str,
    *,
    memo_id: str,
    effective_date: Optional[str],
    extracted_at: Optional[str],
    source_path: Path,
) -> Iterable[Tuple[str, GuidanceMatch]]:
    if not text:
        return []

    results: List[Tuple[str, GuidanceMatch]] = []
    code_meta = _build_code_metadata(text)
    # Pass 1: Strong phrasing matches like "unit price $X"
    for match in PRICE_PATTERN.finditer(text):
        raw_value = match.group("value")
        if not raw_value:
            continue
        try:
            price_value = float(raw_value.replace(",", ""))
        except ValueError:
            continue

        start = max(0, match.start() - WINDOW_RADIUS)
        end = min(len(text), match.end() + WINDOW_RADIUS)
        window = text[start:end]
        cleaned_context = " ".join(window.split())
        unit_match = UNIT_PATTERN.search(window)
        price_unit = unit_match.group(1).upper() if unit_match else None
        pre_segment = text[max(0, match.start() - DESCRIPTION_WINDOW) : match.start()].upper()
        post_segment = text[match.end() : match.end() + DESCRIPTION_WINDOW].upper()

        code_scores: Dict[str, GuidanceMatch] = {}
        for code, meta in code_meta.items():
            if not meta.positions:
                continue
            distance = min(abs(pos - match.start()) for pos in meta.positions)
            if distance > WINDOW_RADIUS:
                continue
            if price_unit and meta.unit and price_unit != meta.unit:
                continue
            guidance = MemoPriceGuidance(
                memo_id=memo_id,
                price=price_value,
                unit=price_unit,
                context=cleaned_context[:300],
                effective_date=effective_date,
                extracted_at=extracted_at,
                source_path=source_path,
            )
            keyword_pre = sum(1 for kw in meta.keywords if kw in pre_segment)
            keyword_post = sum(1 for kw in meta.keywords if kw in post_segment)
            description_upper = (meta.description or "").upper()
            desc_pre = bool(description_upper and description_upper in pre_segment)
            desc_post = bool(description_upper and description_upper in post_segment)
            code_scores[code] = GuidanceMatch(
                guidance=guidance,
                distance=distance,
                code_unit=meta.unit,
                keyword_pre=keyword_pre,
                keyword_post=keyword_post,
                desc_pre=desc_pre,
                desc_post=desc_post,
            )

        if not code_scores:
            continue

        results.extend(code_scores.items())

    # Pass 2: Fallback proximity scan around each pay item code when strong phrasing is absent.
    # For each code occurrence, search a nearby window for a $value and a unit cue.
    # This helps capture table-style memos where the column label provides the semantics.
    if not results:
        for code, meta in code_meta.items():
            if not meta.positions:
                continue
            code_pos = meta.positions[0]
            start = max(0, code_pos - WINDOW_RADIUS)
            end = min(len(text), code_pos + WINDOW_RADIUS)
            window = text[start:end]

            # Look for the closest $ to the code occurrence
            closest_price = None
            closest_dist = None
            unit: Optional[str] = None

            for m in FALLBACK_DOLLAR_PATTERN.finditer(window):
                raw_value = m.group("value")
                try:
                    price_value = float(raw_value.replace(",", ""))
                except ValueError:
                    continue
                # Heuristic: skip very large values likely to be totals rather than unit prices
                if price_value > 1_000_000:
                    continue
                # Require some unit cue nearby: "per <unit>" or a unit token
                local = window[max(0, m.start() - 80) : m.end() + 80]
                unit_match = UNIT_PATTERN.search(local)
                inferred_unit = unit_match.group(1).upper() if unit_match else None
                if not inferred_unit:
                    # Try bare unit tokens
                    tokens = re.findall(r"[A-Za-z]{2,6}", local.upper())
                    for t in tokens:
                        if t in UNIT_TOKENS:
                            inferred_unit = t
                            break
                if not inferred_unit:
                    # As a last resort, require that the window contains a header cue
                    header_cue = re.search(r"UNIT\s+PRICE|PRICE\s+EACH|PRICE\s*/", window, re.IGNORECASE)
                    if not header_cue:
                        continue
                # Compute distance from code to this $ value within the window
                dist = abs((start + m.start()) - code_pos)
                if closest_dist is None or dist < closest_dist:
                    closest_dist = dist
                    closest_price = price_value
                    unit = inferred_unit

            if closest_price is not None and closest_price > 0:
                cleaned_context = " ".join(window.split())
                if unit and meta.unit and unit != meta.unit:
                    continue
                pre_segment = text[max(0, code_pos - DESCRIPTION_WINDOW) : code_pos].upper()
                post_segment = text[code_pos : code_pos + DESCRIPTION_WINDOW].upper()
                keyword_pre = sum(1 for kw in meta.keywords if kw in pre_segment)
                keyword_post = sum(1 for kw in meta.keywords if kw in post_segment)
                description_upper = (meta.description or "").upper()
                desc_pre = bool(description_upper and description_upper in pre_segment)
                desc_post = bool(description_upper and description_upper in post_segment)
                results.append(
                    (
                        code,
                        GuidanceMatch(
                            guidance=MemoPriceGuidance(
                                memo_id=memo_id,
                                price=closest_price,
                                unit=unit,
                                context=cleaned_context[:300],
                                effective_date=effective_date,
                                extracted_at=extracted_at,
                                source_path=source_path,
                            ),
                            distance=closest_dist if closest_dist is not None else WINDOW_RADIUS,
                            code_unit=meta.unit,
                            keyword_pre=keyword_pre,
                            keyword_post=keyword_post,
                            desc_pre=desc_pre,
                            desc_post=desc_post,
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


def _extract_pdf_text(pdf_path: Path, *, max_pages: int = 40, max_chars: int = 200_000) -> str:
    """Extract a bounded amount of text from a PDF for guidance scanning.

    Limits the number of pages and characters to avoid excessive memory/time.
    Uses pypdf if available; otherwise returns an empty string.
    """
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return ""

    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return ""

    parts: List[str] = []
    total = 0
    for i, page in enumerate(reader.pages):
        if max_pages and i >= max_pages:
            break
        try:
            chunk = page.extract_text() or ""
        except Exception:
            chunk = ""
        if not chunk:
            continue
        parts.append(chunk)
        total += len(chunk)
        if max_chars and total >= max_chars:
            break
    text = "\n".join(parts)
    if max_chars and len(text) > max_chars:
        return text[:max_chars]
    return text


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


    def _score_confidence(*, distance: int, unit_match: bool, keyword_pre: int, keyword_post: int, desc_pre: bool, desc_post: bool) -> float:
        """Heuristic confidence score in [0,1] for memo price extraction."""
        # Base from proximity (closer is better)
        prox = max(0.0, min(1.0, 1.0 - (distance / float(WINDOW_RADIUS or 1))))
        unit = 1.0 if unit_match else 0.7
        kw = min(1.0, 0.2 * (keyword_pre + keyword_post))
        desc = 0.2 if (desc_pre or desc_post) else 0.0
        conf = 0.4 * prox + 0.3 * unit + 0.2 * kw + 0.1 * desc
        return float(max(0.0, min(1.0, conf)))
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


_STOPWORDS = {
    "THE",
    "AND",
    "THIS",
    "THAT",
    "WITH",
    "FROM",
    "FOR",
    "ITEM",
    "PAY",
    "UNIT",
    "NOTE",
}


def _build_code_metadata(text: str) -> Dict[str, CodeMetadata]:
    """Index pay item occurrences, units, and keywords for scoring."""

    metadata: Dict[str, Dict[str, object]] = {}
    for match in CODE_PATTERN.finditer(text):
        code = normalize_item_code(match.group(0))
        if not code:
            continue
        entry = metadata.setdefault(
            code,
            {
                "positions": [],
                "unit": None,
                "description": None,
                "keywords": set(),
            },
        )
        entry["positions"].append(match.start())
        if entry["description"] is None:
            line_start = text.rfind("\n", 0, match.start())
            if line_start == -1:
                line_start = 0
            else:
                line_start += 1
            line_end = text.find("\n", match.start())
            if line_end == -1:
                line_end = len(text)
            line = text[line_start:line_end]
            parts = [part.strip() for part in re.split(r"\s{2,}", line) if part.strip()]
            if len(parts) >= 2:
                entry["description"] = parts[1]
                entry["keywords"] = {
                    token.upper()
                    for token in re.findall(r"[A-Za-z]{4,}", parts[1])
                    if token and token.upper() not in _STOPWORDS
                }
            if len(parts) >= 3 and entry["unit"] is None:
                candidate = re.sub(r"[^A-Za-z]", "", parts[2]).upper()
                if candidate in UNIT_TOKENS:
                    entry["unit"] = candidate

    cooked: Dict[str, CodeMetadata] = {}
    for code, payload in metadata.items():
        cooked[code] = CodeMetadata(
            positions=tuple(payload["positions"]),
            unit=payload["unit"],
            description=payload["description"],
            keywords=tuple(sorted(payload["keywords"])),
        )
    return cooked


def _prefer_candidate(candidate: GuidanceMatch, existing: GuidanceMatch) -> bool:
    """Decide whether to keep a new guidance candidate over an existing one."""

    if candidate.keyword_pre != existing.keyword_pre:
        return candidate.keyword_pre > existing.keyword_pre
    if candidate.desc_pre != existing.desc_pre:
        return candidate.desc_pre and not existing.desc_pre
    if candidate.keyword_post != existing.keyword_post:
        return candidate.keyword_post > existing.keyword_post
    if candidate.desc_post != existing.desc_post:
        return candidate.desc_post and not existing.desc_post

    candidate_matches_unit = _unit_matches(candidate)
    existing_matches_unit = _unit_matches(existing)
    if candidate_matches_unit != existing_matches_unit:
        return candidate_matches_unit and not existing_matches_unit

    if candidate.distance != existing.distance:
        return candidate.distance < existing.distance

    if candidate.guidance.unit and not existing.guidance.unit:
        return True
    if existing.guidance.unit and not candidate.guidance.unit:
        return False

    return _is_candidate_newer(candidate.guidance, existing.guidance)


def _unit_matches(entry: GuidanceMatch) -> bool:
    """Return True when the guidance unit matches the code's expected unit."""

    if not entry.guidance.unit or not entry.code_unit:
        return False
    return entry.guidance.unit.upper() == entry.code_unit.upper()


__all__ = ["MemoPriceGuidance", "lookup_memo_price"]
