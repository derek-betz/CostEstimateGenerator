"""
Static mappings between design memo replacements and obsolete pay items.

This module serves as a simple, hard-coded lookup so the pricing pipeline
can roll legacy bid data forward to newly issued pay item codes when
category lookups fail.  The mapping is intentionally compact and easy to
maintain by hand.  See the README for guidance on extending it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .bidtabs_io import normalize_item_code


@dataclass(frozen=True)
class DesignMemoMapping:
    """Container describing a replacement pay item and its obsolete sources."""

    memo_id: str
    effective_date: str
    obsolete_codes: List[str]


# Keep the initial mapping small but explicit so future updates are easy.
_DESIGN_MEMO_MAP: Dict[str, DesignMemoMapping] = {
    # Design Memo 25-10 (2025-04-16)
    normalize_item_code("401-11526"): DesignMemoMapping(
        memo_id="25-10",
        effective_date="2025-04-16",
        obsolete_codes=[
            normalize_item_code("401-10258"),
            normalize_item_code("401-10259"),
        ],
    ),
}


def get_obsolete_mapping(item_code: str) -> Optional[Dict[str, object]]:
    """
    Return design memo metadata for a replacement pay item code.

    Parameters
    ----------
    item_code:
        Target pay item code from the project quantities sheet.

    Returns
    -------
    dict | None
        Dictionary with keys ``memo_id``, ``effective_date``, and
        ``obsolete_codes`` when the given item has an associated design memo
        rollup, or ``None`` when no mapping is defined.

    Notes
    -----
    - Codes are normalized before lookup, so callers can pass values with
      or without dashes.
    - Future updates should extend ``_DESIGN_MEMO_MAP`` above and keep this
      function unchanged.  TODO: Consider loading mappings from an external
      spreadsheet once additional memos are published.
    """

    code = normalize_item_code(item_code)
    mapping = _DESIGN_MEMO_MAP.get(code)
    if mapping is None:
        return None
    return {
        "memo_id": mapping.memo_id,
        "effective_date": mapping.effective_date,
        "obsolete_codes": list(mapping.obsolete_codes),
    }


__all__ = ["get_obsolete_mapping"]
