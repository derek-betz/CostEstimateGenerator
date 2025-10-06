"""Utility helpers for text normalization and encoding sanitization."""

from __future__ import annotations

import re
from typing import Iterable

# Mapping of problematic Unicode characters to their ASCII-friendly equivalents.
_REPLACEMENTS = {
    "\u2018": "'",
    "\u2019": "'",
    "\u201A": "'",
    "\u201B": "'",
    "\u201C": '"',
    "\u201D": '"',
    "\u201E": '"',
    "\u2013": "-",
    "\u2014": "-",
    "\u2015": "-",
    "\u2212": "-",
    "\u2026": "...",
    "\u00A0": " ",
    "\u2007": " ",
    "\u2009": " ",
    "\u200B": "",
    "\u2060": "",
    "\uFFFD": "?",
    "\u00B1": "+/-",
    "\u00D7": "x",
}


def sanitize_text(text: str, *, ascii_only: bool = True, collapse_whitespace: bool = False) -> str:
    """Return a cleaned version of *text* safe for legacy viewers.

    Parameters
    ----------
    text:
        Input string (if `None` results in an empty string).
    ascii_only:
        When `True` encode to ASCII, dropping unknown characters after applying
        the replacement map. If `False` we preserve the Unicode string.
    collapse_whitespace:
        When `True` collapse consecutive whitespace (including newlines) to a
        single space. Useful when preparing inline log messages.
    """

    if text is None:
        return ""

    cleaned = text
    for target, repl in _REPLACEMENTS.items():
        cleaned = cleaned.replace(target, repl)

    # Replace any remaining control characters except tab/newline/carriage return.
    cleaned = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", cleaned)

    if collapse_whitespace:
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if ascii_only:
        cleaned = cleaned.encode("ascii", "ignore").decode("ascii")

    return cleaned


def sanitize_lines(lines: Iterable[str], *, ascii_only: bool = True) -> list[str]:
    """Vector helper that applies :func:sanitize_text to every line."""
    return [sanitize_text(line, ascii_only=ascii_only) for line in lines]
