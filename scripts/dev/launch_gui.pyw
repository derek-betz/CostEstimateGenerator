
"""Legacy helper for launching the GUI in development environments."""

This shim preserves the historical double-click workflow on Windows by
importing the canonical console entry point (costest.gui.main).
"""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if SRC.exists() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from costest.gui import main as gui_main

if __name__ == "__main__":  # pragma: no cover - manual helper
    raise SystemExit(gui_main())
