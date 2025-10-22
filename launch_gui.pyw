"""Windows double-click entry point for the Cost Estimate Generator GUI.

This shim lets the desktop shortcut launch the Tkinter interface by importing
the canonical console script entry point.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if SRC.exists() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from costest.gui import main as gui_main
except ImportError as exc:
    raise SystemExit(
        "Unable to import costest.gui. Install project dependencies before launching the GUI."
    ) from exc

if __name__ == "__main__":  # pragma: no cover - manual launcher
    raise SystemExit(gui_main())
