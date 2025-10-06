from __future__ import annotations

import sys
from pathlib import Path

# Ensure the src directory is importable without requiring installation.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Ensure the src directory is importable without requiring installation.
# Avoid inserting the repository root to prevent shadowing site-packages (e.g., openpyxl).
src_path = PROJECT_ROOT / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
