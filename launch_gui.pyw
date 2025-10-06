import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if SRC.exists():
    sys.path.insert(0, str(SRC))

from costest.gui import main

if __name__ == "__main__":
    main()
