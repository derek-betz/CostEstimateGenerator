
"""Legacy script used for one-off CLI tweaks during development."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
cli_path = REPO_ROOT / 'src/costest/cli.py'
text = cli_path.read_text()
text = text.replace('"quantities_file": str(Path(qty_path).resolve()),', '"quantities_file": str(qty_path),')
cli_path.write_text(text)
