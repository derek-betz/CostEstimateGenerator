from pathlib import Path

path = Path('src/costest/cli.py')
text = path.read_text()
text = text.replace('"quantities_file": str(Path(qty_path).resolve()),', '"quantities_file": str(qty_path),')
path.write_text(text)
