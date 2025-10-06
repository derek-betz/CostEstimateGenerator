from pathlib import Path

path = Path('pyproject.toml')
text = path.read_text()
old = "dependencies = [\n    \"numpy==1.26.4\",\n    \"pandas==1.5.3\",\n    \"openpyxl==3.1.2\",\n    \"python-dotenv>=1.0.0,<2.0.0\",\n    \"xlrd>=2.0.1,<3.0.0\",\n    \"openai>=1.0.0,<2.0.0\",\n    \"reportlab>=4.0.0,<5.0.0\",\n]\n"
new = "dependencies = [\n    \"numpy==1.26.4\",\n    \"pandas==1.5.3\",\n    \"openpyxl==3.1.2\",\n    \"python-dotenv>=1.0.0,<2.0.0\",\n    \"xlrd>=2.0.1,<3.0.0\",\n    \"openai>=1.0.0,<2.0.0\",\n    \"reportlab>=4.0.0,<5.0.0\",\n    \"PyPDF2==3.0.1\",\n]\n"
if old not in text:
    raise SystemExit('dependency block not found in pyproject')
path.write_text(text.replace(old, new, 1))
