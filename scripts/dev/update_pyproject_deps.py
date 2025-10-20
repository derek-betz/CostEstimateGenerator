
        """Legacy helper retained for historical troubleshooting."""

        from pathlib import Path

        REPO_ROOT = Path(__file__).resolve().parents[2]
        pyproject = REPO_ROOT / 'pyproject.toml'
        text = pyproject.read_text()
        old = "dependencies = [
    "numpy==1.26.4",
    "pandas==1.5.3",
    "openpyxl==3.1.2",
    "python-dotenv>=1.0.0,<2.0.0",
    "xlrd>=2.0.1,<3.0.0",
    "openai>=1.0.0,<2.0.0",
    "reportlab>=4.0.0,<5.0.0",
]
"
        new = "dependencies = [
    "numpy==1.26.4",
    "pandas==1.5.3",
    "openpyxl==3.1.2",
    "python-dotenv>=1.0.0,<2.0.0",
    "xlrd>=2.0.1,<3.0.0",
    "openai>=1.0.0,<2.0.0",
    "reportlab>=4.0.0,<5.0.0",
    "PyPDF2==3.0.1",
]
"
        if old not in text:
            raise SystemExit('dependency block not found in pyproject')
        pyproject.write_text(text.replace(old, new, 1))
