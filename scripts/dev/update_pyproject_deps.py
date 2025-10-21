
"""Legacy helper retained for historical troubleshooting."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

OLD_BLOCK = """dependencies = [
    "numpy==1.26.4",
    "pandas==1.5.3",
    "openpyxl==3.1.2",
    "python-dotenv>=1.0.0,<2.0.0",
    "xlrd>=2.0.1,<3.0.0",
    "openai>=1.0.0,<2.0.0",
    "reportlab>=4.0.0,<5.0.0",
]"""

NEW_BLOCK = """dependencies = [
    "numpy==1.26.4",
    "pandas==1.5.3",
    "openpyxl==3.1.2",
    "python-dotenv>=1.0.0,<2.0.0",
    "xlrd>=2.0.1,<3.0.0",
    "openai>=1.0.0,<2.0.0",
    "reportlab>=4.0.0,<5.0.0",
    "PyPDF2==3.0.1",
]"""


def main() -> None:
    text = PYPROJECT_PATH.read_text(encoding="utf-8")

    if NEW_BLOCK in text and OLD_BLOCK not in text:
        print("pyproject.toml dependencies already up to date.")
        return

    if OLD_BLOCK not in text:
        raise SystemExit("dependency block not found in pyproject.toml")

    updated = text.replace(OLD_BLOCK, NEW_BLOCK, 1)
    if updated == text:
        print("No changes applied; pyproject already current.")
        return

    PYPROJECT_PATH.write_text(updated, encoding="utf-8")
    print("Updated dependency block in pyproject.toml.")


if __name__ == "__main__":
    main()
