"""Cost Estimate Generator package."""

from importlib.metadata import PackageNotFoundError, version

try:  # pragma: no cover - best effort metadata lookup
    __version__ = version("costest")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["__version__"]
