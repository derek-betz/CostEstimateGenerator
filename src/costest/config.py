from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from types import SimpleNamespace
from typing import Optional

@dataclass
class Settings:
    base_dir: Path
    bidtabs_dir: Path
    quantities_glob: str
    quantities_path: str
    project_attributes: Path
    region_map: str
    aliases_csv: Path
    output_dir: Path
    disable_ai: bool
    min_sample_target: int

    @classmethod
    def from_env(cls) -> "Settings":
        base = Path(__file__).resolve().parents[2]
        data_dir = base / "data_sample"
        bidtabs_dir = Path(os.getenv("BIDTABS_DIR", str(data_dir / "BidTabsData"))).expanduser().resolve()
        quantities_glob = os.getenv("QTY_FILE_GLOB", str(data_dir / "*_project_quantities.xlsx"))
        quantities_path = os.getenv("QUANTITIES_XLSX", "").strip()
        project_attrs = Path(os.getenv("PROJECT_ATTRS_XLSX", str(data_dir / "project_attributes.xlsx"))).expanduser().resolve()
        region_map = os.getenv("REGION_MAP_XLSX", "").strip()
        aliases_csv = Path(os.getenv("ALIASES_CSV", str(data_dir / "code_aliases.csv"))).expanduser().resolve()
        output_dir = Path(os.getenv("OUTPUT_DIR", str(base / "outputs"))).expanduser().resolve()
        disable_ai = os.getenv("DISABLE_OPENAI", "0").strip().lower() in {"1", "true", "yes"}
        min_sample_target = int(os.getenv("MIN_SAMPLE_TARGET", "50"))
        return cls(
            base_dir=base,
            bidtabs_dir=bidtabs_dir,
            quantities_glob=quantities_glob,
            quantities_path=quantities_path,
            project_attributes=project_attrs,
            region_map=region_map,
            aliases_csv=aliases_csv,
            output_dir=output_dir,
            disable_ai=disable_ai,
            min_sample_target=min_sample_target,
        )


__all__ = ["Settings"]


# --- Test-facing configuration API ---

@dataclass(frozen=True)
class CLIConfig:
    """Lightweight config used by tests to drive the pipeline.

    Paths are absolute; booleans and other flags copied as-is.
    """

    input_payitems: Path
    estimate_audit_csv: Path
    estimate_xlsx: Path
    payitems_workbook: Path
    mapping_debug_csv: Path
    disable_ai: bool = True
    api_key_file: Optional[Path] = None
    dry_run: bool = False
    log_level: str = "INFO"


def _to_path(value: object) -> Path:
    if isinstance(value, Path):
        return value
    return Path(str(value)).expanduser().resolve()


def load_config(args: object) -> CLIConfig:
    """Build a CLIConfig from a SimpleNamespace-like object used by tests.

    This function intentionally accepts a generic object to match tests that
    pass a SimpleNamespace with the expected attributes.
    """
    ns = args if isinstance(args, SimpleNamespace) else SimpleNamespace(**getattr(args, "__dict__", {}))
    return CLIConfig(
        input_payitems=_to_path(getattr(ns, "input_payitems")),
        estimate_audit_csv=_to_path(getattr(ns, "estimate_audit_csv")),
        estimate_xlsx=_to_path(getattr(ns, "estimate_xlsx")),
        payitems_workbook=_to_path(getattr(ns, "payitems_workbook")),
        mapping_debug_csv=_to_path(getattr(ns, "mapping_debug_csv")),
        disable_ai=bool(getattr(ns, "disable_ai", True)),
        api_key_file=_to_path(getattr(ns, "api_key_file")) if getattr(ns, "api_key_file", None) else None,
        dry_run=bool(getattr(ns, "dry_run", False)),
        log_level=str(getattr(ns, "log_level", "INFO")),
    )


__all__.extend(["CLIConfig", "load_config"])
