from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping, Optional


_BOOLEAN_TRUE = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    """Runtime configuration assembled from environment variables and CLI options."""

    base_dir: Path
    bidtabs_dir: Path
    quantities_glob: str
    quantities_path: Optional[Path]
    project_attributes: Path
    region_map_path: Optional[Path]
    aliases_csv: Path
    output_dir: Path
    output_xlsx: Path
    output_audit: Path
    output_payitem_audit: Path
    disable_ai: bool
    disable_alt_seek: bool
    min_sample_target: int
    contract_filter_pct: Optional[float]
    expected_contract_cost: Optional[float]
    project_region: Optional[int]
    project_district: Optional[str]
    legacy_expected_cost_path: Optional[Path]
    apply_dm23_21: bool = False
    verbose: bool = False


def _to_path(value: object | None) -> Optional[Path]:
    if value is None:
        return None
    if isinstance(value, Path):
        return value.expanduser().resolve()
    text = str(value).strip()
    if not text:
        return None
    return Path(text).expanduser().resolve()


def _to_int(value: object | None) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _to_float(value: object | None) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace("$", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _flag(value: object | None) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _BOOLEAN_TRUE


def _namespace(cli_args: object | None) -> SimpleNamespace:
    if cli_args is None:
        return SimpleNamespace()
    if isinstance(cli_args, SimpleNamespace):
        return cli_args
    if hasattr(cli_args, "__dict__"):
        return SimpleNamespace(**{k: v for k, v in vars(cli_args).items()})
    return SimpleNamespace()


def load_config(env: Mapping[str, str], cli_args: object | None = None) -> Config:
    """Build a runtime :class:`Config` from environment variables and CLI options."""

    base_dir = Path(__file__).resolve().parents[2]
    data_dir = base_dir / "data_sample"
    default_bidtabs = (data_dir / "BidTabsData").resolve()
    default_quantities_glob = str((data_dir / "*_project_quantities.xlsx").resolve())
    default_project_attrs = (data_dir / "project_attributes.xlsx").resolve()
    default_aliases = (data_dir / "code_aliases.csv").resolve()
    default_output_dir = (base_dir / "outputs").resolve()
    default_region_map = (base_dir / "references" / "region_map.xlsx").resolve()

    bidtabs_dir = _to_path(env.get("BIDTABS_DIR")) or default_bidtabs
    quantities_glob = env.get("QTY_FILE_GLOB", default_quantities_glob)
    quantities_path = _to_path(env.get("QUANTITIES_XLSX"))
    project_attributes = _to_path(env.get("PROJECT_ATTRS_XLSX")) or default_project_attrs
    region_map_path = _to_path(env.get("REGION_MAP_XLSX"))
    if region_map_path is None and default_region_map.exists():
        region_map_path = default_region_map
    aliases_csv = _to_path(env.get("ALIASES_CSV")) or default_aliases
    output_dir = _to_path(env.get("OUTPUT_DIR")) or default_output_dir
    output_xlsx = _to_path(env.get("OUTPUT_XLSX")) or (output_dir / "Estimate_Draft.xlsx").resolve()
    output_audit = _to_path(env.get("OUTPUT_AUDIT")) or (output_dir / "Estimate_Audit.csv").resolve()
    output_payitem_audit = _to_path(env.get("OUTPUT_PAYITEM_AUDIT")) or (output_dir / "PayItems_Audit.xlsx").resolve()
    min_sample_target = _to_int(env.get("MIN_SAMPLE_TARGET")) or 50
    disable_ai = _flag(env.get("DISABLE_OPENAI"))
    disable_alt_seek = _flag(env.get("DISABLE_ALT_SEEK"))
    apply_dm23_21 = _flag(env.get("APPLY_DM23_21"))
    contract_filter_pct = _to_float(env.get("BIDTABS_CONTRACT_FILTER_PCT"))
    expected_contract_cost = _to_float(env.get("EXPECTED_TOTAL_CONTRACT_COST"))
    project_region = _to_int(env.get("PROJECT_REGION"))
    project_district = env.get("PROJECT_DISTRICT") or None
    legacy_expected_cost_path = _to_path(env.get("EXPECTED_COST_XLSX"))
    verbose = False

    cli_ns = _namespace(cli_args)
    if getattr(cli_ns, "bidtabs_dir", None):
        bidtabs_dir = _to_path(cli_ns.bidtabs_dir) or bidtabs_dir
    if getattr(cli_ns, "quantities_xlsx", None):
        quantities_path = _to_path(cli_ns.quantities_xlsx)
    if getattr(cli_ns, "project_attributes", None):
        project_attributes = _to_path(cli_ns.project_attributes) or project_attributes
    if getattr(cli_ns, "region_map", None):
        region_map_path = _to_path(cli_ns.region_map)
    if getattr(cli_ns, "aliases_csv", None):
        aliases_csv = _to_path(cli_ns.aliases_csv) or aliases_csv
    if getattr(cli_ns, "output_dir", None):
        output_dir = _to_path(cli_ns.output_dir) or output_dir
        output_xlsx = (output_dir / "Estimate_Draft.xlsx").resolve()
        output_audit = (output_dir / "Estimate_Audit.csv").resolve()
        output_payitem_audit = (output_dir / "PayItems_Audit.xlsx").resolve()
    if getattr(cli_ns, "disable_ai", False):
        disable_ai = True
    if getattr(cli_ns, "min_sample_target", None) is not None:
        min_sample_target = max(1, int(cli_ns.min_sample_target))
    if getattr(cli_ns, "verbose", False):
        verbose = bool(cli_ns.verbose)
    if getattr(cli_ns, "apply_dm23_21", False):
        apply_dm23_21 = True

    return Config(
        base_dir=base_dir,
        bidtabs_dir=bidtabs_dir,
        quantities_glob=quantities_glob,
        quantities_path=quantities_path,
        project_attributes=project_attributes,
        region_map_path=region_map_path,
        aliases_csv=aliases_csv,
        output_dir=output_dir,
        output_xlsx=output_xlsx,
        output_audit=output_audit,
        output_payitem_audit=output_payitem_audit,
        disable_ai=disable_ai,
        disable_alt_seek=disable_alt_seek,
        min_sample_target=min_sample_target,
        contract_filter_pct=contract_filter_pct,
        expected_contract_cost=expected_contract_cost,
        project_region=project_region,
        project_district=project_district,
        legacy_expected_cost_path=legacy_expected_cost_path,
        apply_dm23_21=apply_dm23_21,
        verbose=verbose,
    )


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
        cfg = load_config(os.environ, None)
        return cls(
            base_dir=cfg.base_dir,
            bidtabs_dir=cfg.bidtabs_dir,
            quantities_glob=cfg.quantities_glob,
            quantities_path=str(cfg.quantities_path) if cfg.quantities_path else "",
            project_attributes=cfg.project_attributes,
            region_map=str(cfg.region_map_path) if cfg.region_map_path else "",
            aliases_csv=cfg.aliases_csv,
            output_dir=cfg.output_dir,
            disable_ai=cfg.disable_ai,
            min_sample_target=cfg.min_sample_target,
        )


__all__ = ["Config", "Settings", "load_config"]


@dataclass(frozen=True)
class CLIConfig:
    """Lightweight config used by tests to drive the pipeline."""

    input_payitems: Path
    estimate_audit_csv: Path
    estimate_xlsx: Path
    payitems_workbook: Path
    mapping_debug_csv: Path
    disable_ai: bool = True
    api_key_file: Optional[Path] = None
    dry_run: bool = False
    log_level: str = "INFO"


def _to_cli_path(value: object) -> Path:
    if isinstance(value, Path):
        return value
    return Path(str(value)).expanduser().resolve()


def load_cli_config(args: object) -> CLIConfig:
    """Build a CLIConfig from a SimpleNamespace-like object used by tests."""

    ns = args if isinstance(args, SimpleNamespace) else SimpleNamespace(**getattr(args, "__dict__", {}))
    return CLIConfig(
        input_payitems=_to_cli_path(getattr(ns, "input_payitems")),
        estimate_audit_csv=_to_cli_path(getattr(ns, "estimate_audit_csv")),
        estimate_xlsx=_to_cli_path(getattr(ns, "estimate_xlsx")),
        payitems_workbook=_to_cli_path(getattr(ns, "payitems_workbook")),
        mapping_debug_csv=_to_cli_path(getattr(ns, "mapping_debug_csv")),
        disable_ai=bool(getattr(ns, "disable_ai", True)),
        api_key_file=_to_cli_path(getattr(ns, "api_key_file")) if getattr(ns, "api_key_file", None) else None,
        dry_run=bool(getattr(ns, "dry_run", False)),
        log_level=str(getattr(ns, "log_level", "INFO")),
    )


__all__.extend(["CLIConfig", "load_cli_config"])
