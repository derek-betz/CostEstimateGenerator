from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict

from .config import load_config
from .cli import run as run_pipeline


@dataclass
class EstimateOptions:
    bidtabs_dir: Optional[Path] = None
    quantities_xlsx: Optional[Path] = None
    project_attributes: Optional[Path] = None
    output_dir: Optional[Path] = None
    apply_dm23_21: bool = False
    disable_ai: bool = True


def estimate(options: EstimateOptions) -> Dict[str, Path]:
    """Programmatic interface to run the estimator and return artifact paths.

    Returns a dict with keys: xlsx, audit_csv, payitems_workbook, run_metadata.
    """
    import os

    env = dict(os.environ)
    if options.bidtabs_dir:
        env["BIDTABS_DIR"] = str(options.bidtabs_dir)
    if options.quantities_xlsx:
        env["QUANTITIES_XLSX"] = str(options.quantities_xlsx)
    if options.project_attributes:
        env["PROJECT_ATTRS_XLSX"] = str(options.project_attributes)
    if options.output_dir:
        env["OUTPUT_DIR"] = str(options.output_dir)
    if options.apply_dm23_21:
        env["APPLY_DM23_21"] = "1"
    if options.disable_ai:
        env["DISABLE_OPENAI"] = "1"

    cfg = load_config(env, None)
    rc = run_pipeline(runtime_config=cfg)
    if rc != 0:
        raise RuntimeError(f"Estimator run failed with code {rc}")
    return {
        "xlsx": cfg.output_xlsx,
        "audit_csv": cfg.output_audit,
        "payitems_workbook": cfg.output_payitem_audit,
        "run_metadata": cfg.output_dir / "run_metadata.json",
    }
