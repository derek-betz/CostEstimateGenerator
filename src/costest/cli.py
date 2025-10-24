import argparse
import logging
import math
import os
import sys
import json
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

import pandas as pd
from dotenv import load_dotenv

from . import design_memo_prices, design_memos, reference_data
from .ai_reporter import generate_alternate_seek_report
from .alternate_seek import find_alternate_price
from .bidtabs_io import (
    ensure_region_column,
    find_quantities_file,
    load_bidtabs_files,
    load_quantities,
    load_region_map,
    normalize_item_code,
)
from .config import Config
from .config import load_config as load_runtime_config
from .estimate_writer import write_outputs
from .geometry import parse_geometry
from .hma_dm2321 import CrosswalkRow, load_crosswalk, maybe_apply_dm2321_adder, remap_item
from .price_logic import (
    category_breakdown,
    compute_recency_factor,
    compute_region_factor,
    memo_rollup_price,
    prepare_memo_rollup_pool,
)
from .project_meta import DISTRICT_CHOICES, DISTRICT_REGION_MAP, normalize_district
from .reporting import make_summary_text
from . import reference_data as _refdata
from .policy import apply_policy_defaults

if TYPE_CHECKING:
    from .config import CLIConfig, Config

BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = BASE_DIR / "data_sample"
DEFAULT_BIDTABS_DIR = DEFAULT_DATA_DIR / "BidTabsData"
DEFAULT_QTY_GLOB = str(DEFAULT_DATA_DIR / "*_project_quantities.xlsx")
DEFAULT_PROJECT_ATTRS = DEFAULT_DATA_DIR / "project_attributes.xlsx"
DEFAULT_ALIASES = DEFAULT_DATA_DIR / "code_aliases.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "outputs"
DEFAULT_REGION_MAP = BASE_DIR / "references" / "region_map.xlsx"



def _iter_api_key_paths():
    seen = set()
    explicit = os.getenv("OPENAI_API_KEY_FILE", "").strip()
    if explicit:
        candidate = Path(explicit).expanduser().resolve()
        if candidate not in seen:
            seen.add(candidate)
            if candidate.exists():
                yield candidate
    search_roots = [BASE_DIR, *BASE_DIR.parents, Path.cwd()]
    for base in search_roots:
        candidate = (Path(base) / "API_KEY" / "API_KEY.txt").resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            yield candidate

def _load_api_key_from_file() -> None:
    try:
        for path in _iter_api_key_paths():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    _, value = line.split("=", 1)
                else:
                    value = line
                value = value.strip()
                if value:
                    existing = os.environ.get("OPENAI_API_KEY", "")
                    if not existing.strip():
                        os.environ["OPENAI_API_KEY"] = value
                return
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Warning: unable to load API key: %s", exc)


def _resolve_path(value: Optional[str], default: Path) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return default.resolve()


_load_api_key_from_file()
load_dotenv(BASE_DIR / ".env")

DEFAULT_CONFIG = load_runtime_config(os.environ, None)

BIDFOLDER = DEFAULT_CONFIG.bidtabs_dir
QTY_FILE_GLOB = DEFAULT_CONFIG.quantities_glob
QTY_PATH = str(DEFAULT_CONFIG.quantities_path or "")
PROJECT_ATTRS_XLSX = DEFAULT_CONFIG.project_attributes
LEGACY_EXPECTED_COST_XLSX = DEFAULT_CONFIG.legacy_expected_cost_path
LEGACY_REGION_MAP_XLSX = DEFAULT_CONFIG.region_map_path
ALIASES_CSV = DEFAULT_CONFIG.aliases_csv
OUTPUT_DIR = DEFAULT_CONFIG.output_dir
OUT_XLSX = DEFAULT_CONFIG.output_xlsx
OUT_AUDIT = DEFAULT_CONFIG.output_audit
OUT_PAYITEM_AUDIT = DEFAULT_CONFIG.output_payitem_audit
MIN_SAMPLE_TARGET = DEFAULT_CONFIG.min_sample_target

logger = logging.getLogger(__name__)

CATEGORY_LABELS: Sequence[str] = (
    "DIST_12M",
    "DIST_24M",
    "DIST_36M",
    "STATE_12M",
    "STATE_24M",
    "STATE_36M",
)


def _round_unit_price(value: float) -> float:
    if value is None:
        return 0.0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(numeric):  # pragma: no cover - defensive
        return float("nan")
    if numeric <= 0:
        return 0.0
    if numeric < 1.0:
        return round(numeric, 2)
    magnitude = math.floor(math.log10(numeric))
    step = 10 ** max(magnitude - 1, -1)
    rounded = round(numeric / step) * step
    return round(rounded, 2)


def apply_non_geometry_fallbacks(
    rows: List[Dict[str, object]],
    bidtabs: pd.DataFrame,
    project_region: Optional[int],
    payitem_details: Dict[str, pd.DataFrame],
) -> None:
    """
    Apply non-geometry fallback pricing for items with no category data.

    Mutates ``rows`` in place, filling UNIT_PRICE_EST, SOURCE, NOTES, and
    DATA_POINTS_USED when either the Unit Price Summary, design memo
    price guidance, or design memo rollup can provide pricing support.
    """

    if not rows:
        return

    pre_fallback_df = pd.DataFrame(rows)
    summary_lookup = reference_data.load_unit_price_summary()
    recency_factor = compute_recency_factor(pre_fallback_df)
    recency_meta = getattr(compute_recency_factor, "last_meta", {})
    region_factor = compute_region_factor(pre_fallback_df, project_region=project_region)
    region_meta = getattr(compute_region_factor, "last_meta", {})

    def _format_factor(label: str, factor: float, meta: Dict[str, object]) -> str:
        if not meta:
            meta = {}
        if meta.get("used_default"):
            return f"{label}=1.00 (insufficient data)"
        delta = (factor - 1.0) * 100.0
        sign = "+" if delta >= 0 else ""
        return f"{label}={sign}{delta:.1f}%"

    def _clamp_factor(value: float) -> tuple[float, bool]:
        clamped = max(0.75, min(1.25, value))
        changed = not math.isclose(clamped, value, rel_tol=1e-6, abs_tol=1e-6)
        return clamped, changed

    def _apply_memo_price(
        row_obj: Dict[str, object],
        guidance: design_memo_prices.MemoPriceGuidance,
        prior_note: str,
    ) -> None:
        precise_price = round(float(guidance.price), 2)
        row_obj["UNIT_PRICE_EST"] = precise_price
        row_obj["SOURCE"] = "DESIGN_MEMO_PRICE"
        row_obj["DATA_POINTS_USED"] = 0
        row_obj["STD_DEV"] = float("nan")
        row_obj["COEF_VAR"] = float("nan")

        memo_label = f"DESIGN_MEMO_PRICE DM {guidance.memo_id}"
        if guidance.effective_date:
            memo_label += f" (effective {guidance.effective_date})"
        price_label = f"recommended ${guidance.price:,.2f}"
        if guidance.unit:
            price_label += f" per {guidance.unit}"
        note_bits = [memo_label, price_label]
        if guidance.context:
            context = guidance.context.strip()
            if len(context) > 180:
                context = context[:177].rstrip() + "..."
            if context:
                note_bits.append(context)

        row_obj["NOTES"] = " | ".join(part for part in (prior_note, "; ".join(note_bits)) if part)

        payitem_details[row_obj.get("ITEM_CODE")] = pd.DataFrame(
            [
                {
                    "ITEM_CODE": row_obj.get("ITEM_CODE"),
                    "CATEGORY": "DESIGN_MEMO_PRICE",
                    "USED_FOR_PRICING": True,
                    "MEMO_ID": guidance.memo_id,
                    "EFFECTIVE_DATE": guidance.effective_date or "",
                    "EXTRACTED_AT": guidance.extracted_at or "",
                    "RECOMMENDED_PRICE": guidance.price,
                    "RECOMMENDED_UNIT": guidance.unit or "",
                    "CONTEXT": guidance.context or "",
                    "SOURCE_PATH": guidance.source_path.as_posix() if guidance.source_path else "",
                }
            ]
        )

    for row in rows:
        if row.get("ALTERNATE_USED"):
            continue

        code = str(row.get("ITEM_CODE") or "").strip()
        norm_code = normalize_item_code(code)

        existing_note = str(row.get("NOTES", "") or "").strip()
        if existing_note.upper().startswith("NO DATA"):
            existing_note = ""

        memo_guidance = design_memo_prices.lookup_memo_price(norm_code)
        if memo_guidance is not None:
            min_conf = float(os.getenv('MEMO_PRICE_MIN_CONFIDENCE', '0.7'))
            if (memo_guidance.confidence or 1.0) >= min_conf:
                _apply_memo_price(row, memo_guidance, existing_note)
                continue
            else:
                # Low confidence: keep as advisory note; continue with other fallbacks
                advisory = f"Memo guidance found (DM {memo_guidance.memo_id}) at low confidence={memo_guidance.confidence:.2f}; not applied."
                row["NOTES"] = " | ".join(part for part in (existing_note, advisory) if part)

        current_price = float(row.get("UNIT_PRICE_EST", 0) or 0.0)
        counts_zero = all(int(row.get(f"{label}_COUNT", 0) or 0) == 0 for label in CATEGORY_LABELS)
        if current_price > 0 and not pd.isna(current_price):
            continue
        if not counts_zero and str(row.get("SOURCE", "") or "").upper() != "NO_DATA":
            continue

        qty_val = float(row.get("QUANTITY", 0) or 0)

        # Precompute Unit Price Summary sufficiency (for notes and fallback), but do not apply yet
        summary_info = summary_lookup.get(norm_code)
        summary_reason = ""
        summary_eligible = False
        if summary_info:
            try:
                _sum_base_price = float(summary_info.get("weighted_average", 0) or 0)
            except Exception:
                _sum_base_price = 0.0
            _sum_contracts = int(float(summary_info.get("contracts", 0) or 0))
            if _sum_base_price > 0 and _sum_contracts >= 3:
                summary_eligible = True
            else:
                reasons = []
                if _sum_base_price <= 0:
                    reasons.append("weighted average unavailable")
                if _sum_contracts < 3:
                    reasons.append(f"contracts={_sum_contracts}")
                summary_reason = ", ".join(reasons)

        # Try Design Memo Rollup first
        mapping = design_memos.get_obsolete_mapping(norm_code)
        if mapping is None or not mapping.get("obsolete_codes"):
            # No design memo mapping; try summary if eligible, else remain NO_DATA with context.
            if summary_eligible and summary_info:
                try:
                    base_price = float(summary_info.get("weighted_average", 0) or 0)
                except Exception:
                    base_price = 0.0
                contracts = int(float(summary_info.get("contracts", 0) or 0))
                quantity_factor = 1.0
                quantity_note = ""
                if qty_val > 0:
                    try:
                        total_value = float(summary_info.get("total_value", 0) or 0)
                    except Exception:
                        total_value = 0.0
                    typical_per_contract = 0.0
                    if base_price > 0 and total_value > 0 and contracts > 0:
                        total_qty = total_value / base_price
                        typical_per_contract = total_qty / contracts if contracts else 0.0
                    if typical_per_contract > 0:
                        ratio = qty_val / typical_per_contract
                        if ratio >= 2.0:
                            quantity_factor = 0.95
                            quantity_note = "quantity adj=-5%"
                        elif ratio <= 0.5:
                            quantity_factor = 1.05
                            quantity_note = "quantity adj=+5%"
                combined_raw = recency_factor * region_factor * quantity_factor
                combined_factor, capped = _clamp_factor(combined_raw)
                adjusted_price = base_price * combined_factor
                clamp_applied = False
                try:
                    lowest = float(summary_info.get("lowest", 0) or 0)
                except Exception:
                    lowest = 0.0
                try:
                    highest = float(summary_info.get("highest", 0) or 0)
                except Exception:
                    highest = 0.0
                if lowest > 0 and adjusted_price < lowest:
                    adjusted_price = lowest
                    clamp_applied = True
                if highest > 0 and highest >= lowest and adjusted_price > highest:
                    adjusted_price = highest
                    clamp_applied = True
                row["UNIT_PRICE_EST"] = _round_unit_price(adjusted_price)
                row["SOURCE"] = "UNIT_PRICE_SUMMARY"
                row["DATA_POINTS_USED"] = contracts
                row["STD_DEV"] = float("nan")
                row["COEF_VAR"] = float("nan")
                notes_parts = [
                    f"UNIT_PRICE_SUMMARY CY{int(summary_info.get('year', 0) or 0)} (contracts={contracts})",
                    _format_factor("recency", recency_factor, recency_meta),
                    _format_factor("region", region_factor, region_meta),
                ]
                if quantity_note:
                    notes_parts.append(quantity_note)
                if capped:
                    notes_parts.append("combined adj capped at +/-25%")
                if clamp_applied:
                    notes_parts.append("clamped to summary range")
                fallback_note = "; ".join(part for part in notes_parts if part)
                row["NOTES"] = " | ".join(part for part in (existing_note, fallback_note) if part)
                continue
            if memo_guidance is not None:
                _apply_memo_price(row, memo_guidance, existing_note)
                continue
            else:
                # Nothing worked: record insufficiency and keep NO_DATA
                if summary_reason:
                    detail = f"Unit Price Summary insufficient ({summary_reason})."
                    row["NOTES"] = " | ".join(part for part in (existing_note, detail) if part)
                row["SOURCE"] = row.get("SOURCE") or "NO_DATA"
                row["STD_DEV"] = float("nan")
                row["COEF_VAR"] = float("nan")
                continue

        target_quantity = qty_val if qty_val > 0 else None
        base_price, obs_count, source_label = memo_rollup_price(
            bidtabs,
            norm_code,
            mapping["obsolete_codes"],
            project_region=project_region,
            target_quantity=target_quantity,
        )
        memo_pool = prepare_memo_rollup_pool(
            bidtabs,
            mapping["obsolete_codes"],
            project_region=project_region,
            target_quantity=target_quantity,
        )
        if obs_count == 0 or memo_pool.empty or not math.isfinite(base_price) or base_price <= 0:
            # DM insufficient: attempt Unit Price Summary if eligible; otherwise record insufficiency and NO_DATA
            if summary_eligible and summary_info:
                try:
                    sum_base = float(summary_info.get("weighted_average", 0) or 0)
                except Exception:
                    sum_base = 0.0
                contracts = int(float(summary_info.get("contracts", 0) or 0))
                quantity_factor = 1.0
                quantity_note = ""
                if qty_val > 0:
                    try:
                        total_value = float(summary_info.get("total_value", 0) or 0)
                    except Exception:
                        total_value = 0.0
                    typical_per_contract = 0.0
                    if sum_base > 0 and total_value > 0 and contracts > 0:
                        total_qty = total_value / sum_base
                        typical_per_contract = total_qty / contracts if contracts else 0.0
                    if typical_per_contract > 0:
                        ratio = qty_val / typical_per_contract
                        if ratio >= 2.0:
                            quantity_factor = 0.95
                            quantity_note = "quantity adj=-5%"
                        elif ratio <= 0.5:
                            quantity_factor = 1.05
                            quantity_note = "quantity adj=+5%"
                combined_raw = recency_factor * region_factor * quantity_factor
                combined_factor, capped = _clamp_factor(combined_raw)
                adjusted_price = sum_base * combined_factor
                clamp_applied = False
                try:
                    lowest = float(summary_info.get("lowest", 0) or 0)
                except Exception:
                    lowest = 0.0
                try:
                    highest = float(summary_info.get("highest", 0) or 0)
                except Exception:
                    highest = 0.0
                if lowest > 0 and adjusted_price < lowest:
                    adjusted_price = lowest
                    clamp_applied = True
                if highest > 0 and highest >= lowest and adjusted_price > highest:
                    adjusted_price = highest
                    clamp_applied = True
                row["UNIT_PRICE_EST"] = _round_unit_price(adjusted_price)
                row["SOURCE"] = "UNIT_PRICE_SUMMARY"
                row["DATA_POINTS_USED"] = contracts
                row["STD_DEV"] = float("nan")
                row["COEF_VAR"] = float("nan")
                memo_codes_fallback = "+".join(mapping["obsolete_codes"])
                notes_parts = [
                    f"Design memo {mapping['memo_id']} pooling insufficient ({memo_codes_fallback}); review manually.",
                    f"UNIT_PRICE_SUMMARY CY{int(summary_info.get('year', 0) or 0)} (contracts={contracts})",
                    _format_factor("recency", recency_factor, recency_meta),
                    _format_factor("region", region_factor, region_meta),
                ]
                if quantity_note:
                    notes_parts.append(quantity_note)
                if capped:
                    notes_parts.append("combined adj capped at +/-25%")
                if clamp_applied:
                    notes_parts.append("clamped to summary range")
                row["NOTES"] = " | ".join(part for part in (existing_note, "; ".join(notes_parts)) if part)
                continue
            if memo_guidance is not None:
                _apply_memo_price(row, memo_guidance, existing_note)
                continue
            else:
                memo_codes = "+".join(mapping["obsolete_codes"])
                detail = (
                    f"Design memo {mapping['memo_id']} pooling insufficient ({memo_codes}); review manually."
                )
                if summary_reason:
                    detail = f"Unit Price Summary insufficient ({summary_reason}); " + detail
                row["NOTES"] = " | ".join(part for part in (existing_note, detail) if part)
                row["SOURCE"] = row.get("SOURCE") or "NO_DATA"
                row["STD_DEV"] = float("nan")
                row["COEF_VAR"] = float("nan")
                continue

        quantity_factor = 1.0
        quantity_note = ""
        if target_quantity and "QUANTITY" in memo_pool.columns:
            pooled_qty = pd.to_numeric(memo_pool["QUANTITY"], errors="coerce").dropna()
            if not pooled_qty.empty:
                median_qty = float(pooled_qty.median())
                if median_qty > 0:
                    ratio = target_quantity / median_qty
                    if ratio >= 2.0:
                        quantity_factor = 0.95
                        quantity_note = "quantity adj=-5%"
                    elif ratio <= 0.5:
                        quantity_factor = 1.05
                        quantity_note = "quantity adj=+5%"

        combined_raw = recency_factor * region_factor * quantity_factor
        combined_factor, capped = _clamp_factor(combined_raw)
        adjusted_price = base_price * combined_factor
        bounded_note = ""
        pooled_prices = memo_pool["UNIT_PRICE"].astype(float)
        if not pooled_prices.empty:
            pool_min = float(pooled_prices.min())
            pool_max = float(pooled_prices.max())
            lower_bound = pool_min * 0.9 if pool_min > 0 else pool_min
            upper_bound = pool_max * 1.1 if pool_max > 0 else pool_max
            if lower_bound and adjusted_price < lower_bound:
                adjusted_price = lower_bound
                bounded_note = "bounded to pooled range"
            if upper_bound and adjusted_price > upper_bound:
                adjusted_price = upper_bound
                bounded_note = "bounded to pooled range"

        std_dev = float(pooled_prices.std(ddof=0)) if len(pooled_prices) > 1 else 0.0
        if base_price > 0:
            coef_var = abs(std_dev / base_price)
        else:
            coef_var = float("inf")

        row["UNIT_PRICE_EST"] = _round_unit_price(adjusted_price)
        row["SOURCE"] = "DESIGN_MEMO_ROLLUP"
        row["DATA_POINTS_USED"] = int(obs_count)
        row["STD_DEV"] = std_dev
        row["COEF_VAR"] = coef_var if math.isfinite(coef_var) else float("inf")

        memo_codes = "+".join(mapping["obsolete_codes"])
        notes_parts = [
            f"DESIGN_MEMO_ROLLUP DM {mapping['memo_id']} ({mapping['effective_date']}): {memo_codes} -> {norm_code}",
            f"pooled obs={obs_count}",
            source_label,
            _format_factor("recency", recency_factor, recency_meta),
            _format_factor("region", region_factor, region_meta),
        ]
        if quantity_note:
            notes_parts.append(quantity_note)
        if capped:
            notes_parts.append("combined adj capped at +/-25%")
        if bounded_note:
            notes_parts.append(bounded_note)
        if summary_reason:
            notes_parts.append(f"summary insufficient ({summary_reason})")
        fallback_note = "; ".join(part for part in notes_parts if part)
        row["NOTES"] = " | ".join(part for part in (existing_note, fallback_note) if part)

        memo_detail = memo_pool.copy()
        memo_detail["CATEGORY"] = "DESIGN_MEMO_ROLLUP"
        memo_detail["USED_FOR_PRICING"] = True
        payitem_details[code] = memo_detail


def _first_numeric(series: pd.Series) -> Optional[float]:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.iloc[0])


def _extract_expected_contract_cost(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty:
        return None
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    for key in ("EXPECTED_TOTAL_CONTRACT_COST", "EXPECTED_CONTRACT_COST", "EXPECTED_COST"):
        if key in df.columns:
            value = _first_numeric(df[key])
            if value is not None:
                return value
    for col in df.columns:
        value = _first_numeric(df[col])
        if value is not None:
            return value
    return None


def _extract_project_region(df: pd.DataFrame) -> Optional[int]:
    if df is None or df.empty:
        return None
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    for key in ("PROJECT_REGION", "REGION"):
        if key in df.columns:
            value = _first_numeric(df[key])
            if value is not None:
                return int(value)
    return None


def _parse_expected_cost_value(raw: str) -> Optional[float]:
    if not raw:
        return None
    cleaned = raw.replace("$", "").replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _project_inputs_from_config(cfg: Config) -> tuple[
    Optional[float],
    Optional[int],
    Optional[pd.DataFrame],
    Optional[str],
    bool,
]:
    expected_cost = cfg.expected_contract_cost
    district_name = normalize_district(cfg.project_district or "")
    project_region = cfg.project_region

    env_used = any(value is not None and value != "" for value in (expected_cost, district_name, project_region))
    if not env_used:
        return None, None, None, None, False

    if district_name and project_region is None:
        project_region = DISTRICT_REGION_MAP.get(district_name)

    rows = [{"DISTRICT": name, "REGION": number} for number, name in DISTRICT_CHOICES]
    region_map_df = pd.DataFrame(rows)

    return expected_cost, project_region, region_map_df, district_name or None, True


def _sanitize_bidtabs(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleansing: drop non-positive prices and duplicate bid rows."""
    if df is None or df.empty:
        return df

    cleaned = df.copy()
    if "UNIT_PRICE" in cleaned.columns:
        cleaned["UNIT_PRICE"] = pd.to_numeric(cleaned["UNIT_PRICE"], errors="coerce")
        cleaned = cleaned.loc[cleaned["UNIT_PRICE"] > 0].copy()

    if "QUANTITY" in cleaned.columns:
        cleaned["QUANTITY"] = pd.to_numeric(cleaned["QUANTITY"], errors="coerce")

    subset = [
        col
        for col in ["ITEM_CODE", "LETTING_DATE", "UNIT_PRICE", "QUANTITY", "BIDDER"]
        if col in cleaned.columns
    ]
    if subset:
        cleaned = cleaned.drop_duplicates(subset=subset, keep="first")

    return cleaned


def load_project_attributes(
    path: Path,
    legacy_expected_path: Optional[str] = None,
    legacy_region_map_path: Optional[str] = None,
) -> tuple[Optional[float], Optional[int], pd.DataFrame]:
    if not path.exists():
        expected_cost = None
        if legacy_expected_path and Path(legacy_expected_path).exists():
            try:
                legacy_df = pd.read_excel(legacy_expected_path, engine="openpyxl")
                expected_cost = _extract_expected_contract_cost(legacy_df)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "Warning: unable to migrate expected contract cost from %s: %s",
                    legacy_expected_path,
                    exc,
                )
        region_map = pd.DataFrame(columns=["DISTRICT", "REGION"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            pd.DataFrame({
                "EXPECTED_TOTAL_CONTRACT_COST": [expected_cost],
                "PROJECT_REGION": [pd.NA],
            }).to_excel(writer, sheet_name="PROJECT", index=False)
            region_map.to_excel(writer, sheet_name="REGION_MAP", index=False)
        logger.info("Created project attributes workbook at %s. Populate it and rerun.", path)
        return expected_cost, None, region_map

    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Warning: unable to read project attributes file %s: %s", path, exc)
        return None, None, pd.DataFrame(columns=["DISTRICT", "REGION"])

    sheet_lookup = {name.strip().upper(): name for name in xls.sheet_names}
    project_sheet = sheet_lookup.get("PROJECT") or sheet_lookup.get("PROJECT_ATTRIBUTES") or xls.sheet_names[0]
    project_df = xls.parse(project_sheet)
    expected_cost = _extract_expected_contract_cost(project_df)
    project_region = _extract_project_region(project_df)

    region_sheet = sheet_lookup.get("REGION_MAP") or sheet_lookup.get("REGIONS")
    if region_sheet:
        region_map_df = xls.parse(region_sheet)
    elif legacy_region_map_path and Path(legacy_region_map_path).exists():
        try:
            region_map_df = load_region_map(legacy_region_map_path)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Warning: unable to read region map from %s: %s", legacy_region_map_path, exc)
            region_map_df = pd.DataFrame(columns=["DISTRICT", "REGION"])
    else:
        region_map_df = pd.DataFrame(columns=["DISTRICT", "REGION"])

    try:
        region_map_df = load_region_map(region_map_df)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Warning: region map data in %s is invalid: %s", path, exc)
        region_map_df = pd.DataFrame(columns=["DISTRICT", "REGION"])

    return expected_cost, project_region, region_map_df


def run(config: Optional["CLIConfig"] = None, runtime_config: Optional[Config] = None) -> int:
    runtime_cfg = runtime_config or DEFAULT_CONFIG

    # Apply repository policy defaults (non-invasive; env can override)
    try:
        apply_policy_defaults(runtime_cfg.base_dir / "references" / "policy" / "policy.json")
    except Exception:
        logger.debug("Policy defaults not applied", exc_info=True)

    if config is not None:
        try:
            from .config import CLIConfig as _CLI
            assert isinstance(config, _CLI)
        except Exception:
            pass
        runtime_cfg = replace(
            runtime_cfg,
            output_dir=config.estimate_audit_csv.parent.resolve(),
            output_audit=config.estimate_audit_csv,
            output_xlsx=config.estimate_xlsx,
            output_payitem_audit=config.payitems_workbook,
            disable_ai=config.disable_ai,
        )

    stage_counter = 0

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    def log_stage(message: str) -> None:
        nonlocal stage_counter
        stage_counter += 1
        logger.info("[pipeline:%02d] %s", stage_counter, message)

    def log_detail(message: str) -> None:
        logger.info("           %s", message)

    bidtabs_dir = runtime_cfg.bidtabs_dir
    output_dir = runtime_cfg.output_dir
    out_xlsx = runtime_cfg.output_xlsx
    out_audit = runtime_cfg.output_audit
    out_pay_audit = runtime_cfg.output_payitem_audit
    quantities_glob = runtime_cfg.quantities_glob
    quantities_override = runtime_cfg.quantities_path
    project_attrs_path = runtime_cfg.project_attributes
    aliases_path = runtime_cfg.aliases_csv
    min_sample_target = runtime_cfg.min_sample_target
    legacy_expected_path = runtime_cfg.legacy_expected_cost_path
    legacy_region_map_path = runtime_cfg.region_map_path

    dm2321_enabled = runtime_cfg.apply_dm23_21
    dm2321_crosswalk: dict[str, CrosswalkRow] = {}
    dm2321_reverse_meta: dict[str, dict[str, object]] = {}
    dm2321_desc_by_new: dict[str, str] = {}

    if dm2321_enabled:
        log_stage("Loading DM 23-21 HMA crosswalk data")
        dm2321_path = runtime_cfg.base_dir / "data_reference" / "hma_crosswalk_dm23_21.csv"
        dm2321_crosswalk = load_crosswalk(dm2321_path)
        for row in dm2321_crosswalk.values():
            if getattr(row, "new_pay_item", None):
                dm2321_reverse_meta.setdefault(
                    row.new_pay_item,
                    {
                        "course": row.course,
                        "esal_cat": row.esal_cat,
                        "binder_class": row.binder_class,
                        "new_desc": row.new_desc,
                    },
                )
                if row.new_desc:
                    dm2321_desc_by_new[row.new_pay_item] = row.new_desc
        log_detail(f"dm2321_crosswalk_rows={len(dm2321_crosswalk):,}")

    contract_filter_pct = runtime_cfg.contract_filter_pct if runtime_cfg.contract_filter_pct is not None else 50.0

    log_stage("Bootstrapping estimator runtime context")
    log_detail(f"output_dir={output_dir}")
    log_detail(f"artifact_targets={out_xlsx.name},{out_audit.name},{out_pay_audit.name}")
    if config is not None:
        log_detail("config_override=CLIConfig(test harness)")
    log_detail(f"python_version={sys.version.split()[0]} | cwd={Path.cwd()}")

    _load_api_key_from_file()
    (
        env_expected_cost,
        env_project_region,
        env_region_map,
        env_district_name,
        env_used,
    ) = _project_inputs_from_config(runtime_cfg)

    project_district_name = env_district_name
    project_attrs_display = str(project_attrs_path)

    log_stage("Inspecting execution environment for GUI overrides")
    if env_used:
        env_cost_display = f"${env_expected_cost:,.2f}" if env_expected_cost is not None else "(unset)"
        region_display = env_project_region if env_project_region is not None else "(unspecified)"
        district_display = env_district_name or "(unspecified)"
        log_detail(
            "gui_runtime_inputs => expected_cost=%s | project_region=%s | district=%s"
            % (env_cost_display, region_display, district_display)
        )
        if env_region_map is not None and not getattr(env_region_map, "empty", False):
            log_detail(f"region_map_override_rows={len(env_region_map)}")
    if env_used:
        expected_contract_cost = env_expected_cost
        project_region = env_project_region
        region_map = env_region_map if env_region_map is not None else pd.DataFrame(columns=["DISTRICT", "REGION"])
        project_attrs_display = "(GUI inputs)"
    else:
        log_detail("no GUI overrides detected; hydrating project attribute workbook inputs")
        expected_contract_cost, project_region, region_map = load_project_attributes(
            project_attrs_path,
            legacy_expected_path=legacy_expected_path,
            legacy_region_map_path=legacy_region_map_path,
        )
        project_district_name = None
        map_rows = len(region_map) if region_map is not None and not getattr(region_map, "empty", False) else 0
        expected_display = f"${expected_contract_cost:,.2f}" if expected_contract_cost else "(unset)"
        region_display = project_region if project_region is not None else "(unspecified)"
        log_detail(
            "project_attributes => expected_cost=%s | project_region=%s | region_map_rows=%s"
            % (expected_display, region_display, map_rows)
        )
    log_stage("Priming reference data caches")
    payitem_catalog_size = len(reference_data.load_payitem_catalog())
    unit_price_summary_size = len(reference_data.load_unit_price_summary())
    spec_section_size = len(reference_data.load_spec_sections())
    log_detail(
        "reference_cache_sizes => payitems=%s | unit_price_summary=%s | spec_sections=%s"
        % (
            f"{payitem_catalog_size:,}",
            f"{unit_price_summary_size:,}",
            f"{spec_section_size:,}",
        )
    )

    log_stage(f"Ingesting BidTabs corpus from {bidtabs_dir}")
    bid = load_bidtabs_files(bidtabs_dir)
    log_detail(f"raw_bidtabs_rows={len(bid):,} | columns={len(bid.columns)}")

    if dm2321_enabled and "ITEM_CODE" in bid.columns:
        deleted_rows = 0
        keep_indices: list[int] = []
        mapped_codes: list[str] = []
        mapped_rules: list[str | None] = []
        mapped_sources: list[str | None] = []
        mapped_courses: list[str | None] = []
        mapped_esals: list[str | None] = []
        mapped_binders: list[str | None] = []

        codes = bid["ITEM_CODE"].astype(str)
        for idx, item_code in enumerate(codes):
            new_code, meta = remap_item(item_code, dm2321_crosswalk)
            if meta.get("deleted") and new_code is None:
                deleted_rows += 1
                continue
            mapped = new_code or item_code
            reverse_meta = dm2321_reverse_meta.get(mapped, {})
            keep_indices.append(idx)
            mapped_codes.append(mapped)
            mapped_rules.append(meta.get("mapping_rule") or ("DM 23-21" if reverse_meta else None))
            mapped_sources.append(meta.get("source_item") if meta.get("mapping_rule") else None)
            mapped_courses.append(meta.get("course") or reverse_meta.get("course"))
            mapped_esals.append(meta.get("esal_cat") or reverse_meta.get("esal_cat"))
            mapped_binders.append(meta.get("binder_class") or reverse_meta.get("binder_class"))

        if keep_indices:
            bid = bid.iloc[keep_indices].copy()
            bid.reset_index(drop=True, inplace=True)
            bid["ITEM_CODE"] = mapped_codes
            bid["DM2321_MAPPING_RULE"] = mapped_rules
            bid["DM2321_SOURCE_ITEM"] = mapped_sources
            bid["DM2321_COURSE"] = mapped_courses
            bid["DM2321_ESAL_CAT"] = mapped_esals
            bid["DM2321_BINDER_CLASS"] = mapped_binders
        else:
            bid = bid.iloc[0:0].copy()
        if deleted_rows:
            log_detail(f"dm2321_deleted_bidtab_rows={deleted_rows:,}")

    bid = ensure_region_column(bid, region_map)
    log_detail("region dimensions normalized onto BidTabs frame")

    log_stage("Augmenting BidTabs dataset with geometry and numeric coercions")
    geom_info = bid["DESCRIPTION"].apply(parse_geometry)
    bid["GEOM_SHAPE"] = geom_info.map(lambda g: getattr(g, "shape", None))
    bid["GEOM_AREA_SQFT"] = geom_info.map(lambda g: getattr(g, "area_sqft", float("nan")))
    bid["GEOM_DIMENSIONS"] = geom_info.map(lambda g: getattr(g, "dimensions", None))

    if "LETTING_DATE" in bid.columns:
        bid["LETTING_DATE"] = pd.to_datetime(bid["LETTING_DATE"], errors="coerce")
    if "UNIT_PRICE" in bid.columns:
        bid["UNIT_PRICE"] = pd.to_numeric(bid["UNIT_PRICE"], errors="coerce")
    if "WEIGHT" in bid.columns:
        bid["WEIGHT"] = pd.to_numeric(bid["WEIGHT"], errors="coerce")
    if "JOB_SIZE" in bid.columns:
        bid["JOB_SIZE"] = pd.to_numeric(bid["JOB_SIZE"], errors="coerce")

    bid = _sanitize_bidtabs(bid)
    log_detail(f"post-sanitize BidTabs footprint => rows={len(bid):,}")

    log_stage("Resolving project quantities workbook")
    if quantities_override:
        qty_path = Path(quantities_override).expanduser().resolve()
        log_detail(f"config_override_quantities={qty_path}")
    else:
        qty_path = find_quantities_file(quantities_glob, base_dir=BASE_DIR)
        log_detail(f"auto_discovered_quantities={qty_path}")
    qty = load_quantities(qty_path)
    qty_rows = len(qty)
    unique_items = qty["ITEM_CODE"].nunique(dropna=True) if "ITEM_CODE" in qty.columns else qty_rows
    log_detail(f"project_quantities_rows={qty_rows:,} | distinct_item_codes={unique_items:,}")

    if Path(aliases_path).exists():
        alias = pd.read_csv(aliases_path, dtype=str)
        if not alias.empty:
            alias["PROJECT_CODE"] = alias["PROJECT_CODE"].astype(str).str.strip()
            alias["HIST_CODE"] = alias["HIST_CODE"].astype(str).str.strip()
            amap = dict(zip(alias["PROJECT_CODE"], alias["HIST_CODE"]))
            qty["ITEM_CODE"] = qty["ITEM_CODE"].map(lambda c: amap.get(c, c))
            log_detail(f"code_alias_mapping_applied => entries={len(alias):,}")

    contract_filter_pct = max(0.0, min(contract_filter_pct, 500.0))

    filtered_bounds = None
    log_stage("Calibrating BidTabs contract-cost filter window")
    if expected_contract_cost and expected_contract_cost > 0 and "JOB_SIZE" in bid.columns:
        tolerance = contract_filter_pct / 100.0
        lower_bound = expected_contract_cost * (1.0 - tolerance)
        upper_bound = expected_contract_cost * (1.0 + tolerance)
        before_rows = len(bid)
        mask = bid["JOB_SIZE"].between(lower_bound, upper_bound, inclusive="both")
        bid = bid.loc[mask].copy()
        after_rows = len(bid)
        filtered_bounds = (lower_bound, upper_bound)
        pct_display = (
            f"{int(contract_filter_pct)}"
            if contract_filter_pct.is_integer()
            else f"{contract_filter_pct:.2f}".rstrip("0").rstrip(".")
        )
        logger.info(
            "Filtered BidTabs to contracts between $%s and $%s (+/-%s%% of expected $%s); kept %s of %s rows.",
            f"{lower_bound:,.0f}",
            f"{upper_bound:,.0f}",
            pct_display,
            f"{expected_contract_cost:,.0f}",
            after_rows,
            before_rows,
        )
        if bid.empty:
            logger.warning("No BidTabs rows remained after contract cost filtering.")
        else:
            log_detail(f"contract_filter => retained_rows={after_rows:,} of {before_rows:,}")
    else:
        log_detail("contract_filter bypassed (expected_contract_cost missing or JOB_SIZE unavailable)")

    alt_seek_enabled = not runtime_cfg.disable_alt_seek
    if not alt_seek_enabled:
        log_detail("alternate_seek disabled via runtime configuration")

    ai_enabled = not runtime_cfg.disable_ai

    rows = []
    dm2321_deleted_items: list[str] = []
    payitem_details: Dict[str, pd.DataFrame] = {}
    alternate_reports: Dict[str, Dict[str, object]] = {}

    log_stage(f"Running item pricing analytics for {qty_rows:,} project rows")
    for _, r in qty.iterrows():
        code = str(r["ITEM_CODE"]).strip()
        desc = str(r.get("DESCRIPTION", "")).strip()
        unit = str(r.get("UNIT", "")).strip()
        qty_val = float(r.get("QUANTITY", 0) or 0)
        original_code = code

        mapped_from_old: str | None = None
        dm_mapping_rule: str | None = None
        dm_course: str | None = None
        dm_esal: str | None = None
        dm_binder: str | None = None
        dm_new_desc: str | None = None
        dm_adder_applied = False

        if dm2321_enabled:
            new_code, meta = remap_item(code, dm2321_crosswalk)
            if meta.get("deleted") and new_code is None:
                logger.info("[item] %s :: skipped (DM 23-21 deleted)", code or "(blank)")
                dm2321_deleted_items.append(code)
                continue
            mapped_code = new_code or code
            reverse_meta = dm2321_reverse_meta.get(mapped_code, {})
            mapped_from_old = meta.get("source_item") if meta.get("mapping_rule") else None
            dm_course = meta.get("course") or reverse_meta.get("course")
            dm_esal = meta.get("esal_cat") or reverse_meta.get("esal_cat")
            dm_binder = meta.get("binder_class") or reverse_meta.get("binder_class")
            dm_mapping_rule = meta.get("mapping_rule") or ("DM 23-21" if reverse_meta else None)
            dm_new_desc = meta.get("new_desc") or reverse_meta.get("new_desc") or dm2321_desc_by_new.get(mapped_code)
            code = mapped_code
            if dm_new_desc:
                desc = dm_new_desc

        code_display = code or "(blank)"
        desc_compact = " ".join(desc.split())
        if len(desc_compact) > 72:
            desc_compact = desc_compact[:69].rstrip() + "..."
        logger.info("[item] %s :: qty=%s %s :: %s", code_display, f"{qty_val:,.3f}", unit, desc_compact)
        if dm_mapping_rule and mapped_from_old:
            logger.info("        dm2321_mapping => %s -> %s", mapped_from_old, code_display)

        target_quantity = qty_val if qty_val > 0 else None
        if dm2321_enabled and dm_mapping_rule == "DM 23-21":
            target_quantity = None
        price, source_label, cat_data, detail_map, used_categories, combined_used = category_breakdown(
            bid,
            code,
            project_region=project_region,
            include_details=True,
            target_quantity=target_quantity,
        )

        note = ""
        if pd.isna(price):
            price = 0.0
            note = "NO DATA IN ANY CATEGORY; REVIEW."

        used_categories = used_categories or []
        used_category_set = set(used_categories)
        data_points_used = int(cat_data.get("TOTAL_USED_COUNT", len(combined_used)))
        category_display = ", ".join(used_categories) if used_categories else "none"
        logger.info("        data_points_used=%s | categories_used=%s", data_points_used, category_display)

        sampling_warning = False
        if not note and 0 < data_points_used < min_sample_target:
            note = f"Only {data_points_used} data points found (target {min_sample_target})."
            logger.info("        sampling_warning => %s", note)
            sampling_warning = True

        geometry = parse_geometry(desc)
        reference_bundle = reference_data.build_reference_bundle(code)
        if dm2321_enabled and dm_mapping_rule == "DM 23-21" and not pd.isna(price):
            history_sufficient = data_points_used >= min_sample_target
            adjusted_price, adder_flag = maybe_apply_dm2321_adder(
                dm_course,
                float(price),
                enabled=True,
                sufficient_history=history_sufficient,
            )
            if adder_flag:
                logger.info(
                    "        dm2321_adder_applied => course=%s | +$%.2f/ton",
                    dm_course or "(unknown)",
                    adjusted_price - float(price),
                )
            price = adjusted_price
            dm_adder_applied = adder_flag

        unit_price_est = _round_unit_price(price)
        source_label = source_label or ("NO_DATA" if data_points_used == 0 else "")
        source_display = source_label or "historical_category_mix"
        logger.info("        provisional_unit_price=$%s | source=%s", f"{unit_price_est:,.2f}", source_display)
        if note and not sampling_warning:
            logger.info("        note => %s", note)

        row: Dict[str, object] = {
            "ITEM_CODE": code,
            "DESCRIPTION": desc,
            "UNIT": unit,
            "QUANTITY": qty_val,
            "UNIT_PRICE_EST": unit_price_est,
            "NOTES": note,
            "DATA_POINTS_USED": data_points_used,
            "ALTERNATE_USED": False,
            "SOURCE": source_label,
        }

        # Consistency: UNIT normalization and mismatch flag vs. catalog
        try:
            catalog = reference_data.load_payitem_catalog()
            expected_unit = str((catalog.get(code, {}) or {}).get("unit", "")).strip().upper()
            given_unit = str(unit or "").strip().upper()
            if expected_unit:
                row["UNIT_NORMALIZED"] = expected_unit
                row["UNIT_MISMATCH_FLAG"] = (expected_unit != given_unit and given_unit != "")
        except Exception:
            pass

        if dm_mapping_rule:
            if mapped_from_old:
                row["MappedFromOldItem"] = mapped_from_old
            elif original_code != code:
                row["MappedFromOldItem"] = original_code
            else:
                row["MappedFromOldItem"] = None
            row["DM2321_MAPPING_RULE"] = dm_mapping_rule
            row["DM2321_COURSE"] = dm_course
            row["DM2321_ESAL_CAT"] = dm_esal
            row["DM2321_BINDER_CLASS"] = dm_binder
            row["DM2321_ADDER_APPLIED"] = dm_adder_applied
        else:
            row.setdefault("MappedFromOldItem", None)
            row.setdefault("DM2321_MAPPING_RULE", None)
            row.setdefault("DM2321_COURSE", None)
            row.setdefault("DM2321_ESAL_CAT", None)
            row.setdefault("DM2321_BINDER_CLASS", None)
            row.setdefault("DM2321_ADDER_APPLIED", False)

        if geometry is not None:
            row["GEOM_SHAPE"] = geometry.shape
            row["GEOM_AREA_SQFT"] = round(geometry.area_sqft, 4)
            if geometry.dimensions:
                row["GEOM_DIMENSIONS"] = geometry.dimensions

        if alt_seek_enabled and data_points_used == 0 and geometry is not None:
            area_display = getattr(geometry, "area_sqft", float("nan"))
            logger.info("        alternate_seek activating => geometry_area=%s sqft", f"{area_display:.2f}")
            alt_result = find_alternate_price(
                bid,
                code,
                geometry,
                project_region=project_region,
                target_description=desc,
                reference_bundle=reference_bundle,
                allow_ai=ai_enabled,
            )
            if alt_result is not None:
                price = alt_result.final_price
                unit_price_est = _round_unit_price(price)
                data_points_used = alt_result.total_data_points
                row["UNIT_PRICE_EST"] = unit_price_est
                row["DATA_POINTS_USED"] = data_points_used
                row["ALTERNATE_USED"] = True
                source_items = []
                for sel in alt_result.selections:
                    source_label = sel.source or "unknown"
                    source_items.append(f"{sel.item_code} (w={sel.weight:.2f}, src={source_label})")
                row["ALTERNATE_SOURCE_ITEM"] = "; ".join(source_items)
                row["ALTERNATE_RATIO"] = "; ".join(f"{sel.ratio:.3f}" for sel in alt_result.selections)
                row["ALTERNATE_BASE_PRICE"] = "; ".join(f"${sel.base_price:.2f}" for sel in alt_result.selections)
                row["ALTERNATE_SOURCE_AREA"] = "; ".join(f"{sel.area_sqft:.2f}" for sel in alt_result.selections)
                row["ALTERNATE_CANDIDATE_COUNT"] = sum(sel.data_points for sel in alt_result.selections)
                row["ALT_TOTAL_CANDIDATES"] = len(alt_result.candidate_payload)
                row["ALT_SELECTED_COUNT"] = len(alt_result.selections)
                similarity_summary = alt_result.similarity_summary or {}
                for key, value in similarity_summary.items():
                    score_col = (
                        "ALT_SCORE_OVERALL"
                        if key == "overall_score"
                        else f"ALT_SCORE_{key.replace('_score', '').upper()}"
                    )
                    row[score_col] = round(float(value), 4)
                method_label = "AI weighted alternates"
                if alt_result.ai_notes and "failed" in str(alt_result.ai_notes).lower():
                    method_label = "Score-based fallback"
                elif all(
                    sel.reason and sel.reason.lower().startswith("fallback")
                    for sel in alt_result.selections
                ):
                    method_label = "Score-based fallback"
                row["ALTERNATE_METHOD"] = method_label
                if alt_result.ai_notes:
                    row["ALTERNATE_AI_NOTES"] = alt_result.ai_notes
                row["NOTES"] = (
                    "AI weighted pricing"
                    if method_label == "AI weighted alternates"
                    else "Score-based alternate pricing"
                )
                note = row["NOTES"]
                logger.info(
                    "        alternate_seek resolved => selections=%s | datapoints=%s | method=%s",
                    len(alt_result.selections),
                    data_points_used,
                    method_label,
                )
                similarity_flags = []
                for sel in alt_result.selections:
                    for note in sel.notes:
                        similarity_flags.append(f"{sel.item_code}: {note}")
                for code_key, notes_list in (alt_result.candidate_notes or {}).items():
                    for note in notes_list:
                        entry = f"{code_key}: {note}"
                        if entry not in similarity_flags:
                            similarity_flags.append(entry)
                if similarity_flags:
                    joined_flags = " | ".join(similarity_flags)
                    row["ALT_SIMILARITY_NOTES"] = joined_flags[:1000]
                selection_payload = []
                for sel in alt_result.selections:
                    payload = {
                        "item_code": sel.item_code,
                        "description": sel.description,
                        "area_sqft": sel.area_sqft,
                        "base_price": sel.base_price,
                        "adjusted_price": sel.adjusted_price,
                        "ratio": sel.ratio,
                        "data_points": sel.data_points,
                        "weight": sel.weight,
                        "reason": sel.reason,
                        "source": sel.source,
                        "similarity_scores": dict(sel.similarity or {}),
                        "notes": list(sel.notes),
                    }
                    selection_payload.append(payload)
                alt_entry = {
                    "target_area_sqft": geometry.area_sqft,
                    "candidates": alt_result.candidate_payload,
                    "selected": selection_payload,
                    "similarity_summary": similarity_summary,
                    "candidate_notes": alt_result.candidate_notes,
                    "chosen": {
                        "final_unit_price": float(alt_result.final_price),
                        "rounded_unit_price": unit_price_est,
                        "total_data_points": int(data_points_used),
                        "selections": [dict(entry) for entry in selection_payload],
                        "similarity_summary": similarity_summary,
                    },
                    "final_price_raw": float(alt_result.final_price),
                    "final_price_rounded": unit_price_est,
                    "ai_notes": alt_result.ai_notes,
                    "method": method_label,
                }
                ref_snapshot = None
                if alt_result.reference_bundle:
                    alt_entry["references"] = alt_result.reference_bundle
                    ref_snapshot = dict(alt_result.reference_bundle)
                    spec_text = ref_snapshot.get('spec_text')
                    if isinstance(spec_text, str) and len(spec_text) > 4000:
                        ref_snapshot['spec_text'] = spec_text[:4000] + ' \u2026'
                if alt_result.ai_system:
                    alt_entry["ai_system"] = alt_result.ai_system
                if alt_result.show_work_method:
                    alt_entry["show_work_method"] = alt_result.show_work_method
                    row["ALT_SHOW_WORK_METHOD"] = alt_result.show_work_method
                if alt_result.process_improvements:
                    alt_entry["process_improvements"] = alt_result.process_improvements
                alternate_reports[code] = alt_entry
                if alt_result.ai_notes:
                    alt_entry["chosen"]["notes"] = alt_result.ai_notes
                cat_data = alt_result.cat_data
                detail_map = alt_result.detail_map or {}
                used_categories = alt_result.used_categories or []
                combined_used = alt_result.combined_detail
                row["SOURCE"] = "GEOMETRY_ALTERNATE"
            else:
                logger.info("        alternate_seek exhausted without a viable candidate; retaining NO_DATA baseline")

        for label in CATEGORY_LABELS:
            row[f"{label}_PRICE"] = cat_data.get(f"{label}_PRICE", float("nan"))
            row[f"{label}_COUNT"] = cat_data.get(f"{label}_COUNT", 0)
            row[f"{label}_INCLUDED"] = label in used_category_set

        rows.append(row)

        detail_frames = []
        detail_map = detail_map or {}
        seen_ids = set()

        for category_name in CATEGORY_LABELS:
            if category_name not in used_category_set:
                continue
            subset = detail_map.get(category_name)
            if subset is None or subset.empty:
                continue
            detail = subset.copy()
            if "_AUDIT_ROW_ID" in detail.columns:
                detail = detail.loc[~detail["_AUDIT_ROW_ID"].isin(seen_ids)].copy()
                seen_ids.update(detail["_AUDIT_ROW_ID"].tolist())
                detail.drop(columns=["_AUDIT_ROW_ID"], errors="ignore", inplace=True)
            detail["CATEGORY"] = category_name
            detail["USED_FOR_PRICING"] = True
            detail_frames.append(detail)

        if detail_frames:
            payitem_details[code] = pd.concat(detail_frames, ignore_index=True)

    log_stage("Executing non-geometry fallback pricing routines")
    apply_non_geometry_fallbacks(rows, bid, project_region, payitem_details)
    log_detail("non-geometry fallback pass complete")

    def _compute_contract_subtotal(exclude_codes: set[str]) -> float:
        total = 0.0
        for entry in rows:
            code = entry.get("ITEM_CODE")
            if code in exclude_codes:
                continue
            qty_val = float(entry.get("QUANTITY", 0) or 0)
            price_val = float(entry.get("UNIT_PRICE_EST", 0) or 0)
            total += qty_val * price_val
        return total

    def _apply_contract_percent(code: str, percent: float, exclude_codes: set[str], note_label: str) -> None:
        row_obj = next((entry for entry in rows if entry.get("ITEM_CODE") == code), None)
        if row_obj is None:
            log_detail(f"contract_percent skipped => code={code} not present in rows")
            return
        qty_val = float(row_obj.get("QUANTITY", 0) or 0)
        if qty_val <= 0:
            log_detail(f"contract_percent skipped => code={code} has non-positive quantity")
            return
        subtotal = _compute_contract_subtotal(exclude_codes)
        target_amount = subtotal * percent
        rounded_amount = math.floor(target_amount / 1000.0) * 1000.0
        unit_price = round(rounded_amount / qty_val, 2) if qty_val else 0.0
        row_obj["UNIT_PRICE_EST"] = unit_price
        row_obj["DATA_POINTS_USED"] = 0
        row_obj["ALTERNATE_USED"] = False
        for key in (
            "ALTERNATE_SOURCE_ITEM",
            "ALTERNATE_RATIO",
            "ALTERNATE_BASE_PRICE",
            "ALTERNATE_SOURCE_AREA",
            "ALTERNATE_CANDIDATE_COUNT",
            "ALTERNATE_METHOD",
            "ALTERNATE_AI_NOTES",
        ):
            row_obj.pop(key, None)
        note_text = f"{note_label} {percent * 100:.1f}% of applicable items = ${rounded_amount:,.0f}."
        existing_note = str(row_obj.get("NOTES", "") or "").strip()
        row_obj["NOTES"] = f"{existing_note} {note_text}".strip() if existing_note else note_text
        for label in CATEGORY_LABELS:
            row_obj[f"{label}_PRICE"] = float("nan")
            row_obj[f"{label}_COUNT"] = 0
            row_obj[f"{label}_INCLUDED"] = False
        alternate_reports.pop(code, None)
        detail_columns = [
            "ITEM_CODE",
            "DESCRIPTION",
            "CATEGORY",
            "USED_FOR_PRICING",
            "LETTING_DATE",
            "CONTRACTOR",
            "UNIT_PRICE",
            "QUANTITY",
            "DISTRICT",
            "REGION",
            "COUNTY",
            "PROJECT_ID",
            "CONTRACT_ID",
            "WEIGHT",
            "JOB_SIZE",
        ]
        payitem_details[code] = pd.DataFrame(
            {
                "ITEM_CODE": [code],
                "DESCRIPTION": [row_obj.get("DESCRIPTION")],
                "CATEGORY": ["CONTRACT_PERCENT"],
                "USED_FOR_PRICING": [True],
                "LETTING_DATE": [pd.NaT],
                "CONTRACTOR": [pd.NA],
                "UNIT_PRICE": [unit_price],
                "QUANTITY": [qty_val],
                "DISTRICT": [pd.NA],
                "REGION": [pd.NA],
                "COUNTY": [pd.NA],
                "PROJECT_ID": [pd.NA],
                "CONTRACT_ID": [pd.NA],
                "WEIGHT": [pd.NA],
                "JOB_SIZE": [pd.NA],
            },
            columns=detail_columns,
        )
        log_detail(
            (
                "contract_percent applied => code=%s | percent=%.1f%% | "
                "target_amount=$%s | unit_price=$%s"
            )
            % (
                code,
                percent * 100,
                f"{rounded_amount:,.0f}",
                f"{unit_price:,.2f}",
            )
        )

    # Load contract-percent rules from external config for consistency with IDM Chapter 20/Spec edition
    def _load_contract_rules() -> tuple[list[dict], str | None]:
        try:
            cfg_path = runtime_cfg.base_dir / "references" / "specs" / "contract_percents.json"
            if cfg_path.exists():
                payload = json.loads(cfg_path.read_text(encoding="utf-8"))
                edition = payload.get("spec_edition")
                rules = payload.get("rules") or []
                return [dict(r) for r in rules], str(edition) if edition else None
        except Exception:
            logger.debug("Unable to read contract percent rules", exc_info=True)
        return [], None

    rules, spec_edition = _load_contract_rules()
    if rules:
        log_stage(f"Applying contract percent adjustments per IDM Chapter 20 guidance ({spec_edition or 'unspecified'})")
        exclude = {str(r.get("code")) for r in rules}
        for rule in rules:
            try:
                code = str(rule.get("code"))
                pct = float(rule.get("percent"))
                note_label = str(rule.get("note") or "Per IDM Chapter 20:")
                if spec_edition:
                    note_label = f"{note_label} ({spec_edition})"
                _apply_contract_percent(code, pct, exclude, note_label)
            except Exception:
                logger.debug("Skipping invalid contract rule: %s", rule, exc_info=True)
    else:
        log_stage("Applying contract percent adjustments per IDM Chapter 20 guidance (built-in defaults)")
        _apply_contract_percent("105-06845", 0.02, {"105-06845", "110-01001"}, "Per IDM Chapter 20:")
        _apply_contract_percent("110-01001", 0.05, {"105-06845", "110-01001"}, "Per IDM Chapter 20:")

    if dm2321_deleted_items:
        log_detail(f"dm2321_deleted_items_skipped={len(dm2321_deleted_items):,}")

    log_stage("Compiling estimator dataframe for export")
    df = pd.DataFrame(rows)
    log_detail(f"estimate_dataframe_shape => rows={len(df):,} | columns={len(df.columns)}")

    log_stage("Evaluating alternate-seek narrative generation pipeline")
    ai_report_path = None
    if alternate_reports and ai_enabled:
        try:
            ai_report_path = generate_alternate_seek_report(
                df,
                alternate_reports,
                output_dir=output_dir,
                project_region=project_region,
                expected_contract_cost=expected_contract_cost,
                filtered_bounds=filtered_bounds,
                contract_filter_pct=contract_filter_pct,
            )
            if ai_report_path:
                log_detail(f"alternate_seek_report_emitted => {ai_report_path}")
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Warning: unable to generate alternate-seek AI report: %s", exc)
    elif alternate_reports and not ai_enabled:
        logger.info("AI reporting disabled; skipping alternate-seek narrative generation.")

    log_stage("Persisting estimator outputs to disk")
    write_outputs(df, str(out_xlsx), str(out_audit), payitem_details, str(out_pay_audit))
    log_detail(f"outputs_written => {out_xlsx}, {out_audit}, {out_pay_audit}")

    # Emit run provenance/metadata for reliability and auditability
    try:
        # Avoid expensive spec PDF parsing; read spec cache if present
        try:
            spec_sections_count = 0
            if hasattr(_refdata, 'SPEC_CACHE') and _refdata.SPEC_CACHE.exists():
                spec_sections_count = len(json.loads(_refdata.SPEC_CACHE.read_text(encoding='utf-8')))
        except Exception:
            spec_sections_count = 0

        meta = {
            "timestamp": pd.Timestamp.utcnow().isoformat(),
            "python": sys.version.split()[0],
            "apply_dm23_21": bool(runtime_cfg.apply_dm23_21),
            "disable_ai": bool(runtime_cfg.disable_ai),
            "disable_alt_seek": bool(runtime_cfg.disable_alt_seek),
            "min_sample_target": int(runtime_cfg.min_sample_target),
            "aggregate_method": os.getenv('AGGREGATE_METHOD', 'WGT_AVG'),
            "category_sigma_threshold": float(os.getenv('CATEGORY_SIGMA_THRESHOLD', '2.0')),
            "memo_rollup_sigma_threshold": float(os.getenv('MEMO_ROLLUP_SIGMA_THRESHOLD', '2.0')),
            "quantity_elasticity_enabled": os.getenv('ENABLE_QUANTITY_ELASTICITY', '0') in {'1','true','on','yes'},
            "spec_edition": spec_edition,
            "inputs": {
                "bidtabs_dir": str(bidtabs_dir),
                "quantities_path": str(qty_path),
                "project_attributes": str(project_attrs_display),
            },
            "reference_cache_sizes": {
                "payitems": len(_refdata.load_payitem_catalog()),
                "unit_price_summary": len(_refdata.load_unit_price_summary()),
                "spec_sections": spec_sections_count,
            },
        }
        out_meta = (output_dir / "run_metadata.json").resolve()
        out_meta.parent.mkdir(parents=True, exist_ok=True)
        with open(out_meta, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2)
        log_detail(f"run_metadata_emitted => {out_meta}")
    except Exception:  # pragma: no cover - defensive
        logger.debug("Unable to emit run metadata", exc_info=True)

    mapping_debug_path = None
    if config is not None:
        mapping_debug_path = config.mapping_debug_csv
        try:
            default_debug = output_dir / "payitem_mapping_debug.csv"
            if default_debug.exists():
                import shutil
                mapping_debug_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(default_debug, mapping_debug_path)
        except Exception:  # pragma: no cover - defensive
            logger.debug("Suppressing debug file copy error", exc_info=True)

    logger.info("\n=== SUMMARY ===\n")
    logger.info("%s", make_summary_text(df))
    try:
        alt_count = int(df.get("ALTERNATE_USED", pd.Series([False] * len(df))).fillna(False).astype(bool).sum())
        logger.info("Alternates used: %s", alt_count)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Unable to compute alternate count", exc_info=True)
    logger.info("\nInputs used:")
    logger.info(" - BidTabs folder: %s", bidtabs_dir)
    logger.info(" - Quantities file: %s", Path(qty_path).resolve())
    logger.info(" - Project attributes: %s", project_attrs_display)
    if project_district_name:
        district_label = next(
            (name for _, name in DISTRICT_CHOICES if name == project_district_name),
            project_district_name,
        )
        if project_region is not None:
            logger.info("   Project district: %s (region %s)", district_label, project_region)
        else:
            logger.info("   Project district: %s", district_label)
    elif project_region is not None:
        logger.info("   Project region: %s", project_region)
    else:
        logger.info("   Project region: (not provided)")
    if region_map is not None and not getattr(region_map, "empty", False):
        logger.info("   Region map rows: %s", len(region_map))
    if Path(aliases_path).exists():
        logger.info(" - Code aliases: %s", aliases_path)
    if expected_contract_cost is not None:
        logger.info(" - Expected contract cost: $%s", f"{expected_contract_cost:,.0f}")
        if filtered_bounds is not None:
            low, high = filtered_bounds
            logger.info("   Filter bounds applied: $%s to $%s", f"{low:,.0f}", f"{high:,.0f}")
    logger.info("\nOutputs written:")
    logger.info(" - %s", out_xlsx)
    logger.info(" - %s", out_audit)
    logger.info(" - %s", out_pay_audit)
    if ai_report_path:
        logger.info(" - %s", ai_report_path)
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate cost estimate outputs from BidTabs history")
    parser.add_argument("--bidtabs-dir", help="Directory containing BidTabs files")
    parser.add_argument("--quantities-xlsx", help="Path to project quantities workbook")
    parser.add_argument("--project-attributes", help="Path to project attributes workbook")
    parser.add_argument("--region-map", help="Optional region map CSV/XLSX")
    parser.add_argument("--aliases-csv", help="Optional code alias CSV")
    parser.add_argument("--output-dir", help="Directory for generated outputs")
    parser.add_argument("--disable-ai", action="store_true", help="Disable OpenAI usage for alternate-seek weighting")
    parser.add_argument("--min-sample-target", type=int, help="Override minimum data points target per item")
    parser.add_argument(
        "--apply-dm23-21",
        action="store_true",
        help="Enable HMA remapping + transitional adders per INDOT DM 23-21.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase logging verbosity")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    runtime_cfg = load_runtime_config(os.environ, args)
    log_level = logging.DEBUG if runtime_cfg.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(message)s")
    try:
        return run(runtime_config=runtime_cfg)
    except Exception:  # pragma: no cover - defensive
        logger.exception("Fatal error during estimate generation")
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
