import os
import math
import argparse
from pathlib import Path
from typing import Dict, Optional, Sequence, List, TYPE_CHECKING

import pandas as pd
from dotenv import load_dotenv

from .bidtabs_io import (
    load_bidtabs_files,
    load_quantities,
    load_region_map,
    ensure_region_column,
    find_quantities_file,
)
from .price_logic import category_breakdown
from .alternate_seek import find_alternate_price
from .estimate_writer import write_outputs
from .geometry import parse_geometry
from .ai_reporter import generate_alternate_seek_report
from .reporting import make_summary_text
from . import reference_data
from .ai_process_report import generate_process_improvement_report
if TYPE_CHECKING:
    from .config import CLIConfig

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
    for base in [BASE_DIR, *BASE_DIR.parents]:
        candidate = (base / "API_KEY" / "API_KEY.txt").resolve()
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
                    os.environ.setdefault("OPENAI_API_KEY", value)
                return
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Warning: unable to load API key: {exc}")


def _resolve_path(value: Optional[str], default: Path) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return default.resolve()


_load_api_key_from_file()
load_dotenv(BASE_DIR / ".env")

BIDFOLDER = _resolve_path(os.getenv("BIDTABS_DIR"), DEFAULT_BIDTABS_DIR)
QTY_FILE_GLOB = os.getenv("QTY_FILE_GLOB", DEFAULT_QTY_GLOB)
QTY_PATH = os.getenv("QUANTITIES_XLSX", "").strip()
PROJECT_ATTRS_XLSX = _resolve_path(os.getenv("PROJECT_ATTRS_XLSX"), DEFAULT_PROJECT_ATTRS)
LEGACY_EXPECTED_COST_XLSX = os.getenv("EXPECTED_COST_XLSX", "").strip()
_region_map_env = os.getenv("REGION_MAP_XLSX", "").strip()
LEGACY_REGION_MAP_XLSX = _region_map_env
if not LEGACY_REGION_MAP_XLSX and DEFAULT_REGION_MAP.exists():
    LEGACY_REGION_MAP_XLSX = str(DEFAULT_REGION_MAP.resolve())
ALIASES_CSV = _resolve_path(os.getenv("ALIASES_CSV"), DEFAULT_ALIASES)
OUTPUT_DIR = _resolve_path(os.getenv("OUTPUT_DIR"), DEFAULT_OUTPUT_DIR)
OUT_XLSX = Path(os.getenv("OUTPUT_XLSX", str(OUTPUT_DIR / "Estimate_Draft.xlsx"))).expanduser().resolve()
OUT_AUDIT = Path(os.getenv("OUTPUT_AUDIT", str(OUTPUT_DIR / "Estimate_Audit.csv"))).expanduser().resolve()
OUT_PAYITEM_AUDIT = Path(os.getenv("OUTPUT_PAYITEM_AUDIT", str(OUTPUT_DIR / "PayItems_Audit.xlsx"))).expanduser().resolve()
MIN_SAMPLE_TARGET = int(os.getenv("MIN_SAMPLE_TARGET", "50"))

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

    subset = [col for col in ["ITEM_CODE", "LETTING_DATE", "UNIT_PRICE", "QUANTITY", "BIDDER"] if col in cleaned.columns]
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
                print(f"Warning: unable to migrate expected contract cost from {legacy_expected_path}: {exc}")
        region_map = pd.DataFrame(columns=["DISTRICT", "REGION"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            pd.DataFrame({
                "EXPECTED_TOTAL_CONTRACT_COST": [expected_cost],
                "PROJECT_REGION": [pd.NA],
            }).to_excel(writer, sheet_name="PROJECT", index=False)
            region_map.to_excel(writer, sheet_name="REGION_MAP", index=False)
        print(f"Created project attributes workbook at {path}. Populate it and rerun.")
        return expected_cost, None, region_map

    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Warning: unable to read project attributes file {path}: {exc}")
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
            print(f"Warning: unable to read region map from {legacy_region_map_path}: {exc}")
            region_map_df = pd.DataFrame(columns=["DISTRICT", "REGION"])
    else:
        region_map_df = pd.DataFrame(columns=["DISTRICT", "REGION"])

    try:
        region_map_df = load_region_map(region_map_df)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Warning: region map data in {path} is invalid: {exc}")
        region_map_df = pd.DataFrame(columns=["DISTRICT", "REGION"])

    return expected_cost, project_region, region_map_df


def run(config: Optional["CLIConfig"] = None) -> int:
    # If test-provided config is supplied, override output paths for this run only.
    prev_output_dir = OUTPUT_DIR
    prev_out_xlsx = OUT_XLSX
    prev_out_audit = OUT_AUDIT
    prev_out_pay_audit = OUT_PAYITEM_AUDIT
    if config is not None:
        # Defer import to avoid circular at module import
        try:
            from .config import CLIConfig as _CLI
            assert isinstance(config, _CLI)
        except Exception:
            pass
        # Point outputs to the temp folder provided by tests
        globals()["OUTPUT_DIR"] = config.estimate_audit_csv.parent
        globals()["OUT_AUDIT"] = config.estimate_audit_csv
        globals()["OUT_XLSX"] = config.estimate_xlsx
        globals()["OUT_PAYITEM_AUDIT"] = config.payitems_workbook
    expected_contract_cost, project_region, region_map = load_project_attributes(
        PROJECT_ATTRS_XLSX,
        legacy_expected_path=LEGACY_EXPECTED_COST_XLSX or None,
        legacy_region_map_path=LEGACY_REGION_MAP_XLSX or None,
    )
    reference_data.load_payitem_catalog()
    reference_data.load_unit_price_summary()
    reference_data.load_spec_sections()

    bid = load_bidtabs_files(BIDFOLDER)
    bid = ensure_region_column(bid, region_map)

    geom_info = bid['DESCRIPTION'].apply(parse_geometry)
    bid['GEOM_SHAPE'] = geom_info.map(lambda g: getattr(g, 'shape', None))
    bid['GEOM_AREA_SQFT'] = geom_info.map(lambda g: getattr(g, 'area_sqft', float('nan')))
    bid['GEOM_DIMENSIONS'] = geom_info.map(lambda g: getattr(g, 'dimensions', None))

    if "LETTING_DATE" in bid.columns:
        bid["LETTING_DATE"] = pd.to_datetime(bid["LETTING_DATE"], errors="coerce")
    if "UNIT_PRICE" in bid.columns:
        bid["UNIT_PRICE"] = pd.to_numeric(bid["UNIT_PRICE"], errors="coerce")
    if "WEIGHT" in bid.columns:
        bid["WEIGHT"] = pd.to_numeric(bid["WEIGHT"], errors="coerce")
    if "JOB_SIZE" in bid.columns:
        bid["JOB_SIZE"] = pd.to_numeric(bid["JOB_SIZE"], errors="coerce")

    bid = _sanitize_bidtabs(bid)

    # Allow runtime override of quantities workbook via environment variable
    _qty_override = os.getenv("QUANTITIES_XLSX", "").strip()
    if _qty_override:
        qty_path = Path(_qty_override).expanduser().resolve()
    else:
        qty_path = Path(QTY_PATH).expanduser().resolve() if QTY_PATH else find_quantities_file(QTY_FILE_GLOB, base_dir=BASE_DIR)
    qty = load_quantities(qty_path)

    if Path(ALIASES_CSV).exists():
        alias = pd.read_csv(ALIASES_CSV, dtype=str)
        if not alias.empty:
            alias["PROJECT_CODE"] = alias["PROJECT_CODE"].astype(str).str.strip()
            alias["HIST_CODE"] = alias["HIST_CODE"].astype(str).str.strip()
            amap = dict(zip(alias["PROJECT_CODE"], alias["HIST_CODE"]))
            qty["ITEM_CODE"] = qty["ITEM_CODE"].map(lambda c: amap.get(c, c))

    filtered_bounds = None
    if expected_contract_cost and expected_contract_cost > 0 and "JOB_SIZE" in bid.columns:
        lower_bound = 0.5 * expected_contract_cost
        upper_bound = 1.5 * expected_contract_cost
        before_rows = len(bid)
        mask = bid["JOB_SIZE"].between(lower_bound, upper_bound, inclusive="both")
        bid = bid.loc[mask].copy()
        after_rows = len(bid)
        filtered_bounds = (lower_bound, upper_bound)
        print(
            f"Filtered BidTabs to contracts between ${lower_bound:,.0f} and ${upper_bound:,.0f} (+/-50% of expected ${expected_contract_cost:,.0f}); kept {after_rows} of {before_rows} rows."
        )
        if bid.empty:
            print("WARNING: No BidTabs rows remained after contract cost filtering.")

    rows = []
    payitem_details: Dict[str, pd.DataFrame] = {}
    alternate_reports: Dict[str, Dict[str, object]] = {}
    process_improvement_notes: List[Dict[str, object]] = []

    for _, r in qty.iterrows():
        code = str(r["ITEM_CODE"]).strip()
        desc = str(r.get("DESCRIPTION", "")).strip()
        unit = str(r.get("UNIT", "")).strip()
        qty_val = float(r.get("QUANTITY", 0) or 0)

        price, _, cat_data, detail_map, used_categories, combined_used = category_breakdown(
            bid,
            code,
            project_region=project_region,
            include_details=True,
            target_quantity=(qty_val if qty_val > 0 else None),
        )

        note = ""
        if pd.isna(price):
            price = 0.0
            note = "NO DATA IN ANY CATEGORY; REVIEW."

        used_categories = used_categories or []
        used_category_set = set(used_categories)
        data_points_used = int(cat_data.get("TOTAL_USED_COUNT", len(combined_used)))

        if not note and 0 < data_points_used < MIN_SAMPLE_TARGET:
            note = f"Only {data_points_used} data points found (target {MIN_SAMPLE_TARGET})."

        geometry = parse_geometry(desc)
        reference_bundle = reference_data.build_reference_bundle(code)
        unit_price_est = _round_unit_price(price)

        row: Dict[str, object] = {
            "ITEM_CODE": code,
            "DESCRIPTION": desc,
            "UNIT": unit,
            "QUANTITY": qty_val,
            "UNIT_PRICE_EST": unit_price_est,
            "NOTES": note,
            "DATA_POINTS_USED": data_points_used,
            "ALTERNATE_USED": False,
        }

        if geometry is not None:
            row["GEOM_SHAPE"] = geometry.shape
            row["GEOM_AREA_SQFT"] = round(geometry.area_sqft, 4)
            if geometry.dimensions:
                row["GEOM_DIMENSIONS"] = geometry.dimensions

        if data_points_used == 0 and geometry is not None:
            alt_result = find_alternate_price(
                bid,
                code,
                geometry,
                project_region=project_region,
                target_description=desc,
                reference_bundle=reference_bundle,
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
                    col = 'ALT_SCORE_OVERALL' if key == 'overall_score' else f"ALT_SCORE_{key.replace('_score', '').upper()}"
                    row[col] = round(float(value), 4)
                method_label = "AI weighted alternates"
                if alt_result.ai_notes and 'failed' in str(alt_result.ai_notes).lower():
                    method_label = "Score-based fallback"
                elif all(sel.reason and sel.reason.lower().startswith('fallback') for sel in alt_result.selections):
                    method_label = "Score-based fallback"
                row["ALTERNATE_METHOD"] = method_label
                if alt_result.ai_notes:
                    row["ALTERNATE_AI_NOTES"] = alt_result.ai_notes
                row["NOTES"] = "AI weighted pricing" if method_label == "AI weighted alternates" else "Score-based alternate pricing"
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
                notes_payload = {
                    "item_code": code,
                    "description": desc,
                    "unit": unit,
                    "process_improvements": alt_result.process_improvements,
                    "ai_system": alt_result.ai_system,
                    "show_work_method": alt_result.show_work_method,
                    "references": ref_snapshot,
                    "similarity_summary": similarity_summary,
                    "candidate_notes": alt_result.candidate_notes,
                    "ai_notes": alt_result.ai_notes,
                    "alternate_method": method_label,
                }
                if any(notes_payload.get(key) for key in ("process_improvements", "ai_system", "show_work_method", "similarity_summary")):
                    process_improvement_notes.append(notes_payload)
                alternate_reports[code] = alt_entry
                if alt_result.ai_notes:
                    alt_entry["chosen"]["notes"] = alt_result.ai_notes
                cat_data = alt_result.cat_data
                detail_map = alt_result.detail_map or {}
                used_categories = alt_result.used_categories or []
                combined_used = alt_result.combined_detail

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
            return
        qty_val = float(row_obj.get("QUANTITY", 0) or 0)
        if qty_val <= 0:
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

    _apply_contract_percent("105-06845", 0.02, {"105-06845", "110-01001"}, "Per IDM Chapter 20:")
    _apply_contract_percent("110-01001", 0.05, {"105-06845", "110-01001"}, "Per IDM Chapter 20:")

    df = pd.DataFrame(rows)

    ai_report_path = None
    process_report_path = None
    ai_enabled = os.getenv("DISABLE_OPENAI", "0").strip().lower() not in ("1", "true", "yes")
    if alternate_reports and ai_enabled:
        try:
            ai_report_path = generate_alternate_seek_report(
                df,
                alternate_reports,
                output_dir=OUTPUT_DIR,
                project_region=project_region,
                expected_contract_cost=expected_contract_cost,
                filtered_bounds=filtered_bounds,
            )
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Warning: unable to generate alternate-seek AI report: {exc}")
        try:
            process_overview = {
                "inputs": {
                    "bidtabs_dir": str(BIDFOLDER),
                    "quantities_file": str(Path(qty_path).resolve()),
                    "project_attributes": str(PROJECT_ATTRS_XLSX),
                    "expected_contract_cost": float(expected_contract_cost or 0),
                    "project_region": project_region,
                    "min_sample_target": MIN_SAMPLE_TARGET,
                },
                "statistics": {
                    "bidtabs_rows": int(len(bid)),
                    "quantities_items": int(len(qty)),
                    "alternate_items": len(alternate_reports),
                },
                "filters": {
                    "contract_size_bounds": filtered_bounds,
                },
                "pipeline_steps": [
                    "Load BidTabs history, normalize item codes, dates, and geometry.",
                    "Ensure region column via mapping and optional contract-size filtering.",
                    "Compute category-based pricing hierarchy with geometry-aware adjustments.",
                    "Apply IDM percentage overrides for contract-level items.",
                    "Perform alternate-seek with AI assistance when BidTabs coverage is zero.",
                    "Write Estimate_Draft.xlsx, Estimate_Audit.csv, and PayItems_Audit.xlsx outputs.",
                ],
                "ai_enabled": ai_enabled,
            }
            reference_snapshot = reference_data.snapshot_reference_summary()
            process_report_path = generate_process_improvement_report(
                process_overview=process_overview,
                process_notes=process_improvement_notes,
                reference_snapshot=reference_snapshot,
                output_dir=OUTPUT_DIR,
            )
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Warning: unable to generate process improvement report: {exc}")
    elif alternate_reports and not ai_enabled:
        print("AI reporting disabled; skipping alternate-seek narrative generation.")

    write_outputs(df, str(OUT_XLSX), str(OUT_AUDIT), payitem_details, str(OUT_PAYITEM_AUDIT))

    # If running under tests, mirror mapping debug file to requested path
    if config is not None:
        try:
            default_debug = OUTPUT_DIR / "payitem_mapping_debug.csv"
            target_debug = config.mapping_debug_csv
            if default_debug.exists():
                import shutil
                target_debug.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(default_debug, target_debug)
        except Exception:
            pass

    print("\n=== SUMMARY ===\n")
    print(make_summary_text(df))
    print("\nInputs used:")
    print(" - BidTabs folder:", BIDFOLDER)
    print(" - Quantities file:", Path(qty_path).resolve())
    print(" - Project attributes:", PROJECT_ATTRS_XLSX)
    if project_region is not None:
        print(f"   Project region: {project_region}")
    else:
        print("   Project region: (not provided)")
    if region_map is not None and not getattr(region_map, "empty", False):
        print(f"   Region map rows: {len(region_map)}")
    if Path(ALIASES_CSV).exists():
        print(" - Code aliases:", ALIASES_CSV)
    if expected_contract_cost is not None:
        print(f" - Expected contract cost: ${expected_contract_cost:,.0f}")
        if filtered_bounds is not None:
            low, high = filtered_bounds
            print(f"   Filter bounds applied: ${low:,.0f} to ${high:,.0f}")
    print("\nOutputs written:")
    print(" -", OUT_XLSX)
    print(" -", OUT_AUDIT)
    print(" -", OUT_PAYITEM_AUDIT)
    if ai_report_path:
        print(" -", ai_report_path)
    if process_report_path:
        print(" -", process_report_path)

    # Restore globals if we overrode them
    if config is not None:
        globals()["OUTPUT_DIR"] = prev_output_dir
        globals()["OUT_XLSX"] = prev_out_xlsx
        globals()["OUT_AUDIT"] = prev_out_audit
        globals()["OUT_PAYITEM_AUDIT"] = prev_out_pay_audit

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
    return parser.parse_args(argv)


def apply_cli_overrides(args: argparse.Namespace) -> None:
    global BIDFOLDER, QTY_PATH, PROJECT_ATTRS_XLSX, LEGACY_REGION_MAP_XLSX, ALIASES_CSV
    global OUTPUT_DIR, OUT_XLSX, OUT_AUDIT, OUT_PAYITEM_AUDIT, MIN_SAMPLE_TARGET

    if args.bidtabs_dir:
        BIDFOLDER = Path(args.bidtabs_dir).expanduser().resolve()
    if args.quantities_xlsx:
        QTY_PATH = str(Path(args.quantities_xlsx).expanduser().resolve())
    if args.project_attributes:
        PROJECT_ATTRS_XLSX = Path(args.project_attributes).expanduser().resolve()
    if args.region_map:
        LEGACY_REGION_MAP_XLSX = str(Path(args.region_map).expanduser().resolve())
    if args.aliases_csv:
        ALIASES_CSV = Path(args.aliases_csv).expanduser().resolve()
    if args.output_dir:
        OUTPUT_DIR = Path(args.output_dir).expanduser().resolve()
        global OUT_XLSX, OUT_AUDIT, OUT_PAYITEM_AUDIT
        OUT_XLSX = OUTPUT_DIR / "Estimate_Draft.xlsx"
        OUT_AUDIT = OUTPUT_DIR / "Estimate_Audit.csv"
        OUT_PAYITEM_AUDIT = OUTPUT_DIR / "PayItems_Audit.xlsx"
    if args.disable_ai:
        os.environ["DISABLE_OPENAI"] = "1"
    if args.min_sample_target:
        MIN_SAMPLE_TARGET = max(1, int(args.min_sample_target))


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    apply_cli_overrides(args)
    run()


if __name__ == "__main__":  # pragma: no cover
    main()
