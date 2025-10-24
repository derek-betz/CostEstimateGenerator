import math
from pathlib import Path

import pandas as pd

from costest import design_memo_prices, reference_data
from costest.cli import CATEGORY_LABELS, apply_non_geometry_fallbacks
import costest.cli as cli
from costest.estimate_writer import write_outputs


def _blank_row(item_code: str, quantity: float) -> dict:
    row = {
        "ITEM_CODE": item_code,
        "DESCRIPTION": "Test item",
        "UNIT": "EA",
        "QUANTITY": quantity,
        "UNIT_PRICE_EST": 0.0,
        "NOTES": "NO DATA IN ANY CATEGORY; REVIEW.",
        "DATA_POINTS_USED": 0,
        "ALTERNATE_USED": False,
        "SOURCE": "NO_DATA",
    }
    for label in CATEGORY_LABELS:
        row[f"{label}_PRICE"] = float("nan")
        row[f"{label}_COUNT"] = 0
        row[f"{label}_INCLUDED"] = False
    # Provide minimal STATE/DIST prices so adjustment factors have values
    row["STATE_12M_PRICE"] = 110.0
    row["STATE_24M_PRICE"] = 100.0
    row["STATE_36M_PRICE"] = 95.0
    row["DIST_12M_PRICE"] = 108.0
    row["DIST_24M_PRICE"] = float("nan")
    row["DIST_36M_PRICE"] = float("nan")
    return row


def test_summary_fallback_applies(monkeypatch):
    rows = [_blank_row("123-45678", 120.0)]

    summary_stub = {
        "123-45678": {
            "year": 2024,
            "weighted_average": 150.0,
            "contracts": 12.0,
            "total_value": 180000.0,
            "lowest": 120.0,
            "highest": 210.0,
        }
    }
    monkeypatch.setattr(reference_data, "load_unit_price_summary", lambda: summary_stub)

    bidtabs = pd.DataFrame(columns=["ITEM_CODE", "UNIT_PRICE", "QUANTITY", "REGION"])
    payitem_details: dict[str, pd.DataFrame] = {}

    apply_non_geometry_fallbacks(rows, bidtabs, project_region=5, payitem_details=payitem_details)
    row = rows[0]

    assert row["SOURCE"] == "UNIT_PRICE_SUMMARY"
    assert row["UNIT_PRICE_EST"] > 0
    assert row["DATA_POINTS_USED"] == 12
    assert "UNIT_PRICE_SUMMARY CY2024" in row["NOTES"]
    assert "recency" in row["NOTES"]
    assert "123-45678" in payitem_details
    detail = payitem_details["123-45678"]
    assert detail["CATEGORY"].eq("UNIT_PRICE_SUMMARY").all()
    assert set(detail["SUMMARY_SOURCE_ITEM_CODE"]) == {"123-45678"}
    assert detail["SUMMARY_WEIGHTED_AVERAGE"].iloc[-1] == 150.0


def test_design_memo_rollup_applies(monkeypatch):
    rows = [_blank_row("401-11526", 100.0)]

    summary_stub = {
        "401-11526": {
            "year": 2024,
            "weighted_average": 0.0,
            "contracts": 1.0,
            "total_value": 0.0,
            "lowest": 0.0,
            "highest": 0.0,
        }
    }
    monkeypatch.setattr(reference_data, "load_unit_price_summary", lambda: summary_stub)

    data = [
        {"ITEM_CODE": "401-10258", "UNIT_PRICE": 90.0, "QUANTITY": 95.0, "WEIGHT": 1.0, "REGION": 3, "LETTING_DATE": "2024-01-15"},
        {"ITEM_CODE": "401-10259", "UNIT_PRICE": 110.0, "QUANTITY": 120.0, "WEIGHT": 1.0, "REGION": 3, "LETTING_DATE": "2023-09-20"},
        {"ITEM_CODE": "401-10258", "UNIT_PRICE": 95.0, "QUANTITY": 102.0, "WEIGHT": 1.0, "REGION": 3, "LETTING_DATE": "2023-06-10"},
    ]
    bidtabs = pd.DataFrame(data)
    payitem_details: dict[str, pd.DataFrame] = {}

    apply_non_geometry_fallbacks(rows, bidtabs, project_region=3, payitem_details=payitem_details)
    row = rows[0]

    assert row["SOURCE"] == "DESIGN_MEMO_ROLLUP"
    assert row["UNIT_PRICE_EST"] > 0
    assert row["DATA_POINTS_USED"] == 3
    assert math.isfinite(row["COEF_VAR"])
    assert "DESIGN_MEMO_ROLLUP DM 25-10" in row["NOTES"]
    assert "summary insufficient" in row["NOTES"]

    detail = payitem_details["401-11526"]
    assert "CATEGORY" in detail.columns
    assert detail["CATEGORY"].eq("DESIGN_MEMO_ROLLUP").all()


def test_design_memo_rollup_relaxes_quantity_filter(monkeypatch):
    rows = [_blank_row("401-11526", 5000.0)]

    summary_stub = {
        "401-11526": {
            "year": 2024,
            "weighted_average": 0.0,
            "contracts": 1.0,
            "total_value": 0.0,
            "lowest": 0.0,
            "highest": 0.0,
        }
    }
    monkeypatch.setattr(reference_data, "load_unit_price_summary", lambda: summary_stub)

    data = [
        {"ITEM_CODE": "401-10258", "UNIT_PRICE": 90.0, "QUANTITY": 40.0, "WEIGHT": 1.0, "REGION": 3, "LETTING_DATE": "2024-01-15"},
        {"ITEM_CODE": "401-10259", "UNIT_PRICE": 92.0, "QUANTITY": 55.0, "WEIGHT": 1.0, "REGION": 3, "LETTING_DATE": "2023-09-20"},
        {"ITEM_CODE": "401-10258", "UNIT_PRICE": 88.0, "QUANTITY": 60.0, "WEIGHT": 1.0, "REGION": 3, "LETTING_DATE": "2023-06-10"},
    ]
    bidtabs = pd.DataFrame(data)
    payitem_details: dict[str, pd.DataFrame] = {}

    apply_non_geometry_fallbacks(rows, bidtabs, project_region=3, payitem_details=payitem_details)
    row = rows[0]

    assert row["SOURCE"] == "DESIGN_MEMO_ROLLUP"
    assert row["UNIT_PRICE_EST"] > 0
    assert "quantity filter relaxed" in row["NOTES"]

    detail = payitem_details["401-11526"]
    assert detail["CATEGORY"].eq("DESIGN_MEMO_ROLLUP").all()
    assert detail["QUANTITY_FILTER_RELAXED"].all()


def test_design_memo_summary_average_fallback(monkeypatch):
    rows = [_blank_row("401-11526", 500.0)]

    summary_stub = {
        "401-10258": {
            "year": 2024,
            "weighted_average": 120.0,
            "contracts": 10.0,
            "total_value": 240000.0,
            "lowest": 90.0,
            "highest": 140.0,
        },
        "401-10259": {
            "year": 2024,
            "weighted_average": 80.0,
            "contracts": 8.0,
            "total_value": 160000.0,
            "lowest": 70.0,
            "highest": 110.0,
        },
        "401-11526": {
            "year": 2024,
            "weighted_average": 0.0,
            "contracts": 0.0,
            "total_value": 0.0,
            "lowest": 0.0,
            "highest": 0.0,
        },
    }
    monkeypatch.setattr(reference_data, "load_unit_price_summary", lambda: summary_stub)

    def _recency_stub(df: pd.DataFrame) -> float:
        _recency_stub.last_meta = {"used_default": True}
        return 1.0

    _recency_stub.last_meta = {"used_default": True}
    monkeypatch.setattr(cli, "compute_recency_factor", _recency_stub)

    def _region_stub(df: pd.DataFrame, project_region=None) -> float:
        _region_stub.last_meta = {"used_default": True}
        return 1.0

    _region_stub.last_meta = {"used_default": True}
    monkeypatch.setattr(cli, "compute_region_factor", _region_stub)

    bidtabs = pd.DataFrame(columns=["ITEM_CODE", "UNIT_PRICE", "QUANTITY", "REGION"])
    payitem_details: dict[str, pd.DataFrame] = {}

    apply_non_geometry_fallbacks(rows, bidtabs, project_region=3, payitem_details=payitem_details)
    row = rows[0]

    assert row["SOURCE"] == "DESIGN_MEMO_SUMMARY"
    assert math.isclose(float(row["UNIT_PRICE_EST"]), 100.0, rel_tol=1e-6)
    assert row["DATA_POINTS_USED"] == 18
    assert "DESIGN_MEMO_SUMMARY DM 25-10" in row["NOTES"]
    assert "401-10258 WGT_AVG=120.00" in row["NOTES"]
    assert "401-10259 WGT_AVG=80.00" in row["NOTES"]

    detail = payitem_details["401-11526"]
    assert detail["CATEGORY"].eq("DESIGN_MEMO_SUMMARY").all()
    assert set(detail["SUMMARY_SOURCE_ITEM_CODE"]) == {"401-10258", "401-10259"}
    assert math.isclose(
        detail.loc[detail["SUMMARY_SOURCE_ITEM_CODE"] == "401-10258", "SUMMARY_WEIGHTED_AVERAGE"].iloc[0],
        120.0,
        rel_tol=1e-6,
    )
    assert math.isclose(
        detail.loc[detail["SUMMARY_SOURCE_ITEM_CODE"] == "401-10259", "SUMMARY_WEIGHTED_AVERAGE"].iloc[0],
        80.0,
        rel_tol=1e-6,
    )


def test_design_memo_price_guidance_applies(monkeypatch):
    rows = [_blank_row("629-000150", 250.0)]

    monkeypatch.setattr(reference_data, "load_unit_price_summary", lambda: {})

    guidance = design_memo_prices.MemoPriceGuidance(
        memo_id="25-07",
        price=2.22,
        unit="SYS",
        context="unit price of $2.22 per SYS may be used until a bid history is established",
        effective_date="September 1, 2025",
        extracted_at="2025-10-16T17:24:22-0600",
        source_path=None,
    )
    monkeypatch.setattr(
        design_memo_prices,
        "lookup_memo_price",
        lambda code: guidance if code == "629-000150" else None,
    )

    bidtabs = pd.DataFrame(columns=["ITEM_CODE", "UNIT_PRICE", "QUANTITY", "REGION"])
    payitem_details: dict[str, pd.DataFrame] = {}

    apply_non_geometry_fallbacks(rows, bidtabs, project_region=None, payitem_details=payitem_details)
    row = rows[0]

    assert row["SOURCE"] == "DESIGN_MEMO_PRICE"
    assert math.isclose(float(row["UNIT_PRICE_EST"]), 2.22, rel_tol=1e-6)
    assert "DESIGN_MEMO_PRICE DM 25-07" in row["NOTES"]
    assert "recommended $2.22" in row["NOTES"]
    assert row["DATA_POINTS_USED"] == 0

    detail = payitem_details["629-000150"]
    assert detail["CATEGORY"].eq("DESIGN_MEMO_PRICE").all()
    assert detail["MEMO_ID"].iloc[0] == "25-07"
    assert math.isclose(detail["RECOMMENDED_PRICE"].iloc[0], 2.22, rel_tol=1e-6)


def test_confidence_generated_for_fallback(tmp_path, monkeypatch):
    rows = [_blank_row("123-45678", 120.0)]
    summary_stub = {
        "123-45678": {
            "year": 2024,
            "weighted_average": 150.0,
            "contracts": 20.0,
            "total_value": 240000.0,
            "lowest": 120.0,
            "highest": 220.0,
        }
    }
    monkeypatch.setattr(reference_data, "load_unit_price_summary", lambda: summary_stub)

    bidtabs = pd.DataFrame(columns=["ITEM_CODE", "UNIT_PRICE", "QUANTITY", "REGION"])
    payitem_details: dict[str, pd.DataFrame] = {}
    apply_non_geometry_fallbacks(rows, bidtabs, project_region=None, payitem_details=payitem_details)

    df = pd.DataFrame(rows)
    out_dir = Path(tmp_path)
    xlsx_path = out_dir / "estimate.xlsx"
    audit_path = out_dir / "audit.csv"
    pay_audit = out_dir / "pay_audit.xlsx"
    write_outputs(df, str(xlsx_path), str(audit_path), payitem_details, str(pay_audit))

    audit_df = pd.read_csv(audit_path)
    assert "CONFIDENCE" in audit_df.columns
    assert (audit_df["CONFIDENCE"].between(0.0, 1.0)).all()


    pass  # placeholder to keep line numbers stable if needed
