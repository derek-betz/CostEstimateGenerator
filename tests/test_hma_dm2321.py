from pathlib import Path

import pandas as pd

from costest.bidtabs_io import load_bidtabs_files
from costest.hma_dm2321 import (
    DM2321_ADDERS_PER_TON,
    load_crosswalk,
    maybe_apply_dm2321_adder,
    remap_item,
)
from costest.price_logic import category_breakdown

DATA = Path(__file__).resolve().parents[1] / "data_reference"
BIDTABS = Path(__file__).resolve().parents[1] / "data_sample" / "BidTabsData"


def test_remap_active_item():
    xwalk = load_crosswalk(DATA / "hma_crosswalk_dm23_21.csv")
    new_item, meta = remap_item("401-07321", xwalk)
    assert new_item == "401-000001"
    assert meta["mapping_rule"] == "DM 23-21"
    assert meta["course"] == "Surface"


def test_deleted_item_is_flagged():
    xwalk = load_crosswalk(DATA / "hma_crosswalk_dm23_21.csv")
    new_item, meta = remap_item("410-10128", xwalk)
    assert new_item is None
    assert meta["deleted"] is True


def test_adder_applies_until_history_is_sufficient():
    price, applied = maybe_apply_dm2321_adder(
        "Surface", 95.0, enabled=True, sufficient_history=False
    )
    assert price == 98.0
    assert applied is True
    # Once history is sufficient, no adder should apply even if enabled
    price2, applied2 = maybe_apply_dm2321_adder(
        "Surface", 95.0, enabled=True, sufficient_history=True
    )
    assert price2 == 95.0
    assert applied2 is False


def test_adder_respects_course_case_insensitivity():
    base = DM2321_ADDERS_PER_TON.get("Surface")
    price, applied = maybe_apply_dm2321_adder(
        "surface", 100.0, enabled=True, sufficient_history=False
    )
    assert applied is True
    assert price == 100.0 + base


def test_remap_unknown_item_returns_original():
    xwalk = load_crosswalk(DATA / "hma_crosswalk_dm23_21.csv")
    new_item, meta = remap_item("999-99999", xwalk)
    assert new_item == "999-99999"
    assert meta["mapping_rule"] is None
    assert meta["deleted"] is False


def test_dm2321_quantity_band_is_disabled_for_history():
    xwalk = load_crosswalk(DATA / "hma_crosswalk_dm23_21.csv")
    bid = load_bidtabs_files(BIDTABS)

    keep_idx = []
    mapped_codes: list[str] = []
    for idx, code in enumerate(bid["ITEM_CODE"].astype(str)):
        new_code, meta = remap_item(code, xwalk)
        if meta.get("deleted") and new_code is None:
            continue
        keep_idx.append(idx)
        mapped_codes.append(new_code or code)

    bid = bid.iloc[keep_idx].copy()
    bid["ITEM_CODE"] = mapped_codes
    bid["UNIT_PRICE"] = pd.to_numeric(bid["UNIT_PRICE"], errors="coerce")
    bid["QUANTITY"] = pd.to_numeric(bid["QUANTITY"], errors="coerce")
    bid["JOB_SIZE"] = pd.to_numeric(bid.get("JOB_SIZE"), errors="coerce")
    bid = bid.loc[bid["UNIT_PRICE"] > 0].copy()

    expected_contract_cost = 5_000_000
    lower = expected_contract_cost * 0.5
    upper = expected_contract_cost * 1.5
    bid = bid.loc[bid["JOB_SIZE"].between(lower, upper, inclusive="both")].copy()

    _, _, cat_with_band, *_ = category_breakdown(
        bid,
        "401-000041",
        project_region=2,
        include_details=True,
        target_quantity=1471.0,
    )

    _, _, cat_without_band, *_ = category_breakdown(
        bid,
        "401-000041",
        project_region=2,
        include_details=True,
        target_quantity=None,
    )

    assert cat_without_band["TOTAL_USED_COUNT"] >= 30
    assert cat_without_band["TOTAL_USED_COUNT"] > cat_with_band["TOTAL_USED_COUNT"]
