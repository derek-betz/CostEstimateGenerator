from pathlib import Path

from costest.hma_dm2321 import (
    DM2321_ADDERS_PER_TON,
    load_crosswalk,
    maybe_apply_dm2321_adder,
    remap_item,
)

DATA = Path(__file__).resolve().parents[1] / "data_reference"


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
