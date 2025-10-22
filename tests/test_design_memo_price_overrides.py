import math

from costest import design_memo_prices


def test_manual_override_629_000149():
    guidance = design_memo_prices.lookup_memo_price("629-000149")
    assert guidance is not None, "Expected manual guidance override for 629-000149"
    assert guidance.memo_id.startswith("dm-2025-07-20"), guidance.memo_id
    assert math.isclose(guidance.price, 2.22, rel_tol=1e-9)
    assert (guidance.unit or "").upper() == "SYS"
    # Spot-check that context references the memo intent
    assert "Topsoil Management" in guidance.context


def test_manual_override_629_000150():
    guidance = design_memo_prices.lookup_memo_price("629-000150")
    assert guidance is not None, "Expected manual guidance override for 629-000150"
    assert guidance.memo_id.startswith("dm-2025-07-20"), guidance.memo_id
    assert math.isclose(guidance.price, 1.00, rel_tol=1e-9)
    assert (guidance.unit or "").upper() == "DOL"
    assert "Topsoil Amendment Budget" in guidance.context
