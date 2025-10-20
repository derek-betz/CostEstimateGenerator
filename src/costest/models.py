from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PayItem:
    """Lightweight representation of a priced pay item row."""

    item_code: str
    description: str
    unit: str
    quantity: float
    unit_price_est: float
    notes: str = ""
    data_points_used: int = 0
    source: str = ""
    alternate_used: bool = False


@dataclass(frozen=True)
class PricingResult:
    """Normalized view of pricing analytics for a single item."""

    price: float
    source: str
    data_points_used: int
    confidence: Optional[float] = None
    notes: str = ""
    std_dev: Optional[float] = None
    coef_var: Optional[float] = None
