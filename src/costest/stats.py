"""Statistical helpers for cost estimate generation."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

try:  # pragma: no cover - exercised via fallback in tests
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover - triggered in constrained envs
    np = None  # type: ignore[assignment]
import statistics

MEAN_FLOOR = 1e-6

@dataclass
class StatsSummary:
    """Represents computed statistics for a single pay item."""

    data_points: int
    mean: float
    std_dev: float
    coef_var: float
    confidence: float
    source: str = ""
    notes: str = ""
    fallback_used: bool = False

    @property
    def std_for_display(self) -> str:
        if self.data_points == 0:
            return "N/A"
        return f"{self.std_dev:.2f}"

    @property
    def cv_for_display(self) -> str:
        if not math.isfinite(self.coef_var):
            return "N/A"
        return f"{self.coef_var:.4f}"


def to_float_sequence(values: Iterable[float]) -> Sequence[float]:
    cleaned = []
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isnan(number):
            continue
        cleaned.append(number)
    return cleaned

def mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    if np is not None:
        return float(np.mean(values))
    return float(statistics.mean(values))

def std_dev(values: Sequence[float]) -> float:
    n = len(values)
    if n <= 1:
        return 0.0
    if np is not None:
        return float(np.std(values, ddof=1))
    return float(statistics.stdev(values))


def coefficient_of_variation(mean_value: float, std_value: float) -> float:
    mean_floor = max(abs(mean_value), MEAN_FLOOR)
    return float(std_value) / mean_floor


def confidence_score(data_points: int, coef_var: float) -> float:
    if data_points <= 0 or not math.isfinite(coef_var):
        return 0.0
    return (1.0 - math.exp(-data_points / 30.0)) * (1.0 / (1.0 + coef_var))

def compute_summary(values: Iterable[float]) -> StatsSummary:
    numeric_values = to_float_sequence(values)
    count = len(numeric_values)
    if count == 0:
        return StatsSummary(
            data_points=0,
            mean=0.0,
            std_dev=0.0,
            coef_var=math.inf,
            confidence=0.0,
            notes="No historical data",
            fallback_used=True,
        )

    avg = mean(numeric_values)
    stdev = std_dev(numeric_values)
    cv = coefficient_of_variation(avg, stdev)
    conf = confidence_score(count, cv)
    return StatsSummary(
        data_points=count,
        mean=avg,
        std_dev=stdev,
        coef_var=cv,
        confidence=conf,
    )


__all__ = [
    "StatsSummary",
    "MEAN_FLOOR",
    "to_float_sequence",
    "mean",
    "std_dev",
    "coefficient_of_variation",
    "confidence_score",
    "compute_summary",
]
