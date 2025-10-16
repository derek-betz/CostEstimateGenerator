"""Retry helpers for memo networking operations."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar

from .config import RetryPolicy

T = TypeVar("T")


class CircuitBreakerOpen(RuntimeError):
    """Raised when a circuit breaker has been tripped for the operation."""


@dataclass
class CircuitBreaker:
    """Tracks consecutive failures for an operation within a workflow run."""

    threshold: int
    consecutive_failures: int = 0

    def record_success(self) -> None:
        self.consecutive_failures = 0

    def record_failure(self) -> None:
        if self.threshold <= 0:
            return
        self.consecutive_failures += 1

    @property
    def is_open(self) -> bool:
        return self.threshold > 0 and self.consecutive_failures >= self.threshold


def execute_with_retry(
    action: Callable[[float], T],
    *,
    policy: RetryPolicy,
    description: str,
    logger,
    breaker: Optional[CircuitBreaker] = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> T:
    """Execute ``action`` with retry/backoff semantics."""

    if breaker and breaker.is_open:
        raise CircuitBreakerOpen(f"Circuit breaker open for {description}")

    attempt = 0
    while True:
        try:
            result = action(policy.timeout_seconds)
        except Exception as exc:  # pragma: no cover - defensive
            attempt += 1
            if attempt > policy.retries:
                if breaker:
                    breaker.record_failure()
                raise
            delay = max(0.0, policy.backoff_factor * (2 ** (attempt - 1)))
            if delay:
                logger.warning(
                    "Retrying %s in %.2fs (%d/%d attempts) after error: %s",
                    description,
                    delay,
                    attempt,
                    policy.retries,
                    exc,
                )
                sleeper(delay)
            else:
                logger.warning(
                    "Retrying %s (%d/%d attempts) after error: %s",
                    description,
                    attempt,
                    policy.retries,
                    exc,
                )
            continue
        else:
            if breaker:
                breaker.record_success()
            return result
