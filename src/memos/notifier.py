"""Notification helpers for memo automation."""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable

from .config import MemoConfig
from .retry import CircuitBreaker, CircuitBreakerOpen, execute_with_retry

LOGGER = logging.getLogger(__name__)


class MemoNotifier:
    """Sends email notifications for newly parsed memos."""

    def __init__(self, config: MemoConfig) -> None:
        self.config = config
        retry_policy = (
            config.notification.smtp.retry
            if config.notification.smtp
            else None
        )
        threshold = retry_policy.circuit_breaker_failures if retry_policy else 0
        self._breaker = CircuitBreaker(threshold)

    def notify(
        self,
        subject: str,
        body: str,
        attachments: Iterable[Path] | None = None,
        *,
        force: bool = False,
    ) -> bool:
        if not self.config.notification.enabled and not force:
            LOGGER.info("Notifications are disabled; skipping email send")
            return False
        smtp_cfg = self.config.notification.smtp
        if not smtp_cfg:
            raise ValueError("SMTP configuration missing for notifications")
        if not self.config.notification.sender:
            raise ValueError("Notification sender email is not configured")
        if not self.config.notification.recipients:
            LOGGER.warning("No recipients configured; notification skipped")
            return False

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.config.notification.sender
        message["To"] = ", ".join(self.config.notification.recipients)
        message.set_content(body)

        for attachment in attachments or []:
            with attachment.open("rb") as f:
                data = f.read()
            message.add_attachment(
                data,
                maintype="application",
                subtype="octet-stream",
                filename=attachment.name,
            )

        LOGGER.info("Sending memo notification to %s", message["To"])

        if self._breaker.is_open:
            LOGGER.error("SMTP circuit breaker open; notification skipped")
            return False

        def _send(timeout: float) -> None:
            with smtplib.SMTP(smtp_cfg.host, smtp_cfg.port, timeout=timeout) as smtp:
                if smtp_cfg.use_tls:
                    smtp.starttls()
                if smtp_cfg.username and smtp_cfg.password:
                    smtp.login(smtp_cfg.username, smtp_cfg.password)
                smtp.send_message(message)

        try:
            execute_with_retry(
                _send,
                policy=smtp_cfg.retry,
                description="SMTP notification send",
                logger=LOGGER,
                breaker=self._breaker,
            )
        except CircuitBreakerOpen:
            LOGGER.error("SMTP circuit breaker open; notification skipped")
            return False
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.error("Failed to send notification: %s", exc)
            return False
        return True


__all__ = ["MemoNotifier"]
