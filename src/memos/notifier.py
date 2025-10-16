"""Notification helpers for memo automation."""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable

from .config import MemoConfig

LOGGER = logging.getLogger(__name__)


class MemoNotifier:
    """Sends email notifications for newly parsed memos."""

    def __init__(self, config: MemoConfig) -> None:
        self.config = config

    def notify(self, subject: str, body: str, attachments: Iterable[Path] | None = None) -> None:
        if not self.config.notification.enabled:
            LOGGER.info("Notifications are disabled; skipping email send")
            return
        smtp_cfg = self.config.notification.smtp
        if not smtp_cfg:
            raise ValueError("SMTP configuration missing for notifications")
        if not self.config.notification.sender:
            raise ValueError("Notification sender email is not configured")
        if not self.config.notification.recipients:
            LOGGER.warning("No recipients configured; notification skipped")
            return

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
        with smtplib.SMTP(smtp_cfg.host, smtp_cfg.port, timeout=30) as smtp:
            if smtp_cfg.use_tls:
                smtp.starttls()
            if smtp_cfg.username and smtp_cfg.password:
                smtp.login(smtp_cfg.username, smtp_cfg.password)
            smtp.send_message(message)


__all__ = ["MemoNotifier"]
