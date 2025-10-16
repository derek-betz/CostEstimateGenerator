"""Approval workflow utilities."""
from __future__ import annotations

import email
import imaplib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

from .config import MemoConfig
from .retry import CircuitBreaker, CircuitBreakerOpen, execute_with_retry

LOGGER = logging.getLogger(__name__)


@dataclass
class ApprovalResult:
    memo_id: str
    approved: bool
    approved_at: datetime | None = None
    approver: str | None = None


class ApprovalChecker:
    """Checks mailbox for approval phrases."""

    def __init__(self, config: MemoConfig) -> None:
        self.config = config
        mailbox = config.approval.mailbox
        threshold = mailbox.retry.circuit_breaker_failures if mailbox else 0
        self._breaker = CircuitBreaker(threshold)

    def check(self, memo_ids: Iterable[str]) -> List[ApprovalResult]:
        approval_cfg = self.config.approval
        mailbox_cfg = approval_cfg.mailbox
        if approval_cfg.method != "email-reply" or not mailbox_cfg:
            LOGGER.info("Approval method %s not implemented for automatic checking", approval_cfg.method)
            return [ApprovalResult(memo_id=memo_id, approved=False) for memo_id in memo_ids]

        phrase_format = approval_cfg.phrase_format
        phrases = {memo_id: phrase_format.format(memo_id=memo_id) for memo_id in memo_ids}

        LOGGER.info("Connecting to IMAP server %s", mailbox_cfg.host)
        if self._breaker.is_open:
            LOGGER.error("IMAP circuit breaker open; skipping approval check")
            return [ApprovalResult(memo_id=memo_id, approved=False) for memo_id in memo_ids]

        try:
            return execute_with_retry(
                lambda timeout: _run_imap_checks(mailbox_cfg, phrases, timeout),
                policy=mailbox_cfg.retry,
                description="IMAP approval check",
                logger=LOGGER,
                breaker=self._breaker,
            )
        except CircuitBreakerOpen:
            LOGGER.error("IMAP circuit breaker open; skipping approval check")
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.error("Approval check failed: %s", exc)
        return [ApprovalResult(memo_id=memo_id, approved=False) for memo_id in memo_ids]


__all__ = ["ApprovalChecker", "ApprovalResult"]


def _run_imap_checks(
    mailbox_cfg,
    phrases: dict[str, str],
    timeout: float,
) -> List[ApprovalResult]:
    with imaplib.IMAP4_SSL(mailbox_cfg.host, mailbox_cfg.port, timeout=timeout) as imap:
        imap.login(mailbox_cfg.username, mailbox_cfg.password)
        imap.select(mailbox_cfg.folder)
        results: List[ApprovalResult] = []
        for memo_id, phrase in phrases.items():
            status, data = imap.search(None, f'(SUBJECT "{phrase}")')
            if status != "OK" or not data:
                results.append(ApprovalResult(memo_id=memo_id, approved=False))
                continue
            mail_ids = data[0].split()
            if not mail_ids:
                results.append(ApprovalResult(memo_id=memo_id, approved=False))
                continue
            latest_id = mail_ids[-1]
            status, message_data = imap.fetch(latest_id, "(RFC822)")
            if status != "OK" or not message_data:
                results.append(ApprovalResult(memo_id=memo_id, approved=False))
                continue
            msg = email.message_from_bytes(message_data[0][1])
            date_tuple = email.utils.parsedate_to_datetime(msg["Date"]) if msg["Date"] else None
            approver = email.utils.parseaddr(msg.get("From", ""))[1]
            results.append(
                ApprovalResult(
                    memo_id=memo_id,
                    approved=True,
                    approved_at=date_tuple,
                    approver=approver,
                )
            )
        return results
