"""Approval workflow utilities."""
from __future__ import annotations

import email
import imaplib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

from .config import MemoConfig

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

    def check(self, memo_ids: Iterable[str]) -> List[ApprovalResult]:
        approval_cfg = self.config.approval
        mailbox_cfg = approval_cfg.mailbox
        if approval_cfg.method != "email-reply" or not mailbox_cfg:
            LOGGER.info("Approval method %s not implemented for automatic checking", approval_cfg.method)
            return [ApprovalResult(memo_id=memo_id, approved=False) for memo_id in memo_ids]

        phrase_format = approval_cfg.phrase_format
        phrases = {memo_id: phrase_format.format(memo_id=memo_id) for memo_id in memo_ids}

        LOGGER.info("Connecting to IMAP server %s", mailbox_cfg.host)
        with imaplib.IMAP4_SSL(mailbox_cfg.host, mailbox_cfg.port) as imap:
            imap.login(mailbox_cfg.username, mailbox_cfg.password)
            imap.select(mailbox_cfg.folder)
            results: List[ApprovalResult] = []
            for memo_id, phrase in phrases.items():
                status, data = imap.search(None, f'(SUBJECT "{phrase}")')
                if status != "OK":
                    LOGGER.error("Search failed for memo %s", memo_id)
                    results.append(ApprovalResult(memo_id=memo_id, approved=False))
                    continue
                mail_ids = data[0].split()
                if not mail_ids:
                    results.append(ApprovalResult(memo_id=memo_id, approved=False))
                    continue
                latest_id = mail_ids[-1]
                status, message_data = imap.fetch(latest_id, "(RFC822)")
                if status != "OK":
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


__all__ = ["ApprovalChecker", "ApprovalResult"]
