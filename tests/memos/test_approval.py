from __future__ import annotations

from email.message import EmailMessage
from pathlib import Path

import pytest

from costest.memos.approval import ApprovalChecker
from costest.memos.config import MemoConfig


class DummyIMAP:
    def __init__(self, host: str, port: int, timeout=None):
        self.host = host
        self.port = port
        self.logged_in = False
        self.selected_folder = None

    def __enter__(self) -> "DummyIMAP":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def login(self, username: str, password: str) -> None:
        self.logged_in = True

    def select(self, folder: str) -> None:
        self.selected_folder = folder

    def search(self, charset, criteria):
        return "OK", [b"1 2"]

    def fetch(self, mail_id, spec):
        message = EmailMessage()
        message["Subject"] = "Approved memo-1"
        message["From"] = "approver@example.com"
        message["Date"] = "Mon, 1 Jan 2024 00:00:00 -0000"
        message.set_content("Approved memo-1")
        return "OK", [(b"1", message.as_bytes())]


@pytest.fixture
def approval_config(tmp_path: Path) -> MemoConfig:
    return MemoConfig.from_dict(
        {
            "memo_page_url": "https://example.com",
            "approval": {
                "method": "email-reply",
                "mailbox": {
                    "host": "imap.example.com",
                    "port": 993,
                    "username": "user",
                    "password": "pass",
                },
            },
        }
    )


def test_approval_checker(monkeypatch, approval_config) -> None:
    monkeypatch.setattr("costest.memos.approval.imaplib.IMAP4_SSL", DummyIMAP)
    checker = ApprovalChecker(approval_config)
    results = checker.check(["memo-1"])
    assert results[0].approved
    assert results[0].approver == "approver@example.com"


def test_approval_circuit_breaker(monkeypatch, approval_config) -> None:
    def failing_imap(*args, **kwargs):
        raise OSError("imap failure")

    monkeypatch.setattr("costest.memos.approval.imaplib.IMAP4_SSL", failing_imap)
    approval_config.approval.mailbox.retry.retries = 0
    approval_config.approval.mailbox.retry.circuit_breaker_failures = 1
    checker = ApprovalChecker(approval_config)
    results = checker.check(["memo-1"])
    assert not results[0].approved
    results_second = checker.check(["memo-2"])
    assert not results_second[0].approved
