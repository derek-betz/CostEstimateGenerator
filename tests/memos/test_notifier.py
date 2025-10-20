from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from costest.memos.config import MemoConfig
from costest.memos.notifier import MemoNotifier


class DummySMTP:
    def __init__(self, host: str, port: int, timeout: float):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started_tls = False
        self.logged_in = False
        self.sent_messages: List[object] = []

    def __enter__(self) -> "DummySMTP":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def starttls(self) -> None:
        self.started_tls = True

    def login(self, username: str, password: str) -> None:
        self.logged_in = True

    def send_message(self, message) -> None:
        self.sent_messages.append(message)


@pytest.fixture
def notifier_config(tmp_path: Path) -> MemoConfig:
    config = MemoConfig.from_dict(
        {
            "memo_page_url": "https://example.com",
            "storage_root": str(tmp_path / "refs"),
            "notification": {
                "enabled": True,
                "sender": "sender@example.com",
                "recipients": ["r@example.com"],
                "smtp": {
                    "host": "smtp.example.com",
                    "port": 587,
                    "use_tls": True,
                    "username": "user",
                    "password": "pass",
                    "retry": {"retries": 1, "backoff_factor": 0.0},
                },
            },
        }
    )
    return config


def test_notifier_sends_email(monkeypatch, notifier_config) -> None:
    smtp_instance = DummySMTP("smtp.example.com", 587, 30)
    monkeypatch.setattr("smtplib.SMTP", lambda host, port, timeout=None: smtp_instance)

    notifier = MemoNotifier(notifier_config)
    assert notifier.notify("Subject", "Body")
    assert smtp_instance.started_tls
    assert smtp_instance.logged_in
    assert len(smtp_instance.sent_messages) == 1


def test_notifier_retries(monkeypatch, notifier_config) -> None:
    attempts = {"count": 0}

    class FlakySMTP(DummySMTP):
        def send_message(self, message) -> None:
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise OSError("temporary failure")
            super().send_message(message)

    smtp_instance = FlakySMTP("smtp.example.com", 587, 30)
    monkeypatch.setattr("smtplib.SMTP", lambda host, port, timeout=None: smtp_instance)
    monkeypatch.setattr("costest.memos.retry.time.sleep", lambda _: None)
    notifier_config.notification.smtp.retry.retries = 2

    notifier = MemoNotifier(notifier_config)
    assert notifier.notify("Subject", "Body")
    assert attempts["count"] == 2


def test_notifier_force(monkeypatch, notifier_config) -> None:
    notifier_config.notification.enabled = False
    smtp_instance = DummySMTP("smtp.example.com", 587, 30)
    monkeypatch.setattr("smtplib.SMTP", lambda host, port, timeout=None: smtp_instance)

    notifier = MemoNotifier(notifier_config)
    assert notifier.notify("Subject", "Body", force=True)


def test_notifier_disabled(monkeypatch, notifier_config) -> None:
    notifier_config.notification.enabled = False
    notifier = MemoNotifier(notifier_config)
    assert notifier.notify("Subject", "Body") is False
