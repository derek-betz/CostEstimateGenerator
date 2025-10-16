from __future__ import annotations

from pathlib import Path

import pytest

from memos.config import MemoConfig
from pytest import MonkeyPatch


def test_config_env_overrides(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
{
  "memo_page_url": "https://example.com",
  "notification": {"enabled": true},
  "approval": {"method": "email-reply"}
}
        """.strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_USERNAME", "user")
    monkeypatch.setenv("SMTP_PASSWORD", "pass")
    monkeypatch.setenv("SMTP_SENDER", "sender@example.com")
    monkeypatch.setenv("SMTP_RECIPIENTS", "one@example.com,two@example.com")
    monkeypatch.setenv("SMTP_NOTIFY_ON_FAILURE", "1")
    monkeypatch.setenv("IMAP_HOST", "imap.test")
    monkeypatch.setenv("IMAP_USERNAME", "imap-user")
    monkeypatch.setenv("IMAP_PASSWORD", "imap-pass")

    config = MemoConfig.load(config_file)

    assert config.notification.smtp is not None
    assert config.notification.smtp.host == "smtp.test"
    assert config.notification.sender == "sender@example.com"
    assert list(config.recipients) == ["one@example.com", "two@example.com"]
    assert config.notification.enabled_on_failure is True
    assert config.approval.mailbox is not None
    assert config.approval.mailbox.host == "imap.test"


def test_config_retry_env_overrides(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
{
  "memo_page_url": "https://example.com",
  "http": {"retries": 1},
  "notification": {"enabled": false},
  "approval": {"method": "none"}
}
        """.strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("MEMO_HTTP_TIMEOUT", "45")
    monkeypatch.setenv("MEMO_HTTP_BACKOFF", "0.5")
    monkeypatch.setenv("MEMO_DOWNLOAD_RETRIES", "2")

    config = MemoConfig.load(config_file)

    assert config.http_retry.timeout_seconds == 45.0
    assert config.http_retry.retries == 1
    assert config.http_retry.backoff_factor == 0.5
    assert config.download_retry.retries == 2


YAML_AVAILABLE = MemoConfig.load.__globals__.get("yaml") is not None


@pytest.mark.skipif(not YAML_AVAILABLE, reason="PyYAML not installed")
def test_config_yaml_loading(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
memo_page_url: https://example.com
notification:
  enabled: false
approval:
  method: none
        """.strip(),
        encoding="utf-8",
    )

    config = MemoConfig.load(config_file)
    assert config.memo_page_url == "https://example.com"


def test_config_patterns_override(tmp_path: Path) -> None:
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
{
  "memo_page_url": "https://example.com",
  "patterns": {
    "pay_item_regex": "(?P<item>ABC)"
  }
}
        """.strip(),
        encoding="utf-8",
    )

    config = MemoConfig.load(config_file)
    assert config.patterns.pay_item_regex == "(?P<item>ABC)"
