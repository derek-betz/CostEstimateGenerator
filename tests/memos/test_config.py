from __future__ import annotations

import json
from pathlib import Path

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
    monkeypatch.setenv("IMAP_HOST", "imap.test")
    monkeypatch.setenv("IMAP_USERNAME", "imap-user")
    monkeypatch.setenv("IMAP_PASSWORD", "imap-pass")

    config = MemoConfig.load(config_file)

    assert config.notification.smtp is not None
    assert config.notification.smtp.host == "smtp.test"
    assert config.notification.sender == "sender@example.com"
    assert config.recipients == ["one@example.com", "two@example.com"]
    assert config.approval.mailbox is not None
    assert config.approval.mailbox.host == "imap.test"


def test_ai_config_prefers_env_then_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    key_file = tmp_path / "API_KEY.txt"
    key_file.write_text("file-secret", encoding="utf-8")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "memo_page_url": "https://example.com",
                "ai": {
                    "enabled": True,
                    "api_key_path": str(key_file),
                    "model": "test-model",
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("OPENAI_API_KEY", "env-secret")
    config = MemoConfig.load(config_file)
    assert config.ai.enabled is True
    assert config.ai.resolve_api_key() == "env-secret"

    monkeypatch.delenv("OPENAI_API_KEY")
    assert config.ai.resolve_api_key() == "file-secret"
