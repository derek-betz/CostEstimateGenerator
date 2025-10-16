"""Configuration helpers for memo automation."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

try:  # Optional dependency; JSON config is supported without PyYAML.
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - executed when PyYAML is unavailable
    yaml = None

DEFAULT_CONFIG_PATH = Path("references/memos/config.json")
LOGGER = logging.getLogger(__name__)


@dataclass
class SMTPConfig:
    host: str
    port: int = 587
    use_tls: bool = True
    username: str | None = None
    password: str | None = None


@dataclass
class MailboxConfig:
    host: str
    port: int = 993
    username: str | None = None
    password: str | None = None
    folder: str = "INBOX"


@dataclass
class ApprovalConfig:
    method: str = "email-reply"
    phrase_format: str = "Approved {memo_id}"
    mailbox: Optional[MailboxConfig] = None


@dataclass
class NotificationConfig:
    enabled: bool = False
    sender: str | None = None
    recipients: List[str] = field(default_factory=list)
    smtp: Optional[SMTPConfig] = None


@dataclass
class AIConfig:
    enabled: bool = False
    provider: str = "openai"
    api_key_path: Path | None = Path(r"C:\AI\CostEstimateGenerator\API_KEY\API_KEY.txt")
    api_key_env: str = "OPENAI_API_KEY"
    model: str = "gpt-4o-mini"
    system_prompt: Optional[str] = None
    summary_template: Optional[str] = None
    max_context_chars: int = 15000

    def resolve_api_key(self) -> Optional[str]:
        """Return the API key from the environment or configured file."""
        if self.api_key_env and self.api_key_env in os.environ:
            token = os.environ[self.api_key_env].strip()
            if token:
                return token
        if self.api_key_path:
            try:
                content = Path(self.api_key_path).expanduser().read_text(encoding="utf-8")
            except FileNotFoundError:
                return None
            except OSError:
                LOGGER.debug("Unable to read AI API key from %s", self.api_key_path)
                return None
            token = content.strip()
            return token or None
        return None


@dataclass
class MemoConfig:
    memo_page_url: str
    polling_interval_days: int = 30
    storage_root: Path = Path("references/memos")
    raw_directory: Path = Path("references/memos/raw")
    processed_directory: Path = Path("references/memos/processed")
    digests_directory: Path = Path("references/memos/digests")
    state_file: Path = Path("references/memos/state.json")
    index_file: Path = Path("references/memos/index.json")
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    approval: ApprovalConfig = field(default_factory=ApprovalConfig)
    ai: AIConfig = field(default_factory=AIConfig)

    @classmethod
    def load(cls, path: Path | None = None) -> "MemoConfig":
        """Load configuration from YAML/JSON file."""
        config_path = path or DEFAULT_CONFIG_PATH
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with config_path.open("r", encoding="utf-8") as f:
            suffix = config_path.suffix.lower()
            if suffix in {".yaml", ".yml"}:
                if not yaml:
                    raise ImportError(
                        "PyYAML is required to read YAML configs; install it or switch to JSON"
                    )
                raw = yaml.safe_load(f)
            else:
                raw = json.load(f)

        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict) -> "MemoConfig":
        notification = raw.get("notification") or {}
        approval = raw.get("approval") or {}
        ai = raw.get("ai") or {}

        smtp_cfg = None
        if notification.get("smtp"):
            smtp_cfg = SMTPConfig(**_with_env_overrides(notification["smtp"], prefix="SMTP_"))
        elif any(key in os.environ for key in ["SMTP_HOST", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_PORT"]):
            smtp_cfg = SMTPConfig(
                host=os.environ.get("SMTP_HOST", ""),
                port=int(os.environ.get("SMTP_PORT", 587)),
                use_tls=os.environ.get("SMTP_USE_TLS", "true").lower() not in {"0", "false", "no"},
                username=os.environ.get("SMTP_USERNAME"),
                password=os.environ.get("SMTP_PASSWORD"),
            )

        mailbox_cfg = None
        if approval.get("mailbox"):
            mailbox_cfg = MailboxConfig(**_with_env_overrides(approval["mailbox"], prefix="IMAP_"))
        elif any(key in os.environ for key in ["IMAP_HOST", "IMAP_USERNAME", "IMAP_PASSWORD", "IMAP_PORT"]):
            mailbox_cfg = MailboxConfig(
                host=os.environ.get("IMAP_HOST", ""),
                port=int(os.environ.get("IMAP_PORT", 993)),
                username=os.environ.get("IMAP_USERNAME"),
                password=os.environ.get("IMAP_PASSWORD"),
                folder=os.environ.get("IMAP_FOLDER", "INBOX"),
            )

        notification_cfg = NotificationConfig(
            enabled=notification.get("enabled", False),
            sender=notification.get("sender") or os.environ.get("SMTP_SENDER"),
            recipients=list(notification.get("recipients", []) or _env_recipients()),
            smtp=smtp_cfg,
        )

        approval_cfg = ApprovalConfig(
            method=approval.get("method", "email-reply"),
            phrase_format=approval.get("phrase_format", "Approved {memo_id}"),
            mailbox=mailbox_cfg,
        )

        default_ai = AIConfig()
        api_key_path = (
            Path(ai["api_key_path"]).expanduser()
            if ai.get("api_key_path")
            else default_ai.api_key_path
        )
        ai_cfg = AIConfig(
            enabled=bool(ai.get("enabled", False)),
            provider=ai.get("provider", "openai"),
            api_key_path=api_key_path,
            api_key_env=ai.get("api_key_env", "OPENAI_API_KEY"),
            model=ai.get("model", "gpt-4o-mini"),
            system_prompt=ai.get("system_prompt"),
            summary_template=ai.get("summary_template"),
            max_context_chars=int(ai.get("max_context_chars", 15000)),
        )

        return cls(
            memo_page_url=raw["memo_page_url"],
            polling_interval_days=int(raw.get("polling_interval_days", 30)),
            storage_root=Path(raw.get("storage_root", "references/memos")),
            raw_directory=Path(raw.get("raw_directory", "references/memos/raw")),
            processed_directory=Path(raw.get("processed_directory", "references/memos/processed")),
            digests_directory=Path(raw.get("digests_directory", "references/memos/digests")),
            state_file=Path(raw.get("state_file", "references/memos/state.json")),
            index_file=Path(raw.get("index_file", "references/memos/index.json")),
            notification=notification_cfg,
            approval=approval_cfg,
            ai=ai_cfg,
        )

    def ensure_directories(self) -> None:
        for directory in [
            self.storage_root,
            self.raw_directory,
            self.processed_directory,
            self.digests_directory,
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.index_file.parent.mkdir(parents=True, exist_ok=True)

    @property
    def recipients(self) -> Iterable[str]:
        return self.notification.recipients


__all__ = [
    "MemoConfig",
    "NotificationConfig",
    "ApprovalConfig",
    "SMTPConfig",
    "MailboxConfig",
    "AIConfig",
]


def _with_env_overrides(data: dict, prefix: str) -> dict:
    """Override dictionary values with environment variables."""
    result = dict(data)
    for key in ["host", "port", "username", "password", "use_tls", "folder", "sender"]:
        env_key = f"{prefix}{key.upper()}"
        if env_key in os.environ:
            value = os.environ[env_key]
            if key == "port":
                result[key] = int(value)
            elif key == "use_tls":
                result[key] = value.lower() not in {"0", "false", "no"}
            else:
                result[key] = value
    return result


def _env_recipients() -> List[str]:
    raw = os.environ.get("SMTP_RECIPIENTS")
    if not raw:
        return []
    return [email.strip() for email in raw.split(",") if email.strip()]
