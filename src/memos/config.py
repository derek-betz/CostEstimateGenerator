"""Configuration helpers for memo automation."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

try:  # Optional dependency; JSON config is supported without PyYAML.
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - executed when PyYAML is unavailable
    yaml = None

DEFAULT_CONFIG_PATH = Path("references/memos/config.json")


@dataclass
class RetryPolicy:
    """Retry/backoff policy for network operations."""

    timeout_seconds: float = 30.0
    retries: int = 0
    backoff_factor: float = 0.0
    circuit_breaker_failures: int = 3


@dataclass
class PatternConfig:
    """Configurable parsing patterns for memo extraction."""

    pay_item_regex: str = r"\b(?P<item>\d{4,6})\b"
    spec_section_regex: str = r"Section\s+(?P<section>\d{3})"
    dollar_regex: str = r"\$\s?(?P<amount>[0-9,.]+)"
    keywords: List[str] = field(
        default_factory=lambda: [
            "pay item",
            "unit price",
            "specification",
            "standard drawing",
            "change",
            "update",
        ]
    )
    pay_item_limit: int = 50
    pay_item_frequency_guard: int = 200


@dataclass
class SMTPConfig:
    host: str
    port: int = 587
    use_tls: bool = True
    username: str | None = None
    password: str | None = None
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(timeout_seconds=30.0))


@dataclass
class MailboxConfig:
    host: str
    port: int = 993
    username: str | None = None
    password: str | None = None
    folder: str = "INBOX"
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(timeout_seconds=30.0))


@dataclass
class ApprovalConfig:
    method: str = "email-reply"
    phrase_format: str = "Approved {memo_id}"
    mailbox: Optional[MailboxConfig] = None


@dataclass
class NotificationConfig:
    enabled: bool = False
    enabled_on_failure: bool = False
    sender: str | None = None
    recipients: List[str] = field(default_factory=list)
    smtp: Optional[SMTPConfig] = None


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
    http_retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(timeout_seconds=30.0))
    download_retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(timeout_seconds=60.0))
    patterns: PatternConfig = field(default_factory=PatternConfig)

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

        default_patterns = PatternConfig()

        http_retry = _load_retry_policy(
            raw,
            section_key="http",
            timeout_key="http_timeout_seconds",
            retries_key="http_retries",
            backoff_key="http_backoff_factor",
            default_timeout=30.0,
            env_prefix="MEMO_HTTP_",
        )
        download_retry = _load_retry_policy(
            raw,
            section_key="download",
            timeout_key="download_timeout_seconds",
            retries_key="download_retries",
            backoff_key="download_backoff_factor",
            default_timeout=60.0,
            env_prefix="MEMO_DOWNLOAD_",
        )

        smtp_cfg = None
        if notification.get("smtp"):
            smtp_cfg = _load_smtp(notification["smtp"])
        elif any(
            key in os.environ
            for key in [
                "SMTP_HOST",
                "SMTP_USERNAME",
                "SMTP_PASSWORD",
                "SMTP_PORT",
            ]
        ):
            smtp_cfg = _load_smtp({})

        mailbox_cfg = None
        if approval.get("mailbox"):
            mailbox_cfg = _load_mailbox(approval["mailbox"])
        elif any(
            key in os.environ
            for key in ["IMAP_HOST", "IMAP_USERNAME", "IMAP_PASSWORD", "IMAP_PORT"]
        ):
            mailbox_cfg = _load_mailbox({})

        notification_cfg = NotificationConfig(
            enabled=_coerce_bool(notification.get("enabled", False)),
            enabled_on_failure=_coerce_bool(notification.get("enabled_on_failure", False))
            or _coerce_bool(os.environ.get("SMTP_NOTIFY_ON_FAILURE", "false")),
            sender=notification.get("sender") or os.environ.get("SMTP_SENDER"),
            recipients=list(notification.get("recipients", []) or _env_recipients()),
            smtp=smtp_cfg,
        )

        approval_cfg = ApprovalConfig(
            method=approval.get("method", "email-reply"),
            phrase_format=approval.get("phrase_format", "Approved {memo_id}"),
            mailbox=mailbox_cfg,
        )

        patterns_raw = raw.get("patterns") or {}
        patterns = PatternConfig(
            pay_item_regex=patterns_raw.get("pay_item_regex", default_patterns.pay_item_regex),
            spec_section_regex=patterns_raw.get(
                "spec_section_regex", default_patterns.spec_section_regex
            ),
            dollar_regex=patterns_raw.get("dollar_regex", default_patterns.dollar_regex),
            keywords=list(patterns_raw.get("keywords", default_patterns.keywords)),
            pay_item_limit=int(patterns_raw.get("pay_item_limit", default_patterns.pay_item_limit)),
            pay_item_frequency_guard=int(
                patterns_raw.get(
                    "pay_item_frequency_guard", default_patterns.pay_item_frequency_guard
                )
            ),
        )

        return cls(
            memo_page_url=raw["memo_page_url"],
            polling_interval_days=int(raw.get("polling_interval_days", 30)),
            storage_root=_to_path(raw.get("storage_root", "references/memos")),
            raw_directory=_to_path(raw.get("raw_directory", "references/memos/raw")),
            processed_directory=_to_path(
                raw.get("processed_directory", "references/memos/processed")
            ),
            digests_directory=_to_path(raw.get("digests_directory", "references/memos/digests")),
            state_file=_to_path(raw.get("state_file", "references/memos/state.json")),
            index_file=_to_path(raw.get("index_file", "references/memos/index.json")),
            notification=notification_cfg,
            approval=approval_cfg,
            http_retry=http_retry,
            download_retry=download_retry,
            patterns=patterns,
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
    "RetryPolicy",
    "PatternConfig",
]


def _load_retry_policy(
    raw: dict,
    *,
    section_key: str,
    timeout_key: str,
    retries_key: str,
    backoff_key: str,
    default_timeout: float,
    env_prefix: str,
) -> RetryPolicy:
    section = dict(raw.get(section_key, {})) if raw.get(section_key) else {}
    if isinstance(section, dict) and "retry" in section and isinstance(section["retry"], dict):
        retry_section = section["retry"]
        base_section = {k: v for k, v in section.items() if k != "retry"}
        # allow overriding fallback fields without mutating original dict
        section = {**base_section, **retry_section}

    if timeout_key in raw:
        section.setdefault("timeout_seconds", raw[timeout_key])
    if retries_key in raw:
        section.setdefault("retries", raw[retries_key])
    if backoff_key in raw:
        section.setdefault("backoff_factor", raw[backoff_key])
    if "circuit_breaker_failures" not in section and "circuit_breaker_failures" in raw:
        section["circuit_breaker_failures"] = raw["circuit_breaker_failures"]

    mapping = {
        "timeout_seconds": default_timeout,
        "retries": 0,
        "backoff_factor": 0.0,
        "circuit_breaker_failures": 3,
    }
    mapping.update(section)

    env_variants = {
        "timeout_seconds": [
            f"{env_prefix}TIMEOUT_SECONDS",
            f"{env_prefix}TIMEOUT",
        ],
        "retries": [f"{env_prefix}RETRIES"],
        "backoff_factor": [
            f"{env_prefix}BACKOFF_FACTOR",
            f"{env_prefix}BACKOFF",
        ],
        "circuit_breaker_failures": [
            f"{env_prefix}CIRCUIT_BREAKER_FAILURES",
            f"{env_prefix}CIRCUIT_BREAKER",
        ],
    }

    for key, candidates in env_variants.items():
        for env_key in candidates:
            if env_key in os.environ:
                value = os.environ[env_key]
                if key in {"timeout_seconds", "backoff_factor"}:
                    mapping[key] = float(value)
                else:
                    mapping[key] = int(value)
                break

    return RetryPolicy(
        timeout_seconds=float(mapping["timeout_seconds"]),
        retries=int(mapping["retries"]),
        backoff_factor=float(mapping["backoff_factor"]),
        circuit_breaker_failures=max(1, int(mapping["circuit_breaker_failures"])),
    )


def _load_smtp(raw: dict) -> SMTPConfig:
    data = _with_env_overrides(raw, prefix="SMTP_")
    retry_policy = _load_retry_policy(
        {"smtp": data},
        section_key="smtp",
        timeout_key="smtp_timeout_seconds",
        retries_key="smtp_retries",
        backoff_key="smtp_backoff_factor",
        default_timeout=30.0,
        env_prefix="SMTP_",
    )
    return SMTPConfig(
        host=data.get("host", os.environ.get("SMTP_HOST", "")),
        port=int(data.get("port", os.environ.get("SMTP_PORT", 587))),
        use_tls=_coerce_bool(data.get("use_tls", os.environ.get("SMTP_USE_TLS", "true"))),
        username=data.get("username") or os.environ.get("SMTP_USERNAME"),
        password=data.get("password") or os.environ.get("SMTP_PASSWORD"),
        retry=retry_policy,
    )


def _load_mailbox(raw: dict) -> MailboxConfig:
    data = _with_env_overrides(raw, prefix="IMAP_")
    retry_policy = _load_retry_policy(
        {"imap": data},
        section_key="imap",
        timeout_key="imap_timeout_seconds",
        retries_key="imap_retries",
        backoff_key="imap_backoff_factor",
        default_timeout=30.0,
        env_prefix="IMAP_",
    )
    return MailboxConfig(
        host=data.get("host", os.environ.get("IMAP_HOST", "")),
        port=int(data.get("port", os.environ.get("IMAP_PORT", 993))),
        username=data.get("username") or os.environ.get("IMAP_USERNAME"),
        password=data.get("password") or os.environ.get("IMAP_PASSWORD"),
        folder=data.get("folder", os.environ.get("IMAP_FOLDER", "INBOX")),
        retry=retry_policy,
    )


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


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", ""}
    return bool(value)


def _to_path(value: str | Path) -> Path:
    return Path(value).expanduser()
