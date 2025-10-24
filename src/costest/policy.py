from __future__ import annotations

import json
import os
from pathlib import Path


def apply_policy_defaults(path: Path) -> None:
    """Set default environment variables from a policy JSON if not already set.

    This is intentionally conservative: only missing env vars are set.
    """
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    env_defaults = payload.get("env_defaults") or {}
    for key, value in env_defaults.items():
        if str(os.environ.get(key, "")).strip() == "":
            os.environ[key] = str(value)
