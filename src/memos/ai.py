"""AI-assisted review helpers for memo processing."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from .config import MemoConfig
from .parser import ParsedMemo

try:  # pragma: no cover - optional dependency imported at runtime
    from openai import OpenAI
except ImportError:  # pragma: no cover - when openai package is unavailable
    OpenAI = None  # type: ignore

LOGGER = logging.getLogger(__name__)

DEFAULT_SYSTEM_PROMPT = (
    "You are an experienced transportation cost estimator reviewing INDOT design "
    "memos. Identify actionable updates, pricing impacts, and verify extracted "
    "highlights. Call out corrections if automated parsing missed something."
)

DEFAULT_USER_PROMPT = (
    "Memo ID: {memo_id}\n"
    "Detected highlights: {highlights}\n\n"
    "You are given plain-text extracted from the memo. Provide a concise summary "
    "emphasizing specification changes, pay-item impacts, and any guidance for the "
    "cost estimate review team. Flag likely errors or clarifications. Use bullet "
    "lists where helpful. Memo text (truncated={truncated}):\n"\
    "---\n{memo_text}\n---"
)


@dataclass
class AIReview:
    memo_id: str
    analysis_path: Path
    summary: str
    truncated_context: bool = False


class MemoAIReviewer:
    """Coordinates AI calls to enrich memo digests."""

    def __init__(self, config: MemoConfig) -> None:
        self.config = config

    def review(self, memos: Iterable[ParsedMemo]) -> List[AIReview]:
        cfg = self.config.ai
        if not cfg.enabled:
            LOGGER.debug("AI review disabled in configuration")
            return []
        if OpenAI is None:
            LOGGER.warning("openai package is not installed; skipping AI review")
            return []

        api_key = cfg.resolve_api_key()
        if not api_key:
            LOGGER.warning(
                "AI review enabled but API key unavailable; expected at %s or via %s",
                cfg.api_key_path,
                cfg.api_key_env,
            )
            return []

        try:
            client = OpenAI(api_key=api_key)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.error("Failed to initialise OpenAI client: %s", exc)
            return []

        reviews: List[AIReview] = []
        for memo in memos:
            text = self._load_text(memo)
            if not text.strip():
                LOGGER.debug("Skipping AI review for %s because extracted text is empty", memo.memo_id)
                continue

            truncated = False
            if len(text) > cfg.max_context_chars:
                truncated = True
                text = text[: cfg.max_context_chars]

            prompt = self._build_user_prompt(memo, text, truncated)
            try:
                response = client.responses.create(
                    model=cfg.model,
                    input=[
                        {"role": "system", "content": cfg.system_prompt or DEFAULT_SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                )
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error("AI analysis failed for %s: %s", memo.memo_id, exc)
                continue

            summary = self._extract_response_text(response)
            if not summary:
                LOGGER.warning("AI response for %s did not contain text output", memo.memo_id)
                continue

            analysis_path = memo.digest_path.with_name(f"{memo.digest_path.stem}-ai.md")
            analysis_path.write_text(
                self._render_analysis_document(memo, summary, truncated),
                encoding="utf-8",
            )
            self._augment_digest(memo, summary, truncated)
            self._augment_summary_payload(memo, summary, analysis_path, truncated)
            memo.ai_digest_path = analysis_path
            reviews.append(AIReview(memo_id=memo.memo_id, analysis_path=analysis_path, summary=summary, truncated_context=truncated))

        return reviews

    def _load_text(self, memo: ParsedMemo) -> str:
        if memo.text_path.exists():
            try:
                return memo.text_path.read_text(encoding="utf-8")
            except OSError:  # pragma: no cover - unexpected IO issue
                LOGGER.warning("Unable to read text extraction for %s", memo.memo_id)
        return ""

    def _build_user_prompt(self, memo: ParsedMemo, text: str, truncated: bool) -> str:
        highlights_serialised = json.dumps(memo.highlights, indent=2, sort_keys=True)
        template = self.config.ai.summary_template or DEFAULT_USER_PROMPT
        return template.format(
            memo_id=memo.memo_id,
            highlights=highlights_serialised,
            memo_text=text,
            truncated=str(truncated).lower(),
        )

    def _extract_response_text(self, response: object) -> Optional[str]:
        text: Optional[str] = None
        if hasattr(response, "output_text"):
            text = getattr(response, "output_text")
        elif hasattr(response, "choices"):
            choices = getattr(response, "choices")
            if choices:
                choice = choices[0]
                if isinstance(choice, dict):
                    text = choice.get("message", {}).get("content")
                else:
                    message = getattr(choice, "message", None)
                    if message and isinstance(message, dict):
                        text = message.get("content")
                    elif message and hasattr(message, "content"):
                        text = message.content
        if isinstance(text, list):
            # Some models return a list of content parts
            text = "\n".join(part.get("text", "") if isinstance(part, dict) else str(part) for part in text)
        return text.strip() if text else None

    def _render_analysis_document(self, memo: ParsedMemo, summary: str, truncated: bool) -> str:
        lines = ["# AI Review Summary", ""]
        lines.append(f"**Memo ID:** {memo.memo_id}")
        lines.append(f"**Model:** {self.config.ai.model}")
        if truncated:
            lines.append("_Context truncated to fit configured limits._")
        lines.append("")
        lines.append(summary.strip())
        return "\n".join(lines).strip() + "\n"

    def _augment_digest(self, memo: ParsedMemo, summary: str, truncated: bool) -> None:
        try:
            digest = memo.digest_path.read_text(encoding="utf-8")
        except OSError:  # pragma: no cover - should not happen
            LOGGER.warning("Unable to read digest for %s; AI notes not appended", memo.memo_id)
            return
        lines = [digest.rstrip(), "", "## AI Review Notes", ""]
        if truncated:
            lines.append("_Memo text truncated for AI context._")
        lines.append(summary.strip())
        memo.digest_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    def _augment_summary_payload(
        self, memo: ParsedMemo, summary: str, analysis_path: Path, truncated: bool
    ) -> None:
        try:
            data = json.loads(memo.summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):  # pragma: no cover - resilience
            LOGGER.warning("Unable to augment summary JSON for %s", memo.memo_id)
            return
        data["ai_analysis"] = {
            "model": self.config.ai.model,
            "analysis_path": str(analysis_path),
            "truncated_context": truncated,
            "notes": summary.strip(),
        }
        memo.summary_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


__all__ = ["MemoAIReviewer", "AIReview"]
