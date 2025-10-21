"""Validation and correction utilities for memo digests.

This module cross-checks generated Markdown digests and JSON summaries
against the source PDF text. It can recompute highlights deterministically
and optionally call an AI model to suggest corrections. When applying AI
suggestions, each proposed change is verified against the PDF text to avoid
introducing hallucinations.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore

from .parser import MemoParser
from .state import MemoRecord, MemoState
from .config import MemoConfig

LOGGER = logging.getLogger(__name__)


@dataclass
class ValidationDiff:
    memo_id: str
    changed: bool
    fields_changed: List[str]
    before: Dict[str, object]
    after: Dict[str, object]
    notes: List[str]
    pay_items_added: List[str] = None  # type: ignore[assignment]
    pay_items_removed: List[str] = None  # type: ignore[assignment]


class MemoValidator:
    """Cross-checks digests/processed JSON against original PDFs."""

    def __init__(self, config: MemoConfig, state: MemoState) -> None:
        self.config = config
        self.state = state
        self.parser = MemoParser(config, state)

    def _paths_for(self, memo_id: str, record: MemoRecord) -> Tuple[Path, Path, Path]:
        """Return (pdf_path, summary_json_path, digest_md_path)."""
        # Prefer the recorded filename; fallback to memo_id.pdf
        target_name = record.filename if record.filename.lower().endswith(".pdf") else f"{memo_id}.pdf"
        pdf_path = self.config.raw_directory / target_name
        if not pdf_path.exists():
            # try best-effort locate by memo_id substring
            matches = list(self.config.raw_directory.glob(f"*{memo_id}*.pdf"))
            if matches:
                pdf_path = matches[0]
        summary_path = self.config.processed_directory / f"{memo_id}.json"
        digest_path = self.config.digests_directory / f"{memo_id}.md"
        return pdf_path, summary_path, digest_path

    def _recompute_payload(self, memo_id: str, pdf_path: Path, record: MemoRecord) -> Dict[str, object]:
        text = self.parser._extract_text(pdf_path)
        highlights = self.parser._extract_highlights(text)
        metadata = self.parser._build_metadata(record, pdf_path, text, highlights)
        snippets = self.parser._collect_snippets(text, highlights)
        return {"metadata": metadata, "highlights": highlights, "snippets": snippets}

    @staticmethod
    def _diff_payload(old: Dict[str, object], new: Dict[str, object]) -> Tuple[bool, List[str]]:
        changed_fields: List[str] = []
        for key in ("metadata", "highlights", "snippets"):
            if json.dumps(old.get(key, None), sort_keys=True) != json.dumps(new.get(key, None), sort_keys=True):
                changed_fields.append(key)
        return (len(changed_fields) > 0), changed_fields

    def validate_and_correct(
        self,
        *,
        use_ai: bool = False,
        model: str = "gpt-4o-mini",
        dry_run: bool = True,
        limit: Optional[int] = None,
    ) -> List[ValidationDiff]:
        """Validate all known memos and optionally correct outputs.

        Returns a list of diffs with before/after payloads and notes.
        """
        results: List[ValidationDiff] = []
        processed = 0
        for memo_id, record in self.state.memos.items():
            pdf_path, summary_path, digest_path = self._paths_for(memo_id, record)
            if not pdf_path.exists():
                LOGGER.warning("PDF missing for %s; skipping", memo_id)
                continue
            if not summary_path.exists() or not digest_path.exists():
                LOGGER.debug("Artifacts missing for %s; will regenerate if needed", memo_id)

            # Load existing payload if present
            old_payload: Dict[str, object] = {}
            if summary_path.exists():
                try:
                    old_payload = json.loads(summary_path.read_text(encoding="utf-8"))
                except Exception as exc:  # pragma: no cover - defensive
                    LOGGER.error("Failed to read %s: %s", summary_path, exc)
                    old_payload = {}

            # Deterministic recompute
            new_payload = self._recompute_payload(memo_id, pdf_path, record)
            changed, fields = self._diff_payload(old_payload, new_payload)
            notes: List[str] = []
            if changed:
                notes.append("Deterministic recompute differs from stored summary")

            # Optional AI pass for additional discrepancies
            if use_ai:
                ai_notes, ai_payload = self._ai_review(pdf_path, digest_path, new_payload, model)
                if ai_payload:
                    # merge AI-proposed highlight changes after verifying presence in text
                    verified = self._verify_ai_highlights(pdf_path, ai_payload.get("highlights", {}))
                    if verified:
                        # merge with new_payload
                        merged = dict(new_payload)
                        merged["highlights"] = {
                            **new_payload.get("highlights", {}),
                            **verified,
                        }
                        new_payload = merged
                        changed = True
                        if "highlights" not in fields:
                            fields.append("highlights")
                notes.extend(ai_notes)

            # Write changes if required
            if changed and not dry_run:
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                digest_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(new_payload, indent=2), encoding="utf-8")
                digest_content = self.parser._render_digest(new_payload)
                digest_path.write_text(digest_content, encoding="utf-8")
                notes.append(f"Updated {summary_path.name} and {digest_path.name}")

            # Compute pay item delta
            old_items = []
            if isinstance(old_payload.get("highlights"), dict):
                old_items = list(old_payload.get("highlights", {}).get("pay_items", []) or [])
            new_items = []
            if isinstance(new_payload.get("highlights"), dict):
                new_items = list(new_payload.get("highlights", {}).get("pay_items", []) or [])
            added = sorted(set(new_items) - set(old_items))
            removed = sorted(set(old_items) - set(new_items))

            results.append(
                ValidationDiff(
                    memo_id=memo_id,
                    changed=changed,
                    fields_changed=fields,
                    before=old_payload,
                    after=new_payload,
                    notes=notes,
                    pay_items_added=added,
                    pay_items_removed=removed,
                )
            )

            processed += 1
            if limit and processed >= limit:
                break

        return results

    def _ai_review(
        self,
        pdf_path: Path,
        digest_path: Path,
        payload: Dict[str, object],
        model: str,
    ) -> Tuple[List[str], Dict[str, object]]:
        """Ask an AI model to check digest vs PDF and propose highlight fixes.

        Returns notes and an optional partial payload with proposed updates.
        """
        notes: List[str] = []
        if OpenAI is None:
            notes.append("openai package not available; skipping AI review")
            return notes, {}

        # --- Load/OpenAI or Azure OpenAI configuration ---
        def _parse_kv_lines(text: str) -> Dict[str, str]:
            cfg: Dict[str, str] = {}
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = v.strip()
            return cfg

        # Pull from env first
        openai_key = os.getenv("OPENAI_API_KEY", "").strip()
        azure_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
        azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "").strip() or "2024-05-01-preview"
        azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip() or os.getenv("OPENAI_DEPLOYMENT_NAME", "").strip()

        # If missing, try to populate from API_KEY/API_KEY.txt
        if not (openai_key or azure_key):
            fallback = Path("API_KEY") / "API_KEY.txt"
            if fallback.exists():
                try:
                    raw = fallback.read_text(encoding="utf-8").strip()
                    cfg = _parse_kv_lines(raw)
                    # Map recognized keys into env if present
                    for k in ("OPENAI_API_KEY", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_VERSION", "AZURE_OPENAI_DEPLOYMENT", "OPENAI_DEPLOYMENT_NAME"):
                        if k in cfg and cfg[k]:
                            os.environ[k] = cfg[k]
                    # Re-read after env update
                    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
                    azure_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
                    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
                    azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "").strip() or azure_api_version
                    azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip() or os.getenv("OPENAI_DEPLOYMENT_NAME", "").strip() or azure_deployment

                    # Handle single-line content cases
                    if not (openai_key or azure_key):
                        if raw.lower().startswith("sk-"):
                            openai_key = raw
                            os.environ["OPENAI_API_KEY"] = openai_key
                        elif "openai.azure.com" in raw:
                            azure_endpoint = raw
                            os.environ["AZURE_OPENAI_ENDPOINT"] = azure_endpoint
                except Exception:  # pragma: no cover
                    pass

        # If env var exists but seems malformed, try to extract key after '='
        if openai_key and not openai_key.lower().startswith("sk-"):
            if "=" in openai_key:
                maybe = openai_key.split("=", 1)[-1].strip().strip('"').strip("'")
                if maybe.lower().startswith("sk-"):
                    openai_key = maybe
                    os.environ["OPENAI_API_KEY"] = openai_key
                    notes.append("Recovered OpenAI key from malformed env value")
        # If still missing/invalid and we have a fallback file, try again
        if (not openai_key or not openai_key.lower().startswith("sk-")) and (Path("API_KEY") / "API_KEY.txt").exists():
            try:
                raw = (Path("API_KEY") / "API_KEY.txt").read_text(encoding="utf-8").strip()
                if "OPENAI_API_KEY" in raw and "=" in raw:
                    maybe = raw.split("=", 1)[-1].strip().strip('"').strip("'")
                else:
                    maybe = raw
                if maybe.lower().startswith("sk-"):
                    openai_key = maybe
                    os.environ["OPENAI_API_KEY"] = openai_key
                    notes.append("Loaded OpenAI key from API_KEY/API_KEY.txt")
            except Exception:  # pragma: no cover
                pass

        # Determine provider mode
        use_azure = bool(azure_key and azure_endpoint)
        client = None
        try:
            if use_azure:
                # For Azure OpenAI, model should be the deployment name
                deployment_name = azure_deployment or model
                if not deployment_name:
                    notes.append("AZURE deployment name missing; set AZURE_OPENAI_DEPLOYMENT or pass --model")
                    return notes, {}
                client = OpenAI(
                    api_key=azure_key,
                    base_url=f"{azure_endpoint.rstrip('/')}/openai",
                    default_query_params={"api-version": azure_api_version},
                )
                # Override model with deployment
                model = deployment_name
                notes.append("Using Azure OpenAI for AI review")
            else:
                # Validate OpenAI key format if present
                if not openai_key:
                    notes.append("OPENAI_API_KEY missing; skipping AI review")
                    return notes, {}
                if not openai_key.lower().startswith("sk-"):
                    notes.append("OPENAI_API_KEY format invalid (expected 'sk-...'); skipping AI review")
                    return notes, {}
                client = OpenAI(api_key=openai_key)
        except Exception as exc:  # pragma: no cover - defensive
            notes.append(f"Failed to init OpenAI client: {exc}")
            return notes, {}

        # Extract a bounded subset of PDF text for AI to avoid timeouts on complex PDFs
        try:
            pdf_text = self._extract_text_for_ai(pdf_path)
        except Exception as exc:  # pragma: no cover
            notes.append(f"Failed to extract PDF text: {exc}")
            return notes, {}

        digest_text = ""
        if digest_path.exists():
            try:
                digest_text = digest_path.read_text(encoding="utf-8")
            except Exception:  # pragma: no cover
                digest_text = ""

        # Trim overly long inputs by focusing on the first N characters
        max_chars = 120_000  # safeguard; model-dependent context
        if len(pdf_text) > max_chars:
            pdf_text = pdf_text[:max_chars]
            notes.append("PDF text truncated for AI review")

        system = (
            "You are a careful technical analyst. Compare a human-written digest to the source INDOT design memo text. "
            "Identify any inaccuracies (missing pay items, spec sections, dollar amounts, wrong facts) and propose fixes. "
            "Important constraints: Do NOT invent or infer pay items. Only propose pay_items if explicit INDOT pay item codes "
            "(three digits, hyphen, five or six digits; e.g., 706-08496) appear in the source. Many memos are narrative; when no "
            "codes are present, return pay_items: []. Only propose spec_sections if the memo explicitly cites them (e.g., 'Section 706'). "
            "Only propose dollar_amounts that are written with a $ sign in the memo. "
            "Return a STRICT JSON object with keys: 'issues' (list of strings), and 'highlights' (object) where keys may include "
            "'pay_items', 'spec_sections', 'dollar_amounts', 'keywords_present' with corrected lists. If no changes, return empty lists."
        )
        user = (
            "Source memo text (excerpt):\n\n" + pdf_text +
            "\n\nExisting digest:\n\n" + digest_text
        )

        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=0,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                response_format={"type": "json_object"},
            )
            content = resp.choices[0].message.content or "{}"
            proposed = json.loads(content)
        except Exception as exc:  # pragma: no cover - network/parsing
            notes.append(f"AI review failed: {exc}")
            return notes, {}

        issues = proposed.get("issues") or []
        if issues:
            notes.append(f"AI flagged {len(issues)} issue(s)")
        return notes, {"highlights": proposed.get("highlights") or {}}

    def _extract_text_for_ai(self, pdf_path: Path, *, max_chars: int = 120_000, page_limit: int = 30) -> str:
        """Extract up to max_chars of text from up to page_limit pages, optimized for AI checks.

        Falls back to the full parser extraction if PyPDF2 fast-path fails.
        """
        try:
            # Lazy import to avoid hard dependency
            from PyPDF2 import PdfReader  # type: ignore
        except Exception:
            # Fallback to existing full extraction
            return self.parser._extract_text(pdf_path)

        text_parts: List[str] = []
        total = 0
        try:
            reader = PdfReader(str(pdf_path))
            for i, page in enumerate(reader.pages):
                if page_limit and i >= page_limit:
                    break
                try:
                    chunk = page.extract_text() or ""
                except Exception:
                    chunk = ""
                if not chunk:
                    continue
                text_parts.append(chunk)
                total += len(chunk)
                if max_chars and total >= max_chars:
                    break
            txt = "".join(text_parts)
            if max_chars and len(txt) > max_chars:
                return txt[:max_chars]
            return txt
        except Exception:
            # Fallback to full extraction on any error
            return self.parser._extract_text(pdf_path)

    def _verify_ai_highlights(self, pdf_path: Path, highlights: Dict[str, object]) -> Dict[str, List[str]]:
        """Ensure AI-proposed highlights exist in the PDF text; discard otherwise."""
        try:
            text = self.parser._extract_text(pdf_path)
        except Exception:  # pragma: no cover
            return {}

        verified: Dict[str, List[str]] = {}

        def present(value: str) -> bool:
            return value and (value.lower() in text.lower())

        for key in ("pay_items", "spec_sections", "dollar_amounts", "keywords_present"):
            values = highlights.get(key)
            if not isinstance(values, list):
                continue
            keep: List[str] = []
            for v in values:
                if not isinstance(v, str):
                    continue
                if key == "pay_items":
                    # Enforce strict INDOT format (e.g., 706-08496)
                    pattern = getattr(self.config.patterns, "pay_item_regex", r"\b\d{3}-\d{5,6}\b")
                    if re.fullmatch(pattern, v):
                        keep.append(v)
                elif key == "spec_sections":
                    # require Section XXX match
                    if re.search(rf"Section\s+{re.escape(v)}\b", text, re.IGNORECASE):
                        keep.append(v)
                elif key == "dollar_amounts":
                    if re.search(rf"\$\s?{re.escape(v)}\b", text):
                        keep.append(v)
                else:
                    if present(v):
                        keep.append(v)
            if keep:
                verified[key] = sorted(set(keep))

        return verified


__all__ = ["MemoValidator", "ValidationDiff"]
