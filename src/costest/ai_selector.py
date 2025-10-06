"""LLM-assisted selection logic for alternate pay items."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from .ai_reporter import OpenAIClient


@dataclass
class AISelection:
    item_code: str
    weight: float
    reason: Optional[str] = None


def _get_client():
    if OpenAIClient is None:  # type: ignore[name-defined]
        raise RuntimeError(
            "openai package is not installed. Install it with 'pip install openai'."
        )
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Place it in API_KEY/ or export it before running."
        )
    return OpenAIClient(api_key=api_key)  # type: ignore[call-arg]


def _clean_json_payload(content: str) -> Mapping[str, object]:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return json.loads(text)


def choose_alternates_via_ai(
    target_info: Mapping[str, object],
    candidates: Iterable[Mapping[str, object]],
    references: Optional[Mapping[str, object]] = None,
    model: Optional[str] = None,
) -> Tuple[List[AISelection], Optional[str], Dict[str, object]]:
    """Ask the LLM to weigh candidate alternates, returning selections and metadata."""

    if os.getenv("DISABLE_OPENAI", "0").strip().lower() in ("1", "true", "yes"):
        return [], "AI disabled via DISABLE_OPENAI", {}

    client = _get_client()
    model = model or os.getenv("OPENAI_MODEL", "gpt-4.1")

    payload = {
        "target": target_info,
        "candidates": list(candidates),
        "references": references or {},
    }

    instructions = (
        "You are ChatGPT-5 acting as a senior INDOT transportation cost estimator. "
        "You may consult authoritative online sources if needed. "
        "Design a repeatable system that blends BidTabs history, the statewide unit-price summary, "
        "and the Standard Specifications excerpt provided. Return strict JSON in the form:\n"
        "{\n"
        "  \"selected\": [ {\"item_code\": str, \"weight\": float, \"reason\": str} ],\n"
        "  \"notes\": str,\n"
        "  \"system\": {\"overview\": str, \"steps\": [str], \"validation\": str},\n"
        "  \"show_work_method\": str,\n"
        "  \"process_improvements\": str\n"
        "}.\n"
        "Each candidate record includes similarity_scores (geometry/spec/recency/locality/data_volume/overall), notes, and a source tag—use these signals alongside category_counts when assigning weights. "
        "Weights must sum to 1.0. If you rely on a reference rather than a candidate, explain how to incorporate it."
    )

    response = client.chat.completions.create(  # type: ignore[attr-defined]
        model=model,
        temperature=0.15,
        max_tokens=900,
        messages=[
            {
                "role": "system",
                "content": instructions,
            },
            {
                "role": "user",
                "content": json.dumps(payload, indent=2),
            },
        ],
    )

    content = response.choices[0].message.content or ""
    data = _clean_json_payload(content)

    selected_raw = data.get("selected", []) if isinstance(data, dict) else []
    notes = data.get("notes") if isinstance(data, dict) else None
    system = data.get("system") if isinstance(data, dict) else None
    show_work_method = data.get("show_work_method") if isinstance(data, dict) else None
    process_improvements = data.get("process_improvements") if isinstance(data, dict) else None

    selections: List[AISelection] = []
    for entry in selected_raw:
        if not isinstance(entry, Mapping):
            continue
        code = str(entry.get("item_code", "")).strip()
        if not code:
            continue
        weight = entry.get("weight", 0)
        try:
            weight_val = float(weight)
        except (TypeError, ValueError):
            weight_val = 0.0
        reason = entry.get("reason")
        if reason is not None:
            reason = str(reason)
        selections.append(AISelection(item_code=code, weight=weight_val, reason=reason))

    meta = {
        "system": system,
        "show_work_method": show_work_method,
        "process_improvements": process_improvements,
    }
    return selections, str(notes) if notes is not None else None, meta
