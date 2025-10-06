"""Generate a process improvement report using the OpenAI API."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence, Optional

from .text_utils import sanitize_text

try:
    from openai import OpenAI  # type: ignore
    OpenAIClient = OpenAI  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    OpenAIClient = None  # type: ignore



def _call_openai(
    prompt: Mapping[str, object],
    model: str,
    *,
    temperature: float = 0.3,
    max_tokens: int = 2000,
) -> str:
    if OpenAIClient is None:
        raise RuntimeError("openai package is not installed. Install it with 'pip install openai'.")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set. Place it in API_KEY/ or export it before running.")

    client = OpenAIClient(api_key=api_key)

    system_prompt = (
        "You are ChatGPT-5 acting as an INDOT cost-estimation modernization lead. "
        "You may consult authoritative Internet sources if helpful. "
        "Produce a pragmatic modernization plan covering alternate-seek, upstream data processing, and reporting."
    )

    response = client.chat.completions.create(  # type: ignore[attr-defined]
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(prompt, indent=2)},
        ],
    )
    return response.choices[0].message.content.strip()



def generate_process_improvement_report(
    process_overview: Mapping[str, object],
    process_notes: Sequence[Mapping[str, object]],
    reference_snapshot: Mapping[str, object],
    output_dir: str | Path,
    model: Optional[str] = None,
) -> Optional[Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prompt = {
        "process_overview": process_overview,
        "process_notes": list(process_notes),
        "references": reference_snapshot,
        "requests": [
            "Design a sustainable alternate-seek framework that blends BidTabs, statewide unit prices, and specifications.",
            "Recommend improvements to the upstream cost-estimate pipeline (data ingestion, cleansing, QA/QC, analytics).",
            "Describe how the program should document its calculations (\"show the work\") within reports and exports.",
            "Identify metrics, controls, and modernization opportunities INDOT should pursue next, referencing best practices.",
        ],
    }

    model = model or os.getenv("OPENAI_MODEL", "gpt-4.1")
    report_text = _call_openai(prompt, model=model)
    cleaned_text = sanitize_text(report_text, ascii_only=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"Process_Improvement_Report_{timestamp}.md"
    output_path.write_text(cleaned_text, encoding="utf-8")
    return output_path


__all__ = ["generate_process_improvement_report"]
