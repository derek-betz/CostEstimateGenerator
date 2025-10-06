"""AI-assisted reporting utilities for alternate-seek explanations."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Optional

from .text_utils import sanitize_text

import pandas as pd

try:
    from openai import OpenAI  # type: ignore
    OpenAIClient = OpenAI  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    OpenAIClient = None  # type: ignore

try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import simpleSplit
except ImportError:  # pragma: no cover - handled at runtime
    canvas = None  # type: ignore


@dataclass
class AlternateReportItem:
    item_code: str
    description: str
    quantity: float
    unit: str
    target_area_sqft: Optional[float]
    chosen: Mapping[str, object]
    candidates: Iterable[Mapping[str, object]]
    unit_price_est: float
    notes: str
    project_region: Optional[int]
    references: Optional[Mapping[str, object]] = None
    ai_system: Optional[Mapping[str, object]] = None
    show_work_method: Optional[str] = None
    process_improvements: Optional[str] = None


def _format_prompt(project_context: Mapping[str, object], items: list[AlternateReportItem]) -> str:
    payload = {
        "project": project_context,
        "alternate_seek_items": [
            {
                "item_code": i.item_code,
                "description": i.description,
                "unit": i.unit,
                "quantity": i.quantity,
                "target_area_sqft": i.target_area_sqft,
                "unit_price_est": i.unit_price_est,
                "notes": i.notes,
                "project_region": i.project_region,
                "chosen": i.chosen,
                "candidates": list(i.candidates),
                "references": i.references,
                "ai_system": i.ai_system,
                "show_work_method": i.show_work_method,
                "process_improvements": i.process_improvements,
            }
            for i in items
        ],
    }

    instructions = (
        "You are ChatGPT-5 acting as a senior cost estimator. "
        "Prepare an engineering justification that explains how alternate pay items "
        "were used to price the requested items. For each item produce sections with: "
        "(1) a short narrative describing why alternates were needed, (2) a bullet list "
        "summarising every candidate with available data counts, (3) a weighting narrative that "
        "states which alternates were blended, the weight assigned to each, and why those weights "
        "are appropriate (fold any area-ratio explanation into this narrative rather than a "
        "separate section), (4) a 'Show the Work' subsection following the provided show_work_method "
        "guidance and laid out in labelled lines such as 'Given:', 'Formula:', 'Substitute:', 'Result:' using plain text "
        "with parentheses, '*' for multiplication, '/' for division, and decimals kept as decimals, and (5) a concluding "
        "remark on why the resulting unit price is reasonable for the project scope. Cite any references (spec sections, "
        "statewide summaries, or external standards) that influence your reasoning."
    )

    return instructions + "\n\nData:\n" + json.dumps(payload, indent=2)


def _call_openai(prompt: str, model: str, temperature: float = 0.2, max_tokens: int = 1800) -> str:
    if OpenAIClient is None:
        raise RuntimeError(
            "openai package is not installed. Install it with 'pip install openai'."
        )

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Place it in API_KEY/ or export the variable before running."
        )

    client = OpenAIClient(api_key=api_key)
    response = client.chat.completions.create(  # type: ignore[attr-defined]
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are ChatGPT-5, a senior transportation cost estimator.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content.strip()


def _write_pdf(report_text: str, output_path: Path) -> Path:
    if canvas is None:
        raise RuntimeError(
            "reportlab is not installed. Install it with 'pip install reportlab'."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    cleaned = sanitize_text(report_text, ascii_only=True)

    c = canvas.Canvas(str(output_path), pagesize=letter)
    width, height = letter
    margin = 0.75 * inch
    text_width = width - 2 * margin

    text_object = c.beginText(margin, height - margin)
    text_object.setFont("Helvetica", 11)

    lines = []
    for raw_line in cleaned.splitlines():
        wrapped = simpleSplit(raw_line, "Helvetica", 11, text_width)
        if not wrapped:
            lines.append("")
        else:
            lines.extend(wrapped)

    for line in lines:
        text_object.textLine(line)
        if text_object.getY() <= margin:
            c.drawText(text_object)
            c.showPage()
            text_object = c.beginText(margin, height - margin)
            text_object.setFont("Helvetica", 11)

    c.drawText(text_object)
    c.save()
    return output_path


def generate_alternate_seek_report(
    estimate_df: pd.DataFrame,
    alternate_reports: Mapping[str, Mapping[str, object]],
    output_dir: str | Path,
    project_region: Optional[int],
    expected_contract_cost: Optional[float],
    filtered_bounds: Optional[tuple[float, float]],
    model: Optional[str] = None,
) -> Optional[Path]:
    if not alternate_reports:
        return None

    output_dir = Path(output_dir)
    model = model or os.getenv("OPENAI_MODEL", "gpt-4.1")

    items: list[AlternateReportItem] = []
    for item_code, payload in alternate_reports.items():
        match = estimate_df.loc[estimate_df["ITEM_CODE"] == item_code]
        if match.empty:
            continue
        record = match.iloc[0]
        chosen_payload = payload.get("chosen")
        if not chosen_payload and "selected" in payload:
            chosen_payload = {"selections": payload.get("selected", [])}
        if not chosen_payload:
            chosen_payload = {}
        candidates = payload.get("candidates", [])
        normalized_chosen = dict(chosen_payload)
        for key, value in list(normalized_chosen.items()):
            if key == "selections":
                continue
            if isinstance(value, (int, float)):
                normalized_chosen[key] = float(value)
        if "selections" in normalized_chosen:
            cleaned_selections = []
            for entry in normalized_chosen.get("selections", []):
                entry_dict = dict(entry)
                for sub_key, sub_val in list(entry_dict.items()):
                    if isinstance(sub_val, (int, float)):
                        entry_dict[sub_key] = float(sub_val)
                cleaned_selections.append(entry_dict)
            normalized_chosen["selections"] = cleaned_selections
        if "notes" in normalized_chosen and normalized_chosen["notes"] is not None:
            normalized_chosen["notes"] = str(normalized_chosen["notes"])
        normalized_candidates = []
        for cand in candidates:
            cand_dict = dict(cand)
            for key, value in list(cand_dict.items()):
                if isinstance(value, (int, float)):
                    cand_dict[key] = float(value)
            normalized_candidates.append(cand_dict)
        items.append(
            AlternateReportItem(
                item_code=item_code,
                description=str(record.get("DESCRIPTION", "")),
                unit=str(record.get("UNIT", "")),
                quantity=float(record.get("QUANTITY", 0) or 0),
                target_area_sqft=float(payload.get("target_area_sqft") or 0) or None,
                unit_price_est=float(record.get("UNIT_PRICE_EST", 0) or 0.0),
                notes=str(record.get("NOTES", "")),
                chosen=normalized_chosen,
                candidates=normalized_candidates,
                project_region=project_region,
            )
        )

    if not items:
        return None

    context = {
        "project_region": project_region,
        "expected_contract_cost": expected_contract_cost,
        "contract_filter_bounds": filtered_bounds,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    prompt = _format_prompt(context, items)
    narrative = _call_openai(prompt, model=model)

    filename = f"Alternate_Seek_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
    output_path = output_dir / filename
    return _write_pdf(narrative, output_path)
