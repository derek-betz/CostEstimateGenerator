"""Helpers for finding alternate pay items when direct BidTabs data is missing."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd

from . import reference_data
from .geometry import GeometryInfo
from .price_logic import category_breakdown, MIN_SAMPLE_TARGET
from .ai_selector import choose_alternates_via_ai, AISelection

CATEGORY_LABELS = [
    "DIST_12M",
    "DIST_24M",
    "DIST_36M",
    "STATE_12M",
    "STATE_24M",
    "STATE_36M",
]

SIMILARITY_WEIGHTS = {
    "geometry_score": 0.35,
    "spec_score": 0.25,
    "recency_score": 0.2,
    "locality_score": 0.1,
    "data_volume_score": 0.1,
}

_MIN_TARGET = max(10, MIN_SAMPLE_TARGET)


@dataclass
class AlternateCandidate:
    item_code: str
    description: str
    area_sqft: float
    base_price: float
    adjusted_price: float
    ratio: float
    data_points: int
    cat_data: Mapping[str, float]
    shape: Optional[str]
    source: str
    similarity: Dict[str, float] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    spec_section: Optional[str] = None


@dataclass
class SelectedAlternate:
    item_code: str
    description: str
    area_sqft: float
    base_price: float
    adjusted_price: float
    ratio: float
    data_points: int
    weight: float
    reason: Optional[str] = None
    similarity: Mapping[str, float] | None = None
    source: Optional[str] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class AlternateResult:
    target_code: str
    target_area: float
    candidates: List[AlternateCandidate]
    selections: List[SelectedAlternate]
    final_price: float
    detail_map: Dict[str, pd.DataFrame]
    used_categories: List[str]
    combined_detail: pd.DataFrame
    cat_data: Dict[str, float]
    total_data_points: int
    ai_notes: Optional[str]
    candidate_payload: List[Dict[str, object]]
    similarity_summary: Dict[str, float]
    candidate_notes: Dict[str, List[str]]
    reference_bundle: Mapping[str, object] | None = None
    ai_system: Optional[Mapping[str, object]] = None
    show_work_method: Optional[str] = None
    process_improvements: Optional[str] = None


def _item_prefix(item_code: str) -> str:
    item_code = str(item_code)
    if "-" in item_code:
        return item_code.split("-", 1)[0]
    return item_code[:3]


def _clamp(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _extract_section_id(bundle: Mapping[str, object] | None) -> str:
    if not bundle:
        return ""
    payitem = bundle.get("payitem") if isinstance(bundle.get("payitem"), Mapping) else {}
    section = str(payitem.get("section") or "").strip()
    if section:
        return section
    unit_price = bundle.get("unit_price") if isinstance(bundle.get("unit_price"), Mapping) else {}
    section = str(unit_price.get("section") or "").strip()
    if section:
        return section
    spec_meta = bundle.get("spec_section") if isinstance(bundle.get("spec_section"), Mapping) else {}
    section = str(spec_meta.get("id") or "").strip()
    return section


def _has_keyword(text: str | None, keyword: str) -> bool:
    if not text:
        return False
    return keyword.upper() in text.upper()


def _score_candidate(
    target_area: float,
    target_shape: Optional[str],
    candidate: AlternateCandidate,
    target_bundle: Mapping[str, object] | None,
    candidate_bundle: Mapping[str, object] | None,
    *,
    target_description: Optional[str] = None,
    area_tolerance: float = 0.2,
) -> Tuple[Dict[str, float], List[str]]:
    notes: List[str] = []

    area_ratio = 0.0
    if candidate.area_sqft > 0 and target_area > 0:
        area_ratio = min(candidate.area_sqft, target_area) / max(candidate.area_sqft, target_area)
        if abs(candidate.area_sqft - target_area) / max(target_area, 1e-6) > area_tolerance:
            notes.append(
                f"Area differs {abs(candidate.area_sqft - target_area) / max(target_area, 1e-6):.0%} from target"
            )
    shape_score = 0.5
    if target_shape and candidate.shape:
        if target_shape == candidate.shape:
            shape_score = 1.0
        elif target_shape[:3] == candidate.shape[:3]:
            shape_score = 0.7
        else:
            shape_score = 0.4
            notes.append(f"Shape mismatch: target={target_shape} candidate={candidate.shape}")
    elif target_shape or candidate.shape:
        shape_score = 0.6
    geometry_score = _clamp(0.7 * area_ratio + 0.3 * shape_score)

    target_section = _extract_section_id(target_bundle)
    candidate_section = _extract_section_id(candidate_bundle)
    spec_score = 0.5
    if target_section and candidate_section:
        if target_section == candidate_section:
            spec_score = 1.0
        elif target_section.split(".")[0] == candidate_section.split(".")[0]:
            spec_score = 0.75
        else:
            spec_score = 0.55
            notes.append(f"Spec section differs: target={target_section} candidate={candidate_section}")
    elif candidate_section:
        spec_score = 0.6
    else:
        notes.append("Candidate missing specification section metadata")

    target_payitem_desc = ""
    if isinstance(target_bundle, Mapping):
        try:
            target_payitem_desc = str((target_bundle.get("payitem") or {}).get("description", ""))
        except AttributeError:
            target_payitem_desc = ""
    target_desc = " ".join(filter(None, [target_description, target_payitem_desc]))

    candidate_payitem_desc = ""
    if isinstance(candidate_bundle, Mapping):
        try:
            candidate_payitem_desc = str((candidate_bundle.get("payitem") or {}).get("description", ""))
        except AttributeError:
            candidate_payitem_desc = ""
    cand_desc = " ".join(filter(None, [candidate.description, candidate_payitem_desc]))

    def _adjust_for_keyword(keyword: str, penalty: float = 0.15) -> None:
        nonlocal spec_score
        if _has_keyword(target_desc, keyword) != _has_keyword(cand_desc, keyword):
            spec_score = _clamp(spec_score - penalty)
            notes.append(f"Keyword mismatch: '{keyword}' present in one description only")

    for kw in ("COAT", "GALV", "REINFORC", "TEMPORARY", "POLYMER", "STAINLESS"):
        _adjust_for_keyword(kw)

    counts = {label: int(candidate.cat_data.get(f"{label}_COUNT", 0) or 0) for label in CATEGORY_LABELS}
    total_counts = sum(counts.values())
    recent = counts["DIST_12M"] + counts["STATE_12M"]
    mid = counts["DIST_24M"] + counts["STATE_24M"]
    long = counts["DIST_36M"] + counts["STATE_36M"]
    recency_score = 0.0
    if total_counts:
        recency_score = _clamp((3 * recent + 2 * mid + long) / (3 * total_counts))
    locality_counts = counts["DIST_12M"] + counts["DIST_24M"] + counts["DIST_36M"]
    locality_score = _clamp(locality_counts / total_counts) if total_counts else 0.0
    if total_counts == 0:
        notes.append("No BidTabs recency data available; relying on statewide surrogates")

    data_volume_score = _clamp(candidate.data_points / max(_MIN_TARGET, 1))
    if candidate.data_points < _MIN_TARGET:
        notes.append(f"Only {candidate.data_points} BidTabs data points (target {_MIN_TARGET})")

    scores = {
        "geometry_score": geometry_score,
        "spec_score": _clamp(spec_score),
        "recency_score": recency_score,
        "locality_score": locality_score,
        "data_volume_score": data_volume_score,
    }

    overall = 0.0
    for key, weight in SIMILARITY_WEIGHTS.items():
        overall += weight * scores.get(key, 0.0)
    scores["overall_score"] = _clamp(overall)

    return scores, notes


def _build_candidate(
    bidtabs: pd.DataFrame,
    code: str,
    group: pd.DataFrame,
    target_area: float,
    target_shape: Optional[str],
    project_region: int | None,
    target_bundle: Mapping[str, object] | None,
    *,
    target_description: Optional[str] = None,
    area_tolerance: float = 0.2,
    source: str,
) -> Optional[AlternateCandidate]:
    area_series = pd.to_numeric(group.get("GEOM_AREA_SQFT"), errors="coerce") if "GEOM_AREA_SQFT" in group else None
    if area_series is not None:
        area_series = area_series.dropna()
    candidate_area = float(area_series.mean()) if area_series is not None and not area_series.empty else target_area

    shape_series = group.get("GEOM_SHAPE")
    candidate_shape = None
    if shape_series is not None and not shape_series.dropna().empty:
        candidate_shape = str(shape_series.dropna().iloc[0])

    price, _, cat_data = category_breakdown(
        bidtabs,
        code,
        project_region=project_region,
        include_details=False,
    )
    if price is None or (isinstance(price, float) and math.isnan(price)):
        return None

    data_points = int(cat_data.get("TOTAL_USED_COUNT", 0))
    if data_points <= 0 and source.startswith("bidtabs"):
        return None

    ratio = target_area / candidate_area if candidate_area and math.isfinite(candidate_area) else 1.0
    adjusted_price = float(price) * (ratio if math.isfinite(ratio) else 1.0)

    description = ""
    desc_series = group.get("DESCRIPTION")
    if desc_series is not None and not desc_series.dropna().empty:
        description = str(desc_series.dropna().iloc[0])

    candidate_bundle = reference_data.build_reference_bundle(code)
    placeholder = AlternateCandidate(
        item_code=code,
        description=description,
        area_sqft=candidate_area,
        base_price=float(price),
        adjusted_price=float(adjusted_price),
        ratio=float(ratio),
        data_points=data_points,
        cat_data=dict(cat_data),
        shape=candidate_shape,
        source=source,
    )
    scores, notes = _score_candidate(
        target_area,
        target_shape,
        placeholder,
        target_bundle,
        candidate_bundle,
        target_description=target_description,
        area_tolerance=area_tolerance,
    )

    placeholder.similarity = scores
    placeholder.notes = notes
    placeholder.spec_section = _extract_section_id(candidate_bundle)
    return placeholder


def _build_unit_price_candidate(
    target_area: float,
    unit_price_value: float,
    contracts: int,
    reference_bundle: Mapping[str, object] | None,
) -> AlternateCandidate:
    candidate_bundle = reference_bundle or {}
    scores = {
        "geometry_score": 0.6,
        "spec_score": 0.65 if _extract_section_id(candidate_bundle) else 0.5,
        "recency_score": 0.5,
        "locality_score": 0.4,
        "data_volume_score": _clamp(contracts / max(_MIN_TARGET, 1)),
    }
    overall = sum(SIMILARITY_WEIGHTS[k] * scores.get(k, 0.0) for k in SIMILARITY_WEIGHTS)
    scores["overall_score"] = _clamp(overall)
    notes = ["Statewide weighted average from Unit Price Summary"]
    if contracts <= 0:
        notes.append("No contract count supplied in Unit Price Summary")
    return AlternateCandidate(
        item_code="UNIT_PRICE_SUMMARY",
        description="Statewide weighted average unit price",
        area_sqft=target_area,
        base_price=unit_price_value,
        adjusted_price=unit_price_value,
        ratio=1.0,
        data_points=max(contracts, 0),
        cat_data={},
        shape=None,
        source="unit_price_summary",
        similarity=scores,
        notes=notes,
        spec_section=_extract_section_id(reference_bundle),
    )


def _fallback_selection(
    candidates: List[AlternateCandidate],
    target_area: float,
    limit: int = 3,
) -> List[SelectedAlternate]:
    if not candidates:
        return []

    ranked = sorted(
        candidates,
        key=lambda cand: (
            cand.similarity.get("overall_score", 0.0),
            cand.data_points,
            -abs(cand.area_sqft - target_area) if math.isfinite(cand.area_sqft) else 0.0,
        ),
        reverse=True,
    )
    chosen = [cand for cand in ranked if cand.similarity.get("overall_score", 0.0) > 0][:limit]
    if not chosen:
        chosen = ranked[:1]

    total = sum(c.similarity.get("overall_score", 0.0) for c in chosen)
    if total <= 0:
        total = sum(max(c.data_points, 1) for c in chosen)
    selections: List[SelectedAlternate] = []
    for cand in chosen:
        weight = cand.similarity.get("overall_score", 0.0) / total if total else (1.0 / len(chosen))
        selections.append(
            SelectedAlternate(
                item_code=cand.item_code,
                description=cand.description,
                area_sqft=cand.area_sqft,
                base_price=cand.base_price,
                adjusted_price=cand.adjusted_price,
                ratio=cand.ratio,
                data_points=cand.data_points,
                weight=float(weight),
                reason="Fallback: similarity-based weighting",
                similarity=cand.similarity,
                source=cand.source,
                notes=cand.notes,
            )
        )
    return selections


def _normalize_weights(
    selections: List[SelectedAlternate],
    candidate_map: Mapping[str, AlternateCandidate],
) -> List[SelectedAlternate]:
    total = sum(max(sel.weight, 0.0) for sel in selections)
    if total <= 0:
        total = sum(
            candidate_map.get(sel.item_code, AlternateCandidate("", "", 0, 0, 0, 1, 0, {}, None, "")).similarity.get("overall_score", 0.0)
            for sel in selections
        )
    if total <= 0:
        total = sum(sel.data_points for sel in selections)
    if total <= 0 and selections:
        total = len(selections)

    if total <= 0:
        return selections

    for sel in selections:
        weight = max(sel.weight, 0.0)
        if weight == 0 and sel.item_code in candidate_map:
            weight = candidate_map[sel.item_code].similarity.get("overall_score", 0.0)
        if weight == 0:
            weight = sel.data_points or 1.0
        sel.weight = float(weight / total)
    return selections


def find_alternate_price(
    bidtabs: pd.DataFrame,
    target_code: str,
    target_geometry: GeometryInfo | None,
    area_tolerance: float = 0.2,
    project_region: int | None = None,
    target_description: Optional[str] = None,
    reference_bundle: Optional[Mapping[str, object]] = None,
) -> Optional[AlternateResult]:
    """Return an alternate-seek estimate enriched with reference datasets."""

    if target_geometry is None or not math.isfinite(target_geometry.area_sqft) or target_geometry.area_sqft <= 0:
        return None

    if "GEOM_AREA_SQFT" not in bidtabs.columns:
        return None

    reference_bundle = reference_bundle or reference_data.build_reference_bundle(target_code)
    unit_price_info = (reference_bundle or {}).get("unit_price") or {}
    unit_price_value = float(unit_price_info.get("weighted_average") or 0)
    unit_price_contracts = int(float(unit_price_info.get("contracts", 0) or 0))

    target_area = target_geometry.area_sqft
    target_shape = getattr(target_geometry, "shape", None)
    prefix = _item_prefix(target_code)

    candidates_df = bidtabs.copy()
    candidates_df = candidates_df.loc[candidates_df["ITEM_CODE"].astype(str).str.startswith(prefix + "-")]
    candidates_df = candidates_df.loc[candidates_df["ITEM_CODE"] != target_code]
    candidates_df = candidates_df.loc[candidates_df["GEOM_AREA_SQFT"].notna()]

    if target_shape and target_shape != "min_area":
        candidates_df = candidates_df.loc[candidates_df["GEOM_SHAPE"] == target_shape]

    lower = target_area * (1 - area_tolerance)
    upper = target_area * (1 + area_tolerance)
    candidates_df = candidates_df.loc[(candidates_df["GEOM_AREA_SQFT"] >= lower) & (candidates_df["GEOM_AREA_SQFT"] <= upper)]

    candidates: List[AlternateCandidate] = []
    candidate_payload: List[Dict[str, object]] = []
    candidate_map: Dict[str, AlternateCandidate] = {}

    for code, group in candidates_df.groupby("ITEM_CODE"):
        candidate = _build_candidate(
            bidtabs,
            str(code),
            group,
            target_area,
            target_shape,
            project_region,
            reference_bundle,
            target_description=target_description,
            area_tolerance=area_tolerance,
            source="bidtabs-prefix",
        )
        if not candidate:
            continue
        candidates.append(candidate)
        candidate_map[candidate.item_code] = candidate
        candidate_payload.append(
            {
                "item_code": candidate.item_code,
                "description": candidate.description,
                "area_sqft": candidate.area_sqft,
                "shape": candidate.shape,
                "adjusted_price": candidate.adjusted_price,
                "base_price": candidate.base_price,
                "ratio": candidate.ratio,
                "data_points": candidate.data_points,
                "similarity_scores": candidate.similarity,
                "notes": candidate.notes,
                "source": candidate.source,
                "category_counts": {
                    label: int(candidate.cat_data.get(f"{label}_COUNT", 0) or 0)
                    for label in CATEGORY_LABELS
                },
                "spec_section": candidate.spec_section,
            }
        )

    related_items = (reference_bundle or {}).get("related_items") or []
    for related in related_items:
        code = str(related.get("item_code") or "").strip()
        if not code or code == target_code or code in candidate_map:
            continue
        related_group = bidtabs.loc[bidtabs["ITEM_CODE"].astype(str) == code]
        if related_group.empty and unit_price_value <= 0:
            continue
        if related_group.empty:
            continue
        candidate = _build_candidate(
            bidtabs,
            code,
            related_group,
            target_area,
            target_shape,
            project_region,
            reference_bundle,
            target_description=target_description,
            area_tolerance=0.35,
            source="bidtabs-related",
        )
        if not candidate:
            continue
        candidates.append(candidate)
        candidate_map[candidate.item_code] = candidate
        candidate_payload.append(
            {
                "item_code": candidate.item_code,
                "description": candidate.description,
                "area_sqft": candidate.area_sqft,
                "shape": candidate.shape,
                "adjusted_price": candidate.adjusted_price,
                "base_price": candidate.base_price,
                "ratio": candidate.ratio,
                "data_points": candidate.data_points,
                "similarity_scores": candidate.similarity,
                "notes": candidate.notes,
                "source": candidate.source,
                "category_counts": {
                    label: int(candidate.cat_data.get(f"{label}_COUNT", 0) or 0)
                    for label in CATEGORY_LABELS
                },
                "spec_section": candidate.spec_section,
            }
        )

    if unit_price_value > 0:
        reference_candidate = _build_unit_price_candidate(target_area, unit_price_value, unit_price_contracts, reference_bundle)
        candidates.append(reference_candidate)
        candidate_map[reference_candidate.item_code] = reference_candidate
        candidate_payload.append(
            {
                "item_code": reference_candidate.item_code,
                "description": reference_candidate.description,
                "area_sqft": reference_candidate.area_sqft,
                "shape": reference_candidate.shape,
                "adjusted_price": reference_candidate.adjusted_price,
                "base_price": reference_candidate.base_price,
                "ratio": reference_candidate.ratio,
                "data_points": reference_candidate.data_points,
                "similarity_scores": reference_candidate.similarity,
                "notes": reference_candidate.notes,
                "source": reference_candidate.source,
                "spec_section": reference_candidate.spec_section,
            }
        )

    if not candidates:
        return None

    target_info = {
        "item_code": target_code,
        "description": target_description,
        "geometry_shape": target_shape,
        "target_area_sqft": target_area,
        "area_tolerance": area_tolerance,
        "payitem_reference": (reference_bundle or {}).get("payitem"),
        "unit_price_reference": (reference_bundle or {}).get("unit_price"),
        "spec_reference": {
            "metadata": (reference_bundle or {}).get("spec_section"),
            "text": (reference_bundle or {}).get("spec_text"),
        },
        "related_items": (reference_bundle or {}).get("related_items"),
    }

    ai_notes: Optional[str] = None
    ai_meta: Dict[str, object] = {}
    selections: List[SelectedAlternate] = []

    candidate_payload_for_ai = []
    for payload in candidate_payload:
        payload_copy = dict(payload)
        payload_copy["similarity_scores"] = dict(payload_copy.get("similarity_scores", {}))
        candidate_payload_for_ai.append(payload_copy)

    try:
        ai_selected, ai_notes, ai_meta = choose_alternates_via_ai(
            target_info=target_info,
            candidates=candidate_payload_for_ai,
            references=reference_bundle,
        )
        for sel in ai_selected:
            cand = candidate_map.get(sel.item_code)
            if not cand:
                continue
            selections.append(
                SelectedAlternate(
                    item_code=cand.item_code,
                    description=cand.description,
                    area_sqft=cand.area_sqft,
                    base_price=cand.base_price,
                    adjusted_price=cand.adjusted_price,
                    ratio=cand.ratio,
                    data_points=cand.data_points,
                    weight=sel.weight,
                    reason=sel.reason,
                    similarity=cand.similarity,
                    source=cand.source,
                    notes=cand.notes,
                )
            )
    except Exception as exc:  # noqa: BLE001
        ai_notes = f"AI selection failed: {exc}"

    if not selections:
        selections = _fallback_selection(candidates, target_area)
        if not selections:
            return None

    selections = _normalize_weights(selections, candidate_map)

    aggregated_detail_map: Dict[str, List[pd.DataFrame]] = {label: [] for label in CATEGORY_LABELS}
    combined_frames: List[pd.DataFrame] = []
    price_sum_by_category: Dict[str, float] = {label: 0.0 for label in CATEGORY_LABELS}
    count_by_category: Dict[str, int] = {label: 0 for label in CATEGORY_LABELS}
    combined_ids = set()
    reference_used = any(sel.item_code == "UNIT_PRICE_SUMMARY" for sel in selections)

    for sel in selections:
        if sel.item_code == "UNIT_PRICE_SUMMARY":
            continue

        price, _, cat_data, detail_map, used_categories, combined_detail = category_breakdown(
            bidtabs,
            sel.item_code,
            project_region=project_region,
            include_details=True,
        )
        ratio = sel.ratio if sel.ratio and math.isfinite(sel.ratio) else 1.0

        for label in CATEGORY_LABELS:
            count = int(cat_data.get(f"{label}_COUNT", 0) or 0)
            price_value = cat_data.get(f"{label}_PRICE")
            if count > 0 and isinstance(price_value, (int, float)) and not math.isnan(price_value):
                adjusted = float(price_value) * ratio
                price_sum_by_category[label] += adjusted * count
                count_by_category[label] += count

        for label, df in (detail_map or {}).items():
            if df is None or df.empty:
                continue
            local = df.copy()
            local["ALT_SOURCE_ITEM"] = sel.item_code
            local["ALT_WEIGHT"] = sel.weight
            local["ALT_RATIO"] = sel.ratio
            aggregated_detail_map.setdefault(label, []).append(local)
            if "_AUDIT_ROW_ID" in local.columns:
                combined_ids.update(local["_AUDIT_ROW_ID"].tolist())

        if combined_detail is not None and not combined_detail.empty:
            local_combined = combined_detail.copy()
            local_combined["ALT_SOURCE_ITEM"] = sel.item_code
            local_combined["ALT_WEIGHT"] = sel.weight
            local_combined["ALT_RATIO"] = sel.ratio
            combined_frames.append(local_combined)
            if "_AUDIT_ROW_ID" in local_combined.columns:
                combined_ids.update(local_combined["_AUDIT_ROW_ID"].tolist())

    detail_map_final: Dict[str, pd.DataFrame] = {}
    for label, frames in aggregated_detail_map.items():
        if not frames:
            detail_map_final[label] = pd.DataFrame()
            continue
        merged = pd.concat(frames, ignore_index=True)
        if "_AUDIT_ROW_ID" in merged.columns:
            merged = merged.drop_duplicates(subset="_AUDIT_ROW_ID", keep="first")
        detail_map_final[label] = merged

    if combined_frames:
        combined_detail = pd.concat(combined_frames, ignore_index=True)
        if "_AUDIT_ROW_ID" in combined_detail.columns:
            combined_detail = combined_detail.drop_duplicates(subset="_AUDIT_ROW_ID", keep="first")
    else:
        combined_detail = pd.DataFrame()

    total_data_points = len(combined_detail)
    if not combined_ids and not combined_detail.empty and "_AUDIT_ROW_ID" in combined_detail.columns:
        total_data_points = combined_detail["_AUDIT_ROW_ID"].nunique()
    elif combined_ids:
        total_data_points = len(combined_ids)

    aggregated_cat_data: Dict[str, float] = {"TOTAL_USED_COUNT": total_data_points}
    used_categories: List[str] = []
    for label in CATEGORY_LABELS:
        count = count_by_category[label]
        aggregated_cat_data[f"{label}_COUNT"] = count
        if count > 0:
            used_categories.append(label)
            price_total = price_sum_by_category[label]
            aggregated_cat_data[f"{label}_PRICE"] = price_total / count if count else float("nan")
        else:
            aggregated_cat_data[f"{label}_PRICE"] = float("nan")

    if reference_used or unit_price_value > 0:
        aggregated_cat_data["UNIT_PRICE_SUMMARY"] = unit_price_value
        aggregated_cat_data["UNIT_PRICE_SUMMARY_CONTRACTS"] = unit_price_contracts
        aggregated_cat_data["TOTAL_USED_COUNT"] = max(aggregated_cat_data["TOTAL_USED_COUNT"], unit_price_contracts)

    final_price = sum(sel.weight * sel.adjusted_price for sel in selections)

    system_meta = ai_meta.get("system") if isinstance(ai_meta.get("system"), Mapping) else None

    similarity_summary: Dict[str, float] = {key: 0.0 for key in SIMILARITY_WEIGHTS}
    similarity_summary["overall_score"] = 0.0
    for sel in selections:
        cand = candidate_map.get(sel.item_code)
        if not cand:
            continue
        for key in SIMILARITY_WEIGHTS:
            similarity_summary[key] += sel.weight * cand.similarity.get(key, 0.0)
        similarity_summary["overall_score"] += sel.weight * cand.similarity.get("overall_score", 0.0)

    candidate_notes = {cand.item_code: cand.notes for cand in candidates if cand.notes}

    return AlternateResult(
        target_code=target_code,
        target_area=target_area,
        candidates=candidates,
        selections=selections,
        final_price=float(final_price),
        detail_map=detail_map_final,
        used_categories=used_categories,
        combined_detail=combined_detail,
        cat_data=aggregated_cat_data,
        total_data_points=int(aggregated_cat_data["TOTAL_USED_COUNT"]),
        ai_notes=ai_notes,
        candidate_payload=candidate_payload,
        similarity_summary=similarity_summary,
        candidate_notes=candidate_notes,
        reference_bundle=reference_bundle,
        ai_system=system_meta,
        show_work_method=str(ai_meta.get("show_work_method")) if ai_meta.get("show_work_method") is not None else None,
        process_improvements=str(ai_meta.get("process_improvements")) if ai_meta.get("process_improvements") is not None else None,
    )
