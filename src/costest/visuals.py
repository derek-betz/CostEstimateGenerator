"""Optional visual summaries for BidTabs-derived pricing data."""

from __future__ import annotations

import io
import math
import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

try:  # pragma: no cover - optional dependency
    import matplotlib

    matplotlib.use("Agg")  # Ensure headless operation on CI/servers.
    import matplotlib.pyplot as plt
    from matplotlib.ticker import StrMethodFormatter
except Exception:  # pragma: no cover - matplotlib unavailable or misconfigured
    plt = None  # type: ignore
    StrMethodFormatter = None  # type: ignore

from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


@dataclass
class _ChartRecord:
    """Metadata captured for PDF bundling."""

    title: str
    caption: str
    image_bytes: bytes


def _as_bool_mask(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    lowered = series.astype(str).str.strip().str.lower()
    truthy = {"1", "true", "yes", "y", "t"}
    return lowered.isin(truthy)


def _sanitize_item_code(code: str, max_length: int = 60) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", code).strip("_")
    if not cleaned:
        cleaned = "item"
    return cleaned[:max_length]


def _currency_formatter() -> Optional[StrMethodFormatter]:
    if StrMethodFormatter is None:
        return None
    return StrMethodFormatter("$ {x:,.2f}")


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _prepare_used_rows(payitem_details: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for code, detail in payitem_details.items():
        if detail is None or detail.empty:
            continue
        subset = detail.copy()
        if "USED_FOR_PRICING" in subset.columns:
            mask = _as_bool_mask(subset["USED_FOR_PRICING"])
            subset = subset.loc[mask].copy()
        if subset.empty:
            continue
        if "ITEM_CODE" not in subset.columns:
            subset["ITEM_CODE"] = code
        frames.append(subset)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    if "UNIT_PRICE" in combined.columns:
        combined["UNIT_PRICE"] = _to_numeric(combined["UNIT_PRICE"])
    if "LETTING_DATE" in combined.columns:
        combined["LETTING_DATE"] = pd.to_datetime(combined["LETTING_DATE"], errors="coerce")
    return combined


def _write_figure(
    fig: "plt.Figure",
    base_name: str,
    output_dir: Path,
    *,
    save_png: bool,
    save_pdf: bool,
    dpi: int = 140,
) -> Tuple[List[Path], bytes]:
    output_dir.mkdir(parents=True, exist_ok=True)
    created: List[Path] = []
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=dpi, bbox_inches="tight")
    png_bytes = buffer.getvalue()
    if save_png:
        png_path = output_dir / f"{base_name}.png"
        with open(png_path, "wb") as handle:
            handle.write(png_bytes)
        created.append(png_path)
    if save_pdf:
        pdf_path = output_dir / f"{base_name}.pdf"
        fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
        created.append(pdf_path)
    plt.close(fig)
    return created, png_bytes


def _downsample(df: pd.DataFrame, max_points: int = 10000, random_state: int = 42) -> pd.DataFrame:
    if len(df) <= max_points:
        return df
    return df.sample(n=max_points, random_state=random_state)


def emit_visualizations(
    df: pd.DataFrame,
    payitem_details: Dict[str, pd.DataFrame],
    output_dir: str | Path,
    *,
    top_n_items: int = 20,
    format: str = "png",
    bundle_pdf: bool = True,
) -> Dict[str, object]:
    """Emit optional charts summarizing pricing inputs used by the estimator."""

    if plt is None:
        return {"charts": [], "pdf": None, "skipped": ["matplotlib not available"]}

    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    fmt = (format or "png").lower()
    save_png = fmt in {"png", "both"}
    save_pdf = fmt in {"pdf", "both"}
    if not (save_png or save_pdf):
        save_png = True  # Fallback to PNG to ensure artifacts exist.

    used_rows = _prepare_used_rows(payitem_details)

    charts: List[Path] = []
    skipped: List[str] = []
    pdf_entries: List[_ChartRecord] = []
    formatter = _currency_formatter()

    def record_chart(
        fig: "plt.Figure",
        base_name: str,
        title: str,
        caption: str,
    ) -> None:
        nonlocal charts, pdf_entries
        try:
            created, png_bytes = _write_figure(
                fig,
                base_name,
                target_dir,
                save_png=save_png,
                save_pdf=save_pdf,
            )
            charts.extend(created)
            if bundle_pdf:
                pdf_entries.append(_ChartRecord(title=title, caption=caption, image_bytes=png_bytes))
        except Exception as exc:  # pragma: no cover - robust path
            skipped.append(f"failed to save {base_name}: {exc}")

    # Overall unit price distribution -------------------------------------------------
    try:
        prices = used_rows["UNIT_PRICE"] if "UNIT_PRICE" in used_rows.columns else pd.Series(dtype=float)
        prices = _to_numeric(prices).dropna()
        prices = prices[prices > 0]
        if prices.empty:
            skipped.append("overall histogram skipped (no valid unit prices)")
        else:
            fig, ax = plt.subplots(figsize=(8, 5), dpi=140)
            ax.hist(prices, bins=min(50, max(10, int(math.sqrt(len(prices))))), color="#4C72B0", edgecolor="white", alpha=0.85)
            ax.set_title("Unit Price Distribution (All Pricing Rows)")
            ax.set_xlabel("Unit Price")
            ax.set_ylabel("Frequency")
            if formatter is not None:
                ax.xaxis.set_major_formatter(formatter)
            ax.grid(True, axis="y", linestyle="--", alpha=0.3)
            fig.tight_layout()
            record_chart(fig, "overall_unit_price_hist", "Overall Unit Price Distribution", "Histogram of unit prices from all BidTabs rows marked as used for pricing.")
    except Exception as exc:  # pragma: no cover - defensive
        skipped.append(f"overall histogram failed: {exc}")

    # Boxplot by category ------------------------------------------------------------
    try:
        if {"CATEGORY", "UNIT_PRICE"}.issubset(used_rows.columns):
            cat_data = used_rows[["CATEGORY", "UNIT_PRICE"]].dropna()
            cat_data["UNIT_PRICE"] = _to_numeric(cat_data["UNIT_PRICE"])
            cat_data = cat_data.dropna()
            if cat_data.empty or cat_data["CATEGORY"].nunique() <= 1:
                skipped.append("boxplot by category skipped (insufficient category diversity)")
            else:
                order = sorted(cat_data["CATEGORY"].unique())
                data = [cat_data.loc[cat_data["CATEGORY"] == label, "UNIT_PRICE"].tolist() for label in order]
                fig, ax = plt.subplots(figsize=(8, 5), dpi=140)
                ax.boxplot(data, labels=order, patch_artist=True)
                for patch in ax.artists:
                    patch.set_facecolor("#55A868")
                    patch.set_alpha(0.7)
                ax.set_title("Unit Price Distribution by Category")
                ax.set_ylabel("Unit Price")
                ax.tick_params(axis="x", rotation=30)
                if formatter is not None:
                    ax.yaxis.set_major_formatter(formatter)
                ax.grid(True, axis="y", linestyle="--", alpha=0.3)
                fig.tight_layout()
                record_chart(fig, "overall_boxplot_by_category", "Unit Prices by Category", "Box-and-whisker comparison of unit price distributions grouped by category.")
        else:
            skipped.append("boxplot by category skipped (CATEGORY column unavailable)")
    except Exception as exc:  # pragma: no cover - defensive
        skipped.append(f"boxplot by category failed: {exc}")

    # Time trend chart ---------------------------------------------------------------
    try:
        if {"LETTING_DATE", "UNIT_PRICE"}.issubset(used_rows.columns):
            time_df = used_rows[["LETTING_DATE", "UNIT_PRICE", "REGION"]].copy() if "REGION" in used_rows.columns else used_rows[["LETTING_DATE", "UNIT_PRICE"]].copy()
            time_df["LETTING_DATE"] = pd.to_datetime(time_df["LETTING_DATE"], errors="coerce")
            time_df["UNIT_PRICE"] = _to_numeric(time_df["UNIT_PRICE"])  # type: ignore[index]
            time_df = time_df.dropna(subset=["LETTING_DATE", "UNIT_PRICE"])
            if time_df.empty:
                skipped.append("time trend skipped (no valid letting dates)")
            else:
                scatter_df = _downsample(time_df[["LETTING_DATE", "UNIT_PRICE"]])
                fig, ax = plt.subplots(figsize=(8, 5), dpi=140)
                ax.scatter(scatter_df["LETTING_DATE"], scatter_df["UNIT_PRICE"], s=14, alpha=0.25, color="#C44E52", label="Unit price samples")

                monthly = (
                    time_df
                    .groupby(time_df["LETTING_DATE"].dt.to_period("M"))["UNIT_PRICE"]
                    .median()
                    .dropna()
                )
                if not monthly.empty:
                    ax.plot(monthly.index.to_timestamp(), monthly.values, color="#4C72B0", linewidth=2.0, label="Statewide median")

                if "REGION" in time_df.columns:
                    reg_df = time_df.dropna(subset=["REGION"])
                    if not reg_df.empty:
                        top_regions = reg_df["REGION"].value_counts().head(6).index
                        for idx, region in enumerate(top_regions):
                            region_subset = reg_df.loc[reg_df["REGION"] == region]
                            monthly_region = (
                                region_subset
                                .groupby(region_subset["LETTING_DATE"].dt.to_period("M"))["UNIT_PRICE"]
                                .median()
                                .dropna()
                            )
                            if monthly_region.empty:
                                continue
                            color = plt.get_cmap("tab10")(idx % 10)
                            ax.plot(
                                monthly_region.index.to_timestamp(),
                                monthly_region.values,
                                linewidth=1.6,
                                label=f"Region {region} median",
                                color=color,
                            )

                ax.set_title("Unit Price Trends by Letting Month")
                ax.set_xlabel("Letting month")
                ax.set_ylabel("Unit Price")
                if formatter is not None:
                    ax.yaxis.set_major_formatter(formatter)
                ax.grid(True, linestyle="--", alpha=0.3)
                ax.legend(loc="upper left", frameon=False, fontsize="small")
                fig.autofmt_xdate()
                fig.tight_layout()
                record_chart(
                    fig,
                    "time_trend_state_vs_region",
                    "Unit Price Trends",
                    "Scatter of individual letting observations with statewide and regional monthly medians.",
                )
        else:
            skipped.append("time trend skipped (LETTING_DATE or UNIT_PRICE missing)")
    except Exception as exc:  # pragma: no cover - defensive
        skipped.append(f"time trend failed: {exc}")

    # Top-N items by data points ------------------------------------------------------
    try:
        if "ITEM_CODE" in df.columns and "DATA_POINTS_USED" in df.columns:
            items = df[["ITEM_CODE", "DATA_POINTS_USED"]].copy()
            items["DATA_POINTS_USED"] = pd.to_numeric(items["DATA_POINTS_USED"], errors="coerce").fillna(0)
            items = items.sort_values("DATA_POINTS_USED", ascending=False).head(max(1, top_n_items))
            items = items.loc[items["DATA_POINTS_USED"] > 0]
            if items.empty:
                skipped.append("top items chart skipped (no data points recorded)")
            else:
                fig, ax = plt.subplots(figsize=(8, 5), dpi=140)
                ax.barh(items["ITEM_CODE"].astype(str)[::-1], items["DATA_POINTS_USED"][::-1], color="#8172B3")
                ax.set_xlabel("Data points used")
                ax.set_ylabel("Item code")
                ax.set_title("Top Items by BidTabs Data Points Used")
                ax.grid(True, axis="x", linestyle="--", alpha=0.3)
                fig.tight_layout()
                record_chart(fig, "top_items_data_points", "Top Items by Data Points", "Items ranked by historical BidTabs observations used in pricing.")
        else:
            skipped.append("top items chart skipped (required columns missing)")
    except Exception as exc:  # pragma: no cover - defensive
        skipped.append(f"top items chart failed: {exc}")

    # Per-item distributions ---------------------------------------------------------
    try:
        if "ITEM_CODE" in df.columns:
            if "EXTENDED" in df.columns and df["EXTENDED"].notna().any():
                metric_col = pd.to_numeric(df["EXTENDED"], errors="coerce").fillna(0)
            else:
                metric_col = pd.to_numeric(df.get("DATA_POINTS_USED", pd.Series(dtype=float)), errors="coerce").fillna(0)
            tmp = pd.DataFrame({"ITEM_CODE": df["ITEM_CODE"], "METRIC": metric_col})
            tmp = tmp.sort_values("METRIC", ascending=False).head(max(1, top_n_items))
            ranking: Sequence[str] = tmp["ITEM_CODE"].astype(str).tolist()
            unit_price_lookup = pd.to_numeric(df.set_index("ITEM_CODE")["UNIT_PRICE_EST"], errors="coerce")
            for item_code in ranking:
                detail = payitem_details.get(item_code)
                if detail is None or detail.empty:
                    skipped.append(f"per-item chart skipped for {item_code} (no detail data)")
                    continue
                subset = detail.copy()
                if "USED_FOR_PRICING" in subset.columns:
                    subset = subset.loc[_as_bool_mask(subset["USED_FOR_PRICING"])]
                prices = _to_numeric(subset.get("UNIT_PRICE", pd.Series(dtype=float))).dropna()
                prices = prices[prices > 0]
                if prices.empty:
                    skipped.append(f"per-item chart skipped for {item_code} (no valid unit prices)")
                    continue
                est_price = float(unit_price_lookup.get(item_code, float("nan")))
                fig, ax = plt.subplots(figsize=(8, 5), dpi=140)
                ax.hist(prices, bins=min(30, max(6, int(math.sqrt(len(prices))))), color="#64B5CD", edgecolor="white", alpha=0.85)
                if not math.isnan(est_price) and est_price > 0:
                    ax.axvline(est_price, color="#C44E52", linestyle="--", linewidth=2, label=f"Estimate ${est_price:,.2f}")
                    ax.legend(frameon=False)
                ax.set_title(f"Item {item_code} Unit Price Distribution")
                ax.set_xlabel("Unit Price")
                ax.set_ylabel("Frequency")
                if formatter is not None:
                    ax.xaxis.set_major_formatter(formatter)
                ax.grid(True, axis="y", linestyle="--", alpha=0.3)
                fig.tight_layout()
                safe_code = _sanitize_item_code(str(item_code))
                record_chart(
                    fig,
                    f"item_{safe_code}_hist",
                    f"Item {item_code} Distribution",
                    "Distribution of BidTabs unit prices used for this item with the estimator's selected price highlighted.",
                )
        else:
            skipped.append("per-item distributions skipped (ITEM_CODE column missing)")
    except Exception as exc:  # pragma: no cover - defensive
        skipped.append(f"per-item distributions failed: {exc}")

    # Bundle PDF ---------------------------------------------------------------------
    pdf_path: Optional[Path] = None
    if bundle_pdf and pdf_entries:
        try:
            pdf_path = target_dir / "BidTabs_Visual_Summary.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=landscape(letter))
            page_width, page_height = landscape(letter)
            margin = 36
            text_width = page_width - 2 * margin
            image_height = page_height - 2 * margin - 32
            for entry in pdf_entries:
                c.setFont("Helvetica-Bold", 16)
                c.drawString(margin, page_height - margin + 4, entry.title)
                image = ImageReader(io.BytesIO(entry.image_bytes))
                img_width, img_height = image.getSize()
                scale = min(text_width / img_width, image_height / img_height)
                draw_width = img_width * scale
                draw_height = img_height * scale
                x = (page_width - draw_width) / 2
                y = margin + 24
                c.drawImage(image, x, y, width=draw_width, height=draw_height, preserveAspectRatio=True, mask="auto")
                c.setFont("Helvetica", 10)
                caption_lines = textwrap.wrap(entry.caption, width=110) or [entry.caption]
                text_y = margin
                for line in caption_lines:
                    c.drawString(margin, text_y, line)
                    text_y -= 12
                c.showPage()
            c.save()
        except Exception as exc:  # pragma: no cover - defensive
            skipped.append(f"failed to build summary PDF: {exc}")
            pdf_path = None

    return {
        "charts": [str(path) for path in charts],
        "pdf": str(pdf_path) if pdf_path else None,
        "skipped": skipped,
    }

