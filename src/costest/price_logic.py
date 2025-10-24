import os
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Aggregation method for category pricing. Supported:
#  - WGT_AVG (default)
#  - MEAN / AVG
#  - MEDIAN / P50
#  - P40_P60 (average of 40th and 60th percentiles)
#  - TRIMMED_MEAN_P10_P90 (mean of prices between 10th and 90th percentiles)
#  - ROBUST_MEDIAN (alias of MEDIAN; kept for readability)
MODE = os.getenv('AGGREGATE_METHOD', 'WGT_AVG').upper().strip()
PROJECT_REGION = os.getenv('PROJECT_REGION', '').strip()
PROJECT_REGION = int(PROJECT_REGION) if PROJECT_REGION else None
MIN_SAMPLE_TARGET = int(os.getenv('MIN_SAMPLE_TARGET', '50'))
ROLLUP_QUANTITY_LOWER = float(os.getenv('MEMO_ROLLUP_QUANTITY_LOWER', '0.5'))
ROLLUP_QUANTITY_UPPER = float(os.getenv('MEMO_ROLLUP_QUANTITY_UPPER', '1.5'))
ROLLUP_SIGMA_THRESHOLD = float(os.getenv('MEMO_ROLLUP_SIGMA_THRESHOLD', '2.0'))
CATEGORY_SIGMA_THRESHOLD = float(os.getenv('CATEGORY_SIGMA_THRESHOLD', '2.0'))
QUANTITY_FILTER_MIN_POINTS = int(os.getenv('QUANTITY_FILTER_MIN_POINTS', '10'))
PRIMARY_QUANTITY_BAND = (0.5, 1.5)
EXPANDED_QUANTITY_BAND = (PRIMARY_QUANTITY_BAND[0], 2.0)

# Optional experimental quantity elasticity adjustment
ENABLE_QUANTITY_ELASTICITY = os.getenv('ENABLE_QUANTITY_ELASTICITY', '0').strip() in {'1', 'true', 'on', 'yes'}

CATEGORY_DEFS = [
    ('DIST_12M', 'REGION', 0, 12),
    ('DIST_24M', 'REGION', 12, 24),
    ('DIST_36M', 'REGION', 24, 36),
    ('STATE_12M', 'STATE', 0, 12),
    ('STATE_24M', 'STATE', 12, 24),
    ('STATE_36M', 'STATE', 24, 36),
]


def _prepare_pool(bidtabs: pd.DataFrame, item_code: str) -> pd.DataFrame:
    pool = bidtabs.loc[bidtabs['ITEM_CODE'].astype(str) == str(item_code)].copy()
    if pool.empty:
        return pool

    if 'UNIT_PRICE' in pool.columns:
        pool['UNIT_PRICE'] = pd.to_numeric(pool['UNIT_PRICE'], errors='coerce')
        pool = pool.dropna(subset=['UNIT_PRICE'])

    if 'WEIGHT' in pool.columns:
        pool['WEIGHT'] = pd.to_numeric(pool['WEIGHT'], errors='coerce')

    if 'JOB_SIZE' in pool.columns:
        pool['JOB_SIZE'] = pd.to_numeric(pool['JOB_SIZE'], errors='coerce')

    if 'LETTING_DATE' in pool.columns:
        pool['_LET_DT'] = pd.to_datetime(pool['LETTING_DATE'], errors='coerce')
    else:
        pool['_LET_DT'] = pd.NaT

    return pool


def get_pool_for_codes(bidtabs: pd.DataFrame, codes: Sequence[str]) -> pd.DataFrame:
    """
    Retrieve a combined BidTabs pool for a collection of item codes.

    Parameters
    ----------
    bidtabs:
        Source BidTabs dataframe.
    codes:
        Iterable of pay item codes to include.

    Returns
    -------
    DataFrame
        Concatenated BidTabs rows for the requested codes.  Includes an
        auxiliary ``_SOURCE_ITEM_CODE`` column noting the originating code.
    """

    frames: List[pd.DataFrame] = []
    for code in codes:
        prepared = _prepare_pool(bidtabs, code)
        if prepared.empty:
            continue
        block = prepared.copy()
        block['_SOURCE_ITEM_CODE'] = str(code)
        frames.append(block)
    if not frames:
        columns = list(bidtabs.columns)
        if '_SOURCE_ITEM_CODE' not in columns:
            columns.append('_SOURCE_ITEM_CODE')
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True, sort=False)


def prepare_memo_rollup_pool(
    bidtabs: pd.DataFrame,
    codes: Sequence[str],
    project_region: Optional[int] = None,
    target_quantity: Optional[float] = None,
) -> pd.DataFrame:
    """
    Build a filtered pool for design memo rollups.

    Filtering steps:
    - project region (if provided)
    - quantity banding relative to target quantity (0.5x - 1.5x)
    - ±2σ unit price trimming when enough observations exist
    """

    pool = get_pool_for_codes(bidtabs, codes)
    if pool.empty:
        pool.attrs["quantity_filter_attempted_bounds"] = None
        pool.attrs["quantity_filter_applied"] = False
        pool.attrs["quantity_filter_relaxed"] = False
        return pool

    out = pool.copy()
    quantity_bounds: Optional[Tuple[float, float]] = None
    quantity_filter_applied = False
    quantity_filter_relaxed = False

    if project_region is not None and 'REGION' in out.columns:
        region_series = pd.to_numeric(out['REGION'], errors='coerce')
        out = out.loc[region_series == project_region].copy()
        out['REGION'] = region_series
    if target_quantity is not None and target_quantity > 0 and 'QUANTITY' in out.columns:
        lower = ROLLUP_QUANTITY_LOWER * float(target_quantity)
        upper = ROLLUP_QUANTITY_UPPER * float(target_quantity)
        quantity_bounds = (lower, upper)
        qty_series = pd.to_numeric(out['QUANTITY'], errors='coerce')
        mask = qty_series.between(lower, upper, inclusive='both')
        filtered = out.loc[mask].copy()
        if not filtered.empty:
            filtered['QUANTITY'] = qty_series.loc[filtered.index]
            out = filtered
            quantity_filter_applied = True
        elif len(out) > 0:
            out = out.copy()
            out['QUANTITY'] = qty_series.loc[out.index]
            quantity_filter_relaxed = True
        else:
            out = filtered
    elif 'QUANTITY' in out.columns:
        out['QUANTITY'] = pd.to_numeric(out['QUANTITY'], errors='coerce')

    out.attrs["quantity_filter_attempted_bounds"] = quantity_bounds
    out.attrs["quantity_filter_applied"] = quantity_filter_applied
    out.attrs["quantity_filter_relaxed"] = quantity_filter_relaxed
    if 'QUANTITY' in out.columns:
        out['QUANTITY_FILTER_RELAXED'] = quantity_filter_relaxed

    if 'UNIT_PRICE' not in out.columns:
        return out.iloc[0:0]

    out['UNIT_PRICE'] = pd.to_numeric(out['UNIT_PRICE'], errors='coerce')
    out.dropna(subset=['UNIT_PRICE'], inplace=True)

    for weight_col in ('WEIGHT', 'QUANTITY', 'JOB_SIZE'):
        if weight_col in out.columns:
            out[weight_col] = pd.to_numeric(out[weight_col], errors='coerce')

    if 'LETTING_DATE' in out.columns and '_LET_DT' not in out.columns:
        out['_LET_DT'] = pd.to_datetime(out['LETTING_DATE'], errors='coerce')

    if len(out) >= 5 and ROLLUP_SIGMA_THRESHOLD > 0:
        prices = out['UNIT_PRICE'].astype(float)
        mean = prices.mean()
        std = prices.std(ddof=0)
        if std > 0:
            threshold = float(ROLLUP_SIGMA_THRESHOLD)
            mask = (prices >= mean - threshold * std) & (prices <= mean + threshold * std)
            out = out.loc[mask].copy()

    return out


def memo_rollup_price(
    bidtabs: pd.DataFrame,
    replacement_code: str,
    obsolete_codes: Sequence[str],
    project_region: Optional[int] = None,
    target_quantity: Optional[float] = None,
) -> tuple[float, int, str]:
    """
    Aggregate a pooled price for a replacement item by rolling up obsolete codes.

    Returns a tuple of (price, observation count, source string).
    """

    pool = prepare_memo_rollup_pool(
        bidtabs,
        obsolete_codes,
        project_region=project_region,
        target_quantity=target_quantity,
    )
    if pool.empty:
        source_label = f"DESIGN_MEMO_ROLLUP:{replacement_code}"
        return float('nan'), 0, source_label

    prices = pool['UNIT_PRICE'].astype(float).to_numpy()
    weight_source = None
    weights = None
    for col in ('WEIGHT', 'QUANTITY', 'JOB_SIZE'):
        if col in pool.columns and not pool[col].fillna(0).eq(0).all():
            weight_source = col
            weights = pool[col].fillna(0).astype(float).to_numpy()
            if np.isclose(weights.sum(), 0.0):
                weights = None
            break
    if weights is not None and weights.size == prices.size:
        price = float(np.average(prices, weights=weights))
    else:
        price = float(prices.mean())
    count = int(len(prices))
    codes = "+".join(str(code) for code in obsolete_codes)
    suffix = "|qty_relaxed" if pool.attrs.get("quantity_filter_relaxed") else ""
    if weight_source is not None:
        source_label = f"DESIGN_MEMO_ROLLUP:{replacement_code}[w={weight_source}]<-{codes}{suffix}"
    else:
        source_label = f"DESIGN_MEMO_ROLLUP:{replacement_code}<-{codes}{suffix}"
    return price, count, source_label


def _filter_window(
    df: pd.DataFrame,
    min_months: int | None,
    max_months: int | None,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    if '_LET_DT' not in out.columns:
        out['_LET_DT'] = pd.to_datetime(out.get('LETTING_DATE'), errors='coerce')

    dt = out['_LET_DT']
    mask = pd.Series(False, index=out.index)
    valid = dt.notna()

    if valid.any():
        criteria = pd.Series(True, index=dt.index[valid])
        now = pd.Timestamp.today()
        if max_months is not None:
            lower_bound = now - pd.DateOffset(months=max_months)
            criteria &= dt.loc[valid] >= lower_bound
        if min_months is not None:
            upper_bound = now - pd.DateOffset(months=min_months)
            if min_months == 0:
                criteria &= dt.loc[valid] <= upper_bound
            else:
                criteria &= dt.loc[valid] < upper_bound
        mask.loc[valid] = criteria

    if min_months == 0:
        mask |= dt.isna()

    return out.loc[mask].copy()


def _aggregate_price(df: pd.DataFrame) -> tuple[float, int]:
    if df.empty:
        return np.nan, 0

    if MODE == 'WGT_AVG' and 'WEIGHT' in df.columns and not df['WEIGHT'].isna().all():
        weights = df['WEIGHT'].fillna(1.0).astype(float)
        price = float(np.average(df['UNIT_PRICE'], weights=weights))
    elif MODE in ('MEAN', 'AVG'):
        price = float(df['UNIT_PRICE'].mean())
    elif MODE in ('MEDIAN', 'P50'):
        price = float(df['UNIT_PRICE'].median())
    elif MODE == 'P40_P60':
        p40 = df['UNIT_PRICE'].quantile(0.40)
        p60 = df['UNIT_PRICE'].quantile(0.60)
        price = float((p40 + p60) / 2)
    elif MODE == 'TRIMMED_MEAN_P10_P90':
        lower = df['UNIT_PRICE'].quantile(0.10)
        upper = df['UNIT_PRICE'].quantile(0.90)
        trimmed = df.loc[df['UNIT_PRICE'].between(lower, upper, inclusive='both')]
        price = float(trimmed['UNIT_PRICE'].mean()) if not trimmed.empty else float(df['UNIT_PRICE'].mean())
    elif MODE == 'ROBUST_MEDIAN':
        price = float(df['UNIT_PRICE'].median())
    else:
        price = float(df['UNIT_PRICE'].median())

    return price, int(len(df))


def _compute_categories(
    bidtabs: pd.DataFrame,
    item_code: str,
    project_region: int | None,
    collect_details: bool = False,
    target_quantity: float | None = None,
    quantity_band: tuple[float, float] | None = PRIMARY_QUANTITY_BAND,
):
    pool = _prepare_pool(bidtabs, item_code)

    applied_quantity_band: tuple[float, float] | None = None
    if (
        target_quantity is not None
        and target_quantity > 0
        and 'QUANTITY' in pool.columns
        and quantity_band is not None
    ):
        lower_multiplier, upper_multiplier = quantity_band
        lower_q = lower_multiplier * float(target_quantity)
        upper_q = upper_multiplier * float(target_quantity)
        pool = pool.loc[pool['QUANTITY'].between(lower_q, upper_q, inclusive='both')].copy()
        applied_quantity_band = (lower_multiplier, upper_multiplier)

    results: dict[str, float] = {}
    subsets: dict[str, pd.DataFrame] = {}

    for name, scope, min_months, max_months in CATEGORY_DEFS:
        subset = _filter_window(pool, min_months, max_months)

        if scope == 'REGION':
            if project_region is None or 'REGION' not in subset.columns:
                subset = subset.iloc[0:0]
            else:
                subset = subset.loc[subset['REGION'] == project_region]

        subset = subset.copy()
        subset['_AUDIT_ROW_ID'] = subset.index

        if subset.empty:
            results[f'{name}_PRICE'] = np.nan
            results[f'{name}_COUNT'] = 0
            subsets[name] = subset
            continue

        cleaned = subset.copy()
        if (
            'UNIT_PRICE' in cleaned.columns
            and cleaned['UNIT_PRICE'].notna().sum() >= 3
            and CATEGORY_SIGMA_THRESHOLD > 0
        ):
            prices = cleaned['UNIT_PRICE'].astype(float)
            mean = prices.mean()
            std = prices.std(ddof=0)
            if std > 0:
                t = float(CATEGORY_SIGMA_THRESHOLD)
                mask = (prices >= mean - t * std) & (prices <= mean + t * std)
                cleaned = cleaned.loc[mask]

        if cleaned.empty:
            results[f'{name}_PRICE'] = np.nan
            results[f'{name}_COUNT'] = 0
            subsets[name] = cleaned
            continue

        price, count = _aggregate_price(cleaned)
        results[f'{name}_PRICE'] = price if count > 0 else np.nan
        results[f'{name}_COUNT'] = count if count > 0 else 0

        subsets[name] = cleaned

    combined_frames: list[pd.DataFrame] = []
    used_categories: list[str] = []
    seen_ids: set = set()

    for name, _, _, _ in CATEGORY_DEFS:
        subset = subsets.get(name)
        if subset is None or subset.empty:
            continue

        new_rows = subset.loc[~subset['_AUDIT_ROW_ID'].isin(seen_ids)].copy()
        if new_rows.empty:
            continue

        combined_frames.append(new_rows)
        used_categories.append(name)
        seen_ids.update(new_rows['_AUDIT_ROW_ID'].tolist())

        if len(seen_ids) >= MIN_SAMPLE_TARGET:
            break

    if combined_frames:
        combined_detail = pd.concat(combined_frames, ignore_index=False)
        final_price, _ = _aggregate_price(combined_detail)
        total_used = int(len(combined_detail))
        source = used_categories[-1]
    else:
        combined_detail = pd.DataFrame(columns=pool.columns)
        final_price = np.nan
        source = 'NO_DATA'
        total_used = 0

    results['TOTAL_USED_COUNT'] = total_used
    if applied_quantity_band is not None:
        lower_mult, upper_mult = applied_quantity_band
        results['QUANTITY_FILTER_LOWER_MULTIPLIER'] = float(lower_mult)
        results['QUANTITY_FILTER_UPPER_MULTIPLIER'] = float(upper_mult)
    elif target_quantity is not None:
        results['QUANTITY_FILTER_LOWER_MULTIPLIER'] = np.nan
        results['QUANTITY_FILTER_UPPER_MULTIPLIER'] = np.nan
    detail_map = subsets if collect_details else {}

    return final_price, source, results, detail_map, used_categories, combined_detail


def pick_price(bidtabs: pd.DataFrame, item_code: str) -> tuple[float, str]:
    price, source, *_ = _compute_categories(bidtabs, item_code, PROJECT_REGION)
    return price, source


def category_breakdown(
    bidtabs: pd.DataFrame,
    item_code: str,
    project_region: int | None = None,
    include_details: bool = False,
    target_quantity: float | None = None,
) -> tuple[float, str, dict[str, object]] | tuple[float, str, dict[str, object], dict[str, pd.DataFrame], list[str], pd.DataFrame]:
    """Compute category-based pricing statistics for ``item_code``.

    The input dataframe must contain the canonical BidTabs columns such as
    ``ITEM_CODE``, ``UNIT_PRICE``, and category aggregates (``DIST_*``/``STATE_*``).
    When ``include_details`` is ``True`` the function returns the supplemental
    detail map and combined pool dataframe used to derive pricing.
    """
    region = PROJECT_REGION if project_region is None else project_region
    price, source, cat_data, detail_map, used_categories, combined_detail = _compute_categories(
        bidtabs,
        item_code,
        region,
        collect_details=include_details,
        target_quantity=target_quantity,
        quantity_band=PRIMARY_QUANTITY_BAND,
    )

    total_used_primary = int(cat_data.get("TOTAL_USED_COUNT", len(combined_detail)))
    lower_primary = cat_data.get("QUANTITY_FILTER_LOWER_MULTIPLIER")
    upper_primary = cat_data.get("QUANTITY_FILTER_UPPER_MULTIPLIER")
    has_primary_band = (
        lower_primary is not None
        and upper_primary is not None
        and np.isfinite(lower_primary)
        and np.isfinite(upper_primary)
    )

    if target_quantity is not None and target_quantity > 0:
        if total_used_primary < QUANTITY_FILTER_MIN_POINTS and has_primary_band:
            price, source, cat_data, detail_map, used_categories, combined_detail = _compute_categories(
                bidtabs,
                item_code,
                region,
                collect_details=include_details,
                target_quantity=target_quantity,
                quantity_band=EXPANDED_QUANTITY_BAND,
            )
            cat_data["QUANTITY_FILTER_BASE_COUNT"] = float(total_used_primary)
            cat_data["QUANTITY_FILTER_WAS_EXPANDED"] = True
        else:
            cat_data["QUANTITY_FILTER_BASE_COUNT"] = float(total_used_primary)
            cat_data["QUANTITY_FILTER_WAS_EXPANDED"] = False
            if "QUANTITY_FILTER_LOWER_MULTIPLIER" not in cat_data:
                cat_data["QUANTITY_FILTER_LOWER_MULTIPLIER"] = np.nan
                cat_data["QUANTITY_FILTER_UPPER_MULTIPLIER"] = np.nan

    # Optional quantity elasticity adjustment (experimental; disabled by default)
    if ENABLE_QUANTITY_ELASTICITY and target_quantity and target_quantity > 0 and not combined_detail.empty:
        try:
            sub = combined_detail.copy()
            q = pd.to_numeric(sub.get('QUANTITY'), errors='coerce').dropna()
            p = pd.to_numeric(sub.get('UNIT_PRICE'), errors='coerce').dropna()
            joined = pd.DataFrame({'Q': q, 'P': p}).dropna()
            if len(joined) >= 15 and joined['Q'].gt(0).all() and joined['P'].gt(0).all():
                # log-log slope (elasticity)
                x = np.log(joined['Q'].to_numpy())
                y = np.log(joined['P'].to_numpy())
                slope, intercept = np.polyfit(x, y, 1)
                slope = float(np.clip(slope, -0.2, 0.2))
                median_q = float(np.median(joined['Q']))
                if median_q > 0 and np.isfinite(slope):
                    factor = (float(target_quantity) / median_q) ** slope
                    price = float(price) * float(np.clip(factor, 0.8, 1.2))
                    cat_data['QUANTITY_ELASTICITY_SLOPE'] = slope
                    cat_data['QUANTITY_ELASTICITY_APPLIED'] = True
        except Exception:
            cat_data['QUANTITY_ELASTICITY_APPLIED'] = False

    if include_details:
        return price, source, cat_data, detail_map, used_categories, combined_detail
    return price, source, cat_data


def compute_recency_factor(estimate_df: pd.DataFrame) -> float:
    """
    Estimate a recency adjustment based on STATE window ratios.

    Uses STATE_12M_PRICE as the recent anchor and compares against
    STATE_24M_PRICE and STATE_36M_PRICE when available.  Returns the
    median ratio, clamped to [0.9, 1.2].  If no ratios are available the
    factor defaults to 1.0.
    """

    ratios: List[float] = []
    if estimate_df is not None and not estimate_df.empty:
        recent = estimate_df.get('STATE_12M_PRICE')
        if recent is not None:
            recent = pd.to_numeric(recent, errors='coerce')
            for older_col in ('STATE_24M_PRICE', 'STATE_36M_PRICE'):
                older = estimate_df.get(older_col)
                if older is None:
                    continue
                older = pd.to_numeric(older, errors='coerce')
                mask = (recent > 0) & (older > 0)
                if mask.any():
                    series = (recent[mask] / older[mask]).replace([np.inf, -np.inf], np.nan).dropna()
                    ratios.extend(float(value) for value in series.tolist() if np.isfinite(value))
    if ratios:
        factor = float(np.median(ratios))
    else:
        factor = 1.0
    factor = float(np.clip(factor, 0.9, 1.2))
    compute_recency_factor.last_meta = {
        'samples': len(ratios),
        'used_default': len(ratios) == 0,
    }
    return factor


def compute_region_factor(estimate_df: pd.DataFrame, project_region: Optional[int] = None) -> float:
    """
    Estimate a locality adjustment comparing district vs. statewide pricing.

    Uses DIST_12M_PRICE divided by STATE_12M_PRICE when both are present.
    Returns the median ratio, clamped to [0.85, 1.15].  Defaults to 1.0
    if insufficient data exist.
    """

    ratios: List[float] = []
    if estimate_df is not None and not estimate_df.empty:
        dist = estimate_df.get('DIST_12M_PRICE')
        state = estimate_df.get('STATE_12M_PRICE')
        if dist is not None and state is not None:
            dist = pd.to_numeric(dist, errors='coerce')
            state = pd.to_numeric(state, errors='coerce')
            mask = (dist > 0) & (state > 0)
            if mask.any():
                series = (dist[mask] / state[mask]).replace([np.inf, -np.inf], np.nan).dropna()
                ratios.extend(float(value) for value in series.tolist() if np.isfinite(value))
    if ratios:
        factor = float(np.median(ratios))
    else:
        factor = 1.0
    factor = float(np.clip(factor, 0.85, 1.15))
    compute_region_factor.last_meta = {
        'samples': len(ratios),
        'used_default': len(ratios) == 0,
        'project_region': project_region,
    }
    return factor
