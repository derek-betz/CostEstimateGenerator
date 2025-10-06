import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MODE = 'WGT_AVG'
PROJECT_REGION = os.getenv('PROJECT_REGION', '').strip()
PROJECT_REGION = int(PROJECT_REGION) if PROJECT_REGION else None
MIN_SAMPLE_TARGET = int(os.getenv('MIN_SAMPLE_TARGET', '50'))

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
    else:
        price = float(df['UNIT_PRICE'].median())

    return price, int(len(df))


def _compute_categories(
    bidtabs: pd.DataFrame,
    item_code: str,
    project_region: int | None,
    collect_details: bool = False,
    target_quantity: float | None = None,
):
    pool = _prepare_pool(bidtabs, item_code)

    if target_quantity is not None and target_quantity > 0 and 'QUANTITY' in pool.columns:
        lower_q = 0.5 * float(target_quantity)
        upper_q = 1.5 * float(target_quantity)
        pool = pool.loc[pool['QUANTITY'].between(lower_q, upper_q, inclusive='both')].copy()

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
        if 'UNIT_PRICE' in cleaned.columns and cleaned['UNIT_PRICE'].notna().sum() >= 3:
            prices = cleaned['UNIT_PRICE'].astype(float)
            mean = prices.mean()
            std = prices.std(ddof=0)
            if std > 0:
                mask = (prices >= mean - 2 * std) & (prices <= mean + 2 * std)
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
):
    region = PROJECT_REGION if project_region is None else project_region
    price, source, cat_data, detail_map, used_categories, combined_detail = _compute_categories(
        bidtabs, item_code, region, collect_details=include_details, target_quantity=target_quantity
    )
    if include_details:
        return price, source, cat_data, detail_map, used_categories, combined_detail
    return price, source, cat_data
