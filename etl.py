from typing import Any

import pandas as pd
import json
from itertools import combinations
from collections import defaultdict

try:
    from .config import MAX_AGE_DAYS
except ImportError:
    from config import MAX_AGE_DAYS


MULTI_VALUE_FIELDS = [
    "countries_impacted",
    "energy_poverty_phase",
    "intervention_type",
    "professionals_involved",
    "partners_involved",
]


def load_json(file) -> list[dict]:
    """Load raw JSON from a file-like object or path."""
    if hasattr(file, "read"):
        raw = json.load(file)
    else:
        with open(file, "r", encoding="utf-8") as f:
            raw = json.load(f)
    return raw if isinstance(raw, list) else [raw]


def _split_field(val: str | None) -> list[str]:
    if not val or pd.isna(val):
        return []
    return [v.strip() for v in str(val).split(";") if v.strip()]


def normalise(raw: list[dict]) -> pd.DataFrame:
    """Flatten and normalise raw records into a clean DataFrame."""
    df = pd.DataFrame(raw)

    # Deduplicate
    df = df.drop_duplicates(subset=["project_url"])

    # Parse datetime
    df["parsed_at"] = pd.to_datetime(df["parsed_at"], utc=True, errors="coerce")

    # Strip whitespace from string cols
    str_cols = df.select_dtypes(include="object").columns
    for c in str_cols:
        df[c] = df[c].str.strip()

    # Normalise multi-value fields into Python lists
    for field in MULTI_VALUE_FIELDS:
        if field in df.columns:
            df[field + "_list"] = df[field].apply(_split_field)

    # Derived: country count per project
    df["country_count"] = df["countries_impacted_list"].apply(len)

    # Derived: intervention count per project
    df["intervention_count"] = df["intervention_type_list"].apply(len)

    # Derived: phase count per project
    df["phase_count"] = df["energy_poverty_phase_list"].apply(len)

    return df.reset_index(drop=True)


def explode_field(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Return a long-form DataFrame exploded on a list field."""
    col = field + "_list"
    if col not in df.columns:
        return pd.DataFrame()
    tmp = df[["project_title", col]].copy()
    tmp = tmp.explode(col).rename(columns={col: field})
    return tmp[tmp[field].notna() & (tmp[field] != "")]


def cooccurrence_matrix(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Build a co-occurrence matrix for a multi-value field across projects."""
    col = field + "_list"
    if col not in df.columns:
        return pd.DataFrame()

    counts = defaultdict(int)
    all_vals = set()

    for vals in df[col]:
        vals = [v for v in vals if v]
        all_vals.update(vals)
        for a, b in combinations(sorted(vals), 2):
            counts[(a, b)] += 1
        for v in vals:
            counts[(v, v)] += 1  # diagonal = total occurrences

    labels = sorted(all_vals)
    matrix = pd.DataFrame(0, index=labels, columns=labels)
    for (a, b), cnt in counts.items():
        matrix.loc[a, b] = cnt
        if a != b:
            matrix.loc[b, a] = cnt

    return matrix


def cross_field_cooccurrence(df: pd.DataFrame, field_a: str, field_b: str) -> pd.DataFrame:
    """Cross co-occurrence between two multi-value fields."""
    col_a = field_a + "_list"
    col_b = field_b + "_list"
    if col_a not in df.columns or col_b not in df.columns:
        return pd.DataFrame()

    counts = defaultdict(int)
    all_a, all_b = set(), set()

    for _, row in df.iterrows():
        vals_a = [v for v in row[col_a] if v]
        vals_b = [v for v in row[col_b] if v]
        all_a.update(vals_a)
        all_b.update(vals_b)
        for a in vals_a:
            for b in vals_b:
                counts[(a, b)] += 1

    if not all_a or not all_b:
        return pd.DataFrame()

    matrix = pd.DataFrame(0, index=sorted(all_a), columns=sorted(all_b))
    for (a, b), cnt in counts.items():
        matrix.loc[a, b] = cnt

    return matrix


def data_quality_report(df: pd.DataFrame) -> pd.DataFrame:
    """Return a summary of nulls and fill rates per column."""
    total = len(df)
    report = []
    for col in df.columns:
        if col.endswith("_list"):
            continue
        null_count = df[col].isna().sum()
        empty_count = (df[col] == "").sum() if df[col].dtype == object else 0
        missing = null_count + empty_count
        report.append({
            "field": col,
            "total": total,
            "missing": int(missing),
            "fill_rate_%": round(100 * (total - missing) / total, 1),
        })
    return pd.DataFrame(report)


def run_controlled_stage_one() -> list[dict[str, Any]]:
    """Run parser stage one only when the previous output is older than MAX_AGE_DAYS."""
    try:
        from .parser import runStageOneWithControl
    except ImportError:
        from parser import runStageOneWithControl
    return runStageOneWithControl(MAX_AGE_DAYS * 24 * 60 * 60)


if __name__ == "__main__":
    run_controlled_stage_one()
