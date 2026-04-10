import json
from itertools import combinations
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

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

COUNTRY_NORMALISATION_MAP = {
    "United States of America": "USA",
    "United Kingdon": "UK",
    "Great Britain": "UK",
    "United Kingdom of Great Britain and Northern Ireland": "UK",
    "United Kingdom": "UK",
    "Russian Federation": "Russia",
    "Micronesia (Federated States of)": "Micronesia",
}
PROJECT_URL_PREFIX = "https://energy-poverty.ec.europa.eu/node/"


def _load_jsonl_lines(lines: list[str]) -> list[dict]:
    records: list[dict] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            records.append(parsed)
        else:
            raise ValueError("Each JSONL line must contain a JSON object.")
    return records


def _decode_text(raw: bytes | str) -> str:
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


def load_json(file) -> list[dict]:
    """Load raw JSON or JSONL from a file-like object or path."""
    suffix = ""

    if hasattr(file, "read"):
        raw_content = file.read()
        text = _decode_text(raw_content)
        if hasattr(file, "name"):
            suffix = Path(file.name).suffix.lower()
    else:
        path = Path(file)
        suffix = path.suffix.lower()
        text = path.read_text(encoding="utf-8")

    if suffix == ".jsonl":
        return _load_jsonl_lines(text.splitlines())

    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return _load_jsonl_lines(text.splitlines())

    return raw if isinstance(raw, list) else [raw]


def _split_field(val: str | None) -> list[str]:
    if not val or pd.isna(val):
        return []
    return [v.strip() for v in str(val).split(";") if v.strip()]


def _normalise_countries(values: list[str]) -> list[str]:
    return [COUNTRY_NORMALISATION_MAP.get(value, value) for value in values]


def _build_project_url(atlas_id: Any) -> str | None:
    if atlas_id is None or pd.isna(atlas_id):
        return None
    atlas_id_str = str(atlas_id).strip()
    if not atlas_id_str:
        return None
    return f"{PROJECT_URL_PREFIX}{atlas_id_str}"


def normalise(raw: list[dict]) -> pd.DataFrame:
    """Flatten and normalise raw records into a clean DataFrame."""
    df = pd.DataFrame(raw)

    if "project_url" not in df.columns:
        df["project_url"] = None
    if "atlas_id" in df.columns:
        missing_project_url = df["project_url"].isna()
        if df["project_url"].dtype == object:
            missing_project_url = missing_project_url | (df["project_url"].fillna("").str.strip() == "")
        df.loc[missing_project_url, "project_url"] = df.loc[missing_project_url, "atlas_id"].apply(_build_project_url)

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

    if "countries_impacted" in df.columns:
        df["countries_impacted_list"] = df["countries_impacted_list"].apply(_normalise_countries)

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
