from __future__ import annotations

import pandas as pd


def iqr_outlier_mask(series: pd.Series, k: float = 1.5) -> pd.Series:
    """Marca valores fora de [Q1 - k*IQR, Q3 + k*IQR] (regra clássica de boxplot)."""
    s = series.dropna()
    if s.empty:
        return pd.Series(False, index=series.index)
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return pd.Series(False, index=series.index)
    low = q1 - k * iqr
    high = q3 + k * iqr
    return (series < low) | (series > high)


def mark_outliers_iqr(df: pd.DataFrame, group_col: str, value_col: str, k: float = 1.5) -> pd.DataFrame:
    """Adiciona coluna `is_outlier` por grupo (ex.: uma série temporal por commodity)."""
    out = df.copy()
    out["is_outlier"] = False
    for _, grp in df.groupby(group_col):
        idx = grp.index
        out.loc[idx, "is_outlier"] = iqr_outlier_mask(grp[value_col], k=k).values
    return out


def descriptive_price_stats(df: pd.DataFrame, value_col: str = "price_value") -> pd.Series:
    """Estatísticas descritivas para a coluna numérica indicada."""
    s = df[value_col].dropna()
    if s.empty:
        return pd.Series(
            {"count": 0, "mean": float("nan"), "median": float("nan"), "std": float("nan"), "min": float("nan"), "max": float("nan")}
        )
    return pd.Series(
        {
            "count": int(s.count()),
            "mean": float(s.mean()),
            "median": float(s.median()),
            "std": float(s.std(ddof=1)) if len(s) > 1 else 0.0,
            "min": float(s.min()),
            "max": float(s.max()),
        }
    )
