import pandas as pd

from src.analysis.eda_tools import descriptive_price_stats, iqr_outlier_mask, mark_outliers_iqr


def test_iqr_outlier_mask_flags_extreme_values() -> None:
    s = pd.Series([10.0, 11.0, 12.0, 13.0, 100.0])
    mask = iqr_outlier_mask(s, k=1.5)
    assert bool(mask.iloc[-1])
    assert int(mask.iloc[:-1].sum()) == 0


def test_mark_outliers_iqr_per_group() -> None:
    df = pd.DataFrame(
        {
            "commodity_code": ["A", "A", "A", "B", "B", "B", "B", "B"],
            "price_value": [10.0, 11.0, 12.0, 1.0, 2.0, 3.0, 4.0, 100.0],
        }
    )
    out = mark_outliers_iqr(df, "commodity_code", "price_value")
    assert "is_outlier" in out.columns
    b_flags = out.loc[out["commodity_code"] == "B", "is_outlier"]
    assert bool(b_flags.iloc[-1])


def test_descriptive_price_stats() -> None:
    df = pd.DataFrame({"price_value": [1.0, 2.0, 3.0, 4.0]})
    stats = descriptive_price_stats(df)
    assert stats["count"] == 4
    assert stats["mean"] == 2.5
    assert stats["median"] == 2.5
    assert stats["min"] == 1.0
    assert stats["max"] == 4.0
