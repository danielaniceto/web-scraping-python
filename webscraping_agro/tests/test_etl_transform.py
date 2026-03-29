from pathlib import Path

import pandas as pd

from src.etl.transform_load import transform_raw_to_processed


def test_transform_raw_to_processed_standardizes_types_and_categories(tmp_path: Path) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "commodity_name": "Food Price Index",
                "price_text": "123.4",
                "ref_period": "2024-01",
                "currency": "INDEX_POINTS",
                "unit": "index_points",
                "source_name": "fao_food_price_index",
                "source_url": "https://example.com",
                "record_id": "REC_1",
            },
            {
                "commodity_name": "unknown item",
                "price_text": None,
                "ref_period": "2024-02",
                "currency": None,
                "unit": None,
                "source_name": "fao_food_price_index",
                "source_url": "https://example.com",
                "record_id": "REC_2",
            },
        ]
    )
    raw_path = tmp_path / "records.parquet"
    raw_df.to_parquet(raw_path, index=False)

    transformed = transform_raw_to_processed(raw_path)

    assert list(transformed.columns) == [
        "commodity_code",
        "region_code",
        "price_month",
        "price_value",
        "currency_code",
        "unit",
        "source_system",
        "source_url",
        "source_record_id",
    ]
    assert transformed.loc[0, "commodity_code"] == "FOOD_PRICE_INDEX"
    assert transformed.loc[1, "commodity_code"] == "UNKNOWN"
    assert transformed.loc[1, "price_value"] == 0.0
    assert transformed.loc[0, "region_code"] == "GLOBAL"
