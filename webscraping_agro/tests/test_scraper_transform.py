from src.scraping.commodity_scraper import transform_raw_records


def test_transform_raw_records_normalizes_name_and_fields() -> None:
    raw_items = [
        {"name": "Soja", "symbol": "SJA", "price": "123.45", "currency": "BRL"},
        {"commodity": " milho ", "ticker": "MLH", "last": "88.70", "ccy": "USD"},
    ]

    records = transform_raw_records(raw_items)

    assert len(records) == 2
    assert records[0].commodity_name == "SOJA"
    assert records[0].symbol == "SJA"
    assert records[1].commodity_name == "MILHO"
    assert records[1].currency == "USD"
