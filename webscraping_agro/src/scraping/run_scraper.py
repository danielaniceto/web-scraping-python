from __future__ import annotations

from src.scraping.commodity_scraper import fetch_commodities, save_raw, transform_raw_records


def main() -> None:
    raw_items = fetch_commodities()
    records = transform_raw_records(raw_items)
    csv_path, json_path = save_raw(records)
    print(f"Coleta concluida: {len(records)} registros")
    print(f"CSV raw:  {csv_path}")
    print(f"JSON raw: {json_path}")


if __name__ == "__main__":
    main()
