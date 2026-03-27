from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import json
import pandas as pd
import requests
from bs4 import BeautifulSoup


SOURCE_NAME = "fao_food_price_index"
SOURCE_URL = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
RAW_OUTPUT_DIR = Path("data/raw/commodities")
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}


@dataclass
class CommodityRecord:
    commodity_name: str
    symbol: str | None
    price_text: str | None
    currency: str | None
    ref_period: str | None
    unit: str | None
    source_name: str
    source_url: str
    collected_at: str
    record_id: str


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_name(value: str | None) -> str:
    if not value:
        return "UNKNOWN"
    return value.strip().upper()


def _resolve_fao_csv_url(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        anchor_text = anchor.get_text(" ", strip=True).lower()
        if "csv" in anchor_text or ".csv" in href.lower():
            return href
    raise ValueError("Nao foi possivel localizar o link CSV na pagina da FAO.")


def fetch_commodities(timeout: int = 25) -> list[dict[str, Any]]:
    landing_response = requests.get(SOURCE_URL, headers=DEFAULT_HEADERS, timeout=timeout)
    landing_response.raise_for_status()

    csv_url = _resolve_fao_csv_url(landing_response.text)
    csv_response = requests.get(csv_url, headers=DEFAULT_HEADERS, timeout=timeout)
    csv_response.raise_for_status()

    lines = csv_response.text.splitlines()
    header_idx = next(
        (idx for idx, line in enumerate(lines) if line.strip().startswith("Date,")),
        None,
    )
    if header_idx is None:
        raise ValueError("Nao foi possivel identificar cabecalho de dados no CSV da FAO.")

    df = pd.read_csv(StringIO(csv_response.text), skiprows=header_idx, usecols=range(7))
    df = df[df["Date"].notna()]

    expected_columns = {"Date", "Food Price Index", "Meat", "Dairy", "Cereals", "Oils", "Sugar"}
    available = [col for col in df.columns if col in expected_columns]
    if "Date" not in available:
        raise ValueError("Arquivo CSV nao contem coluna Date esperada.")

    melted = df[available].melt(id_vars=["Date"], var_name="commodity", value_name="price_index")
    melted["currency"] = "INDEX_POINTS"
    return melted.to_dict(orient="records")


def transform_raw_records(raw_items: list[dict[str, Any]]) -> list[CommodityRecord]:
    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[CommodityRecord] = []

    for idx, item in enumerate(raw_items, start=1):
        name_candidate = (
            item.get("name")
            or item.get("title")
            or item.get("commodity")
            or item.get("product")
        )
        symbol_candidate = item.get("symbol") or item.get("ticker")
        price_candidate = item.get("price") or item.get("last") or item.get("value") or item.get("price_index")
        currency_candidate = item.get("currency") or item.get("ccy")
        period_candidate = item.get("Date") or item.get("date")

        normalized_name = _normalize_name(_safe_str(name_candidate))
        symbol = _safe_str(symbol_candidate)
        price_text = _safe_str(price_candidate)
        currency = _safe_str(currency_candidate)
        ref_period = _safe_str(period_candidate)

        record_id = f"{normalized_name}_{ref_period or 'NODATE'}_{idx}"
        records.append(
            CommodityRecord(
                commodity_name=normalized_name,
                symbol=symbol,
                price_text=price_text,
                currency=currency,
                ref_period=ref_period,
                unit="index_points",
                source_name=SOURCE_NAME,
                source_url=SOURCE_URL,
                collected_at=now_iso,
                record_id=record_id,
            )
        )
    return records


def save_raw(records: list[CommodityRecord], output_dir: Path = RAW_OUTPUT_DIR) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"commodities_raw_{stamp}.csv"
    json_path = output_dir / f"commodities_raw_{stamp}.json"

    data_dicts = [asdict(record) for record in records]
    pd.DataFrame(data_dicts).to_csv(csv_path, index=False, encoding="utf-8")
    json_path.write_text(json.dumps(data_dicts, ensure_ascii=False, indent=2), encoding="utf-8")

    return csv_path, json_path
