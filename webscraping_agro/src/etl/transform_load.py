from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


RAW_BASE_DIR = Path("data/raw/fao/food_price_index")
PROCESSED_BASE_DIR = Path("data/processed/fao/food_price_index")


@dataclass
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


COMMODITY_MAP = {
    "FOOD PRICE INDEX": "FOOD_PRICE_INDEX",
    "MEAT": "MEAT",
    "DAIRY": "DAIRY",
    "CEREALS": "CEREALS",
    "OILS": "OILS",
    "SUGAR": "SUGAR",
}


def find_latest_raw_file(base_dir: Path = RAW_BASE_DIR) -> Path:
    candidates = sorted(base_dir.glob("ingested_at=*/run_id=*/records.parquet"))
    if not candidates:
        raise FileNotFoundError(f"Nenhum records.parquet encontrado em: {base_dir}")
    return candidates[-1]


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def transform_raw_to_processed(raw_path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(raw_path)
    if frame.empty:
        raise ValueError("Arquivo raw vazio. Nada para processar.")

    frame = frame.copy()
    frame["commodity_name"] = frame["commodity_name"].map(_normalize_text)
    frame["commodity_code"] = frame["commodity_name"].map(COMMODITY_MAP)

    # Requisito do teste: tratar valores ausentes e padronizar categorias.
    frame["commodity_code"] = frame["commodity_code"].fillna("UNKNOWN")
    frame["price_value"] = pd.to_numeric(frame["price_text"], errors="coerce")
    frame["price_value"] = frame["price_value"].fillna(0.0)
    frame["price_value"] = frame["price_value"].clip(lower=0)

    period_as_dt = pd.to_datetime(frame["ref_period"], format="%Y-%m", errors="coerce")
    frame["price_month"] = period_as_dt.dt.to_period("M").dt.to_timestamp()
    frame = frame[frame["price_month"].notna()].copy()

    frame["region_code"] = "GLOBAL"
    frame["currency_code"] = frame["currency"].fillna("INDEX_POINTS")
    frame["unit"] = frame["unit"].fillna("index_points")
    frame["source_system"] = frame["source_name"]
    frame["source_url"] = frame["source_url"].fillna("")
    frame["source_record_id"] = frame["record_id"]

    cols = [
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
    return frame[cols].sort_values(["commodity_code", "price_month"]).reset_index(drop=True)


def save_processed(df: pd.DataFrame, source_raw_path: Path) -> tuple[Path, Path]:
    run_id = source_raw_path.parent.name
    ingested_at = source_raw_path.parent.parent.name
    out_dir = PROCESSED_BASE_DIR / ingested_at / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "prices_processed.csv"
    parquet_path = out_dir / "prices_processed.parquet"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_parquet(parquet_path, index=False)
    return csv_path, parquet_path


def postgres_config_from_env() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "agro"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def load_processed_to_postgres(df: pd.DataFrame, cfg: PostgresConfig) -> int:
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT region_id FROM processed.dim_region WHERE region_code = 'GLOBAL'")
                row = cur.fetchone()
                if not row:
                    raise ValueError("Regiao GLOBAL nao encontrada. Rode sql/seed_reference.sql antes.")
                region_id = row[0]

                cur.execute("SELECT commodity_code, commodity_id FROM processed.dim_commodity")
                commodity_lookup = {code: cid for code, cid in cur.fetchall()}

                payload: list[tuple] = []
                for rec in df.itertuples(index=False):
                    commodity_id = commodity_lookup.get(rec.commodity_code)
                    if commodity_id is None:
                        continue
                    payload.append(
                        (
                            commodity_id,
                            region_id,
                            rec.price_month.date(),
                            float(rec.price_value),
                            rec.currency_code,
                            rec.unit,
                            rec.source_system,
                            rec.source_url,
                            rec.source_record_id,
                            datetime.now(timezone.utc),
                        )
                    )

                if not payload:
                    return 0

                execute_batch(
                    cur,
                    """
                    INSERT INTO processed.fact_price_monthly (
                        commodity_id,
                        region_id,
                        price_month,
                        price_value,
                        currency_code,
                        unit,
                        source_system,
                        source_url,
                        source_record_id,
                        ingested_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (commodity_id, region_id, price_month, source_system)
                    DO UPDATE SET
                        price_value = EXCLUDED.price_value,
                        currency_code = EXCLUDED.currency_code,
                        unit = EXCLUDED.unit,
                        source_url = EXCLUDED.source_url,
                        source_record_id = EXCLUDED.source_record_id,
                        ingested_at = EXCLUDED.ingested_at
                    """,
                    payload,
                )
                return len(payload)
    finally:
        conn.close()
