from __future__ import annotations

import argparse

from src.etl.transform_load import (
    find_latest_raw_file,
    load_processed_to_postgres,
    postgres_config_from_env,
    save_processed,
    transform_raw_to_processed,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa ETL raw -> processed (FAO).")
    parser.add_argument(
        "--load-postgres",
        action="store_true",
        help="Tambem carrega o resultado no PostgreSQL (schema processed).",
    )
    args = parser.parse_args()

    raw_path = find_latest_raw_file()
    processed_df = transform_raw_to_processed(raw_path)
    csv_path, parquet_path = save_processed(processed_df, raw_path)

    print(f"Raw origem:      {raw_path}")
    print(f"Processed linhas:{len(processed_df)}")
    print(f"Processed CSV:   {csv_path}")
    print(f"Processed Parquet:{parquet_path}")

    if args.load_postgres:
        cfg = postgres_config_from_env()
        loaded = load_processed_to_postgres(processed_df, cfg)
        print(f"Linhas carregadas no PostgreSQL: {loaded}")


if __name__ == "__main__":
    main()
