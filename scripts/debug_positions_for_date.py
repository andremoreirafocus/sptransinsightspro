import argparse
from datetime import date

import duckdb
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Inspect trusted SPTrans position records for one date."
    )
    parser.add_argument("--date", default="2026-06-05", help="Date in YYYY-MM-DD format.")
    parser.add_argument("--linha-lt", required=True, help="Line code, e.g. 2678-41.")
    parser.add_argument("--veiculo-id", type=int, help="Vehicle id. Omit to list vehicles.")
    parser.add_argument("--start-hour", type=int, default=0, help="Inclusive start hour.")
    parser.add_argument("--end-hour", type=int, default=23, help="Inclusive end hour.")
    parser.add_argument("--limit", type=int, default=300, help="Rows to print.")
    return parser.parse_args()


def connect():
    con = duckdb.connect(database=":memory:")
    con.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='datalake';
        SET s3_secret_access_key='datalake';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


def main():
    args = parse_args()
    target_date = date.fromisoformat(args.date)
    s3_path = (
        "s3://trusted/sptrans/positions/"
        f"year={target_date:%Y}/month={target_date:%m}/day={target_date:%d}/**"
    )

    con = connect()
    if args.veiculo_id is None:
        sql = f"""
        SELECT
            linha_lt,
            veiculo_id,
            COUNT(*) AS position_count,
            MIN(veiculo_ts) AS first_position_ts,
            MAX(veiculo_ts) AS last_position_ts,
            MIN(linha_sentido) AS min_sentido,
            MAX(linha_sentido) AS max_sentido
        FROM read_parquet('{s3_path}', hive_partitioning = true)
        WHERE hour::INTEGER >= {args.start_hour}
          AND hour::INTEGER <= {args.end_hour}
          AND linha_lt = '{args.linha_lt}'
        GROUP BY linha_lt, veiculo_id
        ORDER BY position_count DESC, veiculo_id
        LIMIT {args.limit};
        """
    else:
        sql = f"""
        SELECT
            veiculo_ts,
            linha_lt,
            veiculo_id,
            linha_sentido,
            is_circular,
            extracao_ts,
            veiculo_lat,
            veiculo_long,
            distance_to_first_stop,
            distance_to_last_stop,
            trip_linear_distance
        FROM read_parquet('{s3_path}', hive_partitioning = true)
        WHERE hour::INTEGER >= {args.start_hour}
          AND hour::INTEGER <= {args.end_hour}
          AND linha_lt = '{args.linha_lt}'
          AND veiculo_id = {args.veiculo_id}
        ORDER BY linha_lt, veiculo_id, veiculo_ts ASC
        LIMIT {args.limit};
        """

    df = con.execute(sql).df()
    print(f"Date: {args.date}")
    print(f"S3 path: {s3_path}")
    print(f"Shape: {df.shape}")
    with pd.option_context(
        "display.max_rows",
        args.limit,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
