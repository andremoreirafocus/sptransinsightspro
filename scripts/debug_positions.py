import duckdb
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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

linha_lt = "1177-10"
veiculo_id = 31736

hours_interval = 12
bucket_name = "trusted"
app_folder = "sptrans"
positions_table_name = "positions"
now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
current_hour = int(now.strftime("%H"))
min_hour = current_hour - hours_interval
if min_hour < 0:
    min_hour = 0
s3_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}/year={year}/month={month}/day={day}/**"

sql = f"""
SELECT
    veiculo_ts, linha_lt, veiculo_id, linha_sentido, is_circular, extracao_ts,
    veiculo_lat, veiculo_long,
    distance_to_first_stop, distance_to_last_stop,
    trip_linear_distance
FROM read_parquet('{s3_path}', hive_partitioning = true)
WHERE hour::INTEGER >= {min_hour} AND hour::INTEGER <= {current_hour}
AND linha_lt = '{linha_lt}'
AND veiculo_id = {veiculo_id}
ORDER BY linha_lt, veiculo_id, veiculo_ts ASC;
"""
df = con.execute(sql).df()

print(f"Shape: {df.shape}")
with pd.option_context('display.max_rows', 200, 'display.max_columns', None, 'display.width', None):
    print(df.head(200).to_string())
