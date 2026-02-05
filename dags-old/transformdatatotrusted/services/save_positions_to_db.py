from transformdatatotrusted.infra.db import bulk_insert_data_table
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_positions_to_db(config, positions_table):
    """
    Insert 10k+ items from memory list.
    Assumes list format: (extracao_ts, veiculo_id, linha_lt, linha_code,
                          linha_sentido, lt_destino, lt_origem, veiculo_prefixo,
                          veiculo_acessivel, veiculo_ts, veiculo_lat, veiculo_long)
    """
    table_name = config["POSITIONS_TABLE_NAME"]
    insert_sql = f"""
    INSERT INTO {table_name} (
        extracao_ts, veiculo_id, linha_lt, linha_code, linha_sentido,
        lt_destino, lt_origem, veiculo_prefixo, veiculo_acessivel, veiculo_ts,
        veiculo_lat, veiculo_long, is_circular, first_stop_id, first_stop_lat,
        first_stop_lon, last_stop_id, last_stop_lat, last_stop_lon,
        distance_to_first_stop, distance_to_last_stop
    ) VALUES %s
    """

    bulk_insert_data_table(config, insert_sql, positions_table)
