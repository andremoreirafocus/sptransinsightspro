from src.infra.db import bulk_insert_data_table
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_positions_to_db(config, positions_table, table_name):
    """
    Insert 10k+ items from memory list.
    Assumes list format: (extracao_ts, veiculo_id, linha_lt, linha_code,
                          linha_sentido, lt_destino, lt_origem, veiculo_prefixo,
                          veiculo_acessivel, veiculo_ts, veiculo_lat, veiculo_long)
    """

    insert_sql = f"""
    INSERT INTO {table_name} (
        extracao_ts, veiculo_id, linha_lt, linha_code, linha_sentido,
        lt_destino, lt_origem, veiculo_prefixo, veiculo_acessivel, veiculo_ts,
        veiculo_lat, veiculo_long
    ) VALUES %s
    """

    bulk_insert_data_table(config, insert_sql, positions_table)
