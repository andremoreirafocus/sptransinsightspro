import duckdb


def get_duckdb_connection(connection):
    endpoint = connection["endpoint"]
    access_key = connection["access_key"]
    secret_key = connection["secret_key"]
    con = None
    try:
        con = duckdb.connect(database=":memory:")
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='{endpoint}'; 
            SET s3_access_key_id='{access_key}';
            SET s3_secret_access_key='{secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
    except Exception as e:
        if con is not None:
            con.close()
        raise ValueError(f"Connection error: {e}") from e
    return con
