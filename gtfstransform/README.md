Este projeto faz:
- lê cada um dos arquivos em um prefixo de uma pasta raw no minio

Configurações:
RAW_BUCKET_NAME = "raw"
APP_FOLDER = "gtfs"
TRUSTED_BUCKET_NAME = "trusted"


CREATE TABLE trusted.routes (
    route_id TEXT,
    agency_id INTEGER,	
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    route_color TEXT,
    route_text_color TEXT
);



Para instalar os requisitos:
pip install -r requirements.txt

Para executar: 
python ./main.py
