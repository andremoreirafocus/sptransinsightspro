Este projeto faz:
- lê um arquivo que as posicoes dos onibus fornecidos pela sptrans em um determinado ano, mes, dia, hora e minuto.
- Os dados são armazenados em um bucket no minio em uma subpasta (prefixo) seguindo uma estrutura de particionamento por ano, mes e dia
- o nome do arquivo a ser recuperado corresponde à hora e ao minuto em que os dados foram extraídos da api da sptrans
- transforma os dados em uma big table consolidada em memória
- salva o conteúdo da tabela da memória para uma tabela especificada

Configurações:
SOURCE_BUCKET = <source_bucket> # the bucket for the app to load data from
APP_FOLDER = <app_folder> # the subfolder for the app to load data from
TABLE_NAME=<table_name_including_schema> # where data will be written
MINIO_ENDPOINT=<hostname:port> # format 
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Database commands:
create database sptrans_insights;
\l
\c 
CREATE SCHEMA trusted;
\dn
CREATE TABLE trusted.posicoes (
    id BIGSERIAL PRIMARY KEY,
    extracao_ts TIMESTAMPTZ,       -- metadata.extracted_at: 
    veiculo_id INTEGER,            -- p: id do veiculo
    linha_lt TEXT,                 -- c: Letreiro completo
    linha_code INTEGER,            -- cl: Código linha
    linha_sentido INTEGER,         -- sl: Sentido
    lt_destino TEXT,               -- lt0: Destino
    lt_origem TEXT,                -- lt1: Origem
    veiculo_prefixo INTEGER,       -- p: Prefixo
    veiculo_acessivel BOOLEAN,     -- a: Acessível
    veiculo_ts TIMESTAMPTZ,        -- ta: Timestamp UTC
    veiculo_lat DOUBLE PRECISION,  -- py: Latitude
    veiculo_long DOUBLE PRECISION  -- px: Longitude
);


Para instalar os requisitos:
pip install -r requirements.txt

Para executar: 
python ./main.py


