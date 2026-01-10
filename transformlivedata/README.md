Este projeto faz:
- lê cada um dos arquivos em um prefixo de uma pasta raw no minio

Configurações:

Requisitos:
create database sptrans_insights;
\l
\c 
CREATE SCHEMA trusted;
\dn
CREATE TABLE trusted.posicoes (
    id BIGSERIAL PRIMARY KEY,
    timestamp_extracao TEXT,       -- metadata.extracted_at: horário extracao,
    veiculo_id INTEGER,             -- p: id do veiculo
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

