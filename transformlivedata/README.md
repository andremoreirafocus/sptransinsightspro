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

Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar: 
python ./main.py

Instruções adicionais:
Database commands:
create database sptrans_insights;
\l
\c sptrans_insights
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

CREATE TABLE trusted.posicoes_teste (
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
    veiculo_long DOUBLE PRECISION,  -- px: Longitude
    is_circular BOOLEAN,
    first_stop_id INTEGER,
    first_stop_lat DOUBLE PRECISION,
    first_stop_lon DOUBLE PRECISION,
    last_stop_id INTEGER,
    last_stop_lat DOUBLE PRECISION,
    last_stop_lon DOUBLE PRECISION,
    distance_to_first_stop DOUBLE PRECISION,
    distance_to_last_stop DOUBLE PRECISION
);


----------------------------------------------------

########################### EXPLORACAO ############################

"Linhas com mais posicoes "
SELECT linha_lt, COUNT(linha_lt)
FROM trusted.posicoes
group by linha_lt
order by  COUNT(linha_lt) desc;
2290-10	22631
6000-10	21545
3459-10	17448
5175-10	16661
4310-10	15434

"Veiculos com mais posicoes distintas de uma linha"
SELECT 
    veiculo_id, 
    COUNT(DISTINCT (veiculo_lat, veiculo_long)) AS unique_positions_count
FROM 
    trusted.posicoes
WHERE 
    linha_lt = '2290-10' 
    AND linha_sentido = 1
GROUP BY 
    veiculo_id
ORDER BY 
    unique_positions_count DESC;
41559	163
41514	161
41522	155
41580	153
41595	152

# Descobrindo as paradas dos terminais da linha
select * from trusted.stop_times
where trip_id = '2290-10-0';
2290-10-0	15:00:00	15:00:00	750006788	1
2290-10-0	17:42:00	17:42:00	800016547	69

select * from trusted.stop_times
where trip_id = '2290-10-1';
2290-10-1	17:00:00	17:00:00	800016547	1
2290-10-1	19:22:00	19:22:00	750006786	54

# Descobrindo a posicao dos terminal origem
select * from trusted.stops
where stop_id = 800016547;
800016547	Terminal Parque Dom Pedro II - Plat 06	Term. Parque Dom Pedro II - Plat 06 Ref.: Av Do Estado/ Vdto Antonio Nakashima - (pmv - 59)	-23.547014	-46.629795

# Tentando achar em que momento o veiculo estava no terminal origem
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546000 and veiculo_lat > -23.548000 ) and (veiculo_long < -46.629000 and veiculo_long > -46.631000)
order by veiculo_ts;

# Tentando achar em que momento o veiculo estava no terminal origem
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546500 and veiculo_lat > -23.547500 ) and (veiculo_long < -46.629500 and veiculo_long > -46.630500)
order by veiculo_ts;

# Descobrindo a posicao dos terminal destino
select * from trusted.stops
where stop_id = 750006788;
750006788	Terminal S. Mateus - Plat. D	Term. S. Mateus - Plat. D Ref.: Av Sapopemba/ Pc Felisberto Fernandes Da Silva	-23.613206	-46.476113

# Tentando achar em que momento o veiculo estava no terminal origem
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.612000 and veiculo_lat > -23.614000 ) and (veiculo_long < -46.475000 and veiculo_long > -46.477000);


# Achando a distancia estimada em metros do veiculo em relaçao ao terminal de origem, ordenado pela distancia e horario
WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    distance_meters, veiculo_ts;

# Achando a distancia estimada em metros do veiculo em relaçao ao terminal de origem, ordenado pelo horario
WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    veiculo_ts;


# Achando a distancia estimada em metros do veiculo em relaçao ao terminal de origem, e ao terminal de destino ordenado pelo horario
WITH constants AS (
    SELECT 
        -23.547014 AS lat_tp, 
        -46.629795 AS long_tp,
        -23.613206 AS lat_ts,  	
        -46.476113 AS long_ts
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_tp, 2) + 
        POW(p.veiculo_long - c.long_tp, 2)
    ) * 106428 AS distance_meters_tp,
    SQRT(
        POW(p.veiculo_lat - c.lat_ts, 2) + 
        POW(p.veiculo_long - c.long_ts, 2)
    ) * 106428 AS distance_meters_ts
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    veiculo_ts;



----------------------------------------------------
Queries de exploração:

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1
order by veiculo_ts;

select count(*) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1;
8938

select count(*) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 2;
6197

select distinct(veiculo_id) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1;

select count(distinct(veiculo_id)) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1;
64 veiculos

#Quantidade de posicoes por veiculo nesta linha e sentido
SELECT 
    veiculo_id, 
    COUNT(*) AS total_records
FROM 
    trusted.posicoes
WHERE 
    linha_lt = '2290-10' 
    AND linha_sentido = 1
GROUP BY 
    veiculo_id
ORDER BY 
    total_records DESC;
# o 41542 é o de maior número de posicoes

#As posicoes de um veiculo em um sentido
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542
order by veiculo_ts;

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542 and veiculo_lat = -23.613206 and veiculo_long = -46.476113;

select * from trusted.stops
where stop_id = 750006788;
750006788	Terminal S. Mateus - Plat. D	Term. S. Mateus - Plat. D Ref.: Av Sapopemba/ Pc Felisberto Fernandes Da Silva	-23.613206	-46.476113

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542 and (veiculo_lat < -23.612000 and veiculo_lat > -23.614000 ) and (veiculo_long < -46.475000 and veiculo_long > -46.477000);
3396743	2026-01-13 12:34:19.233 -0300	41542	2290-10	718	1	TERM. PQ. D. PEDRO II	TERM. SÃO MATEUS	41542	true	2026-01-13 15:33:50.000 -0300	-23.6123985	-46.4752955
3985518	2026-01-13 12:34:19.233 -0300	41542	2290-10	718	1	TERM. PQ. D. PEDRO II	TERM. SÃO MATEUS	41542	true	2026-01-13 15:33:50.000 -0300	-23.6123985	-46.4752955

select * from trusted.stops
where stop_id = 800016547;
800016547	Terminal Parque Dom Pedro II - Plat 06	Term. Parque Dom Pedro II - Plat 06 Ref.: Av Do Estado/ Vdto Antonio Nakashima - (pmv - 59)	-23.547014	-46.629795

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542 and (veiculo_lat < -23.546000 and veiculo_lat > -23.548000 ) and (veiculo_long < -46.629000 and veiculo_long > -46.631000)
order by veiculo_ts;


"Veiculos com mais posicoes distintas"
SELECT 
    veiculo_id, 
    COUNT(DISTINCT (veiculo_lat, veiculo_long)) AS unique_positions_count
FROM 
    trusted.posicoes
WHERE 
    linha_lt = '2290-10' 
    AND linha_sentido = 1
GROUP BY 
    veiculo_id
ORDER BY 
    unique_positions_count DESC;
41559	163
41514	161
41522	155
41580	153
41595	152

select * from trusted.stops
where stop_id = 800016547;
800016547	Terminal Parque Dom Pedro II - Plat 06	Term. Parque Dom Pedro II - Plat 06 Ref.: Av Do Estado/ Vdto Antonio Nakashima - (pmv - 59)	-23.547014	-46.629795

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546000 and veiculo_lat > -23.548000 ) and (veiculo_long < -46.629000 and veiculo_long > -46.631000)
order by veiculo_ts;

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546500 and veiculo_lat > -23.547500 ) and (veiculo_long < -46.629500 and veiculo_long > -46.630500)
order by veiculo_ts;

select * from trusted.stops
where stop_id = 750006788;
750006788	Terminal S. Mateus - Plat. D	Term. S. Mateus - Plat. D Ref.: Av Sapopemba/ Pc Felisberto Fernandes Da Silva	-23.613206	-46.476113

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.612000 and veiculo_lat > -23.614000 ) and (veiculo_long < -46.475000 and veiculo_long > -46.477000);


filtrado
WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
    AND (p.veiculo_lat < -23.546500 AND p.veiculo_lat > -23.547500) 
    AND (p.veiculo_long < -46.629500 AND p.veiculo_long > -46.630500)
ORDER BY 
    p.veiculo_ts;


WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    distance_meters, veiculo_ts;



