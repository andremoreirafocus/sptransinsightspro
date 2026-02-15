## Objetivo deste subprojeto
Fazer a transformação dos dados extraídos de posição dos ôibus da API da SPTrans e já disponibilizados na camada raw pelo microserviço loadlivedata enriquecendo-os com os dados extraídos do GTFS da SPTrans, extraidos e transformados pelo processo gtfs.
A implementação final é feita via a DAG transformlivedata do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática - via arquivo config.py - de acordo com o ambiente de execução, seja produção, via Airflow, ou desenvolvimento, local.


## O que este subprojeto faz
- lê um arquivo JSON com as posicoes instantâneas dos onibus fornecidos pela sptrans armazenado no bucket da camada raw no serviço de object storage particionados por ano, mes e dia
- transforma os dados em uma big table consolidada em memória enriquecendo-os com dados da tabela de detalhes das viagens gerada pelo subprocesso gtfs
- salva os dados de posição instantânea transformados e enriquecidos da tabela em memória para uma pasta (prefixo) sptrans no bucket da camada trusted no serviço de object storage, particionados por ano, mes, dia e hora para acelerar a busca e filtragem por timestamp de extração das posições instantâneas dos ônibus


## Pré-requisitos
- Disponibilidade de dois buckets: uma para a camada raw e outro para a camada trusted, previamente criados no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket da camada raw e leitura e escrita na camada trusted
- Criação do arquivo de configurações

## Configurações
RAW_BUCKET = <raw_bucket> 
APP_FOLDER = "sptrans"
GTFS_FOLDER = "gtfs"
POSITIONS_TABLE_NAME="positions"
TRIP_DETAILS_TABLE_NAME="trip_details"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
RAW_DATA_COMPRESSION = "true" # se desejar ativar a compressao ao salvar os arquivos 
RAW_DATA_COMPRESSION_EXTENSION = ".zst" # define a extensao do arquivo comprimido a ser lido

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python transformlivedata-v2.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

## Estrutura da tabela de posições instantâneas enriquecidas criadas neste subprojeto usando comando equivalente SQL:

```sql
CREATE TABLE trusted.positions (
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
```

## Estrutura da tabela antes do enriquecimento por trip_details
CREATE TABLE trusted.positions (
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

####################  EXPLORACAO POSITIONS ################

# exploracao para a camada refined
select count(*) from  trusted.positions

select linha_lt, count(1) from  trusted.positions as p
group by p.linha_lt
order by count(1) desc
6000-10	6396
2290-10	5272
3459-10	5013
5175-10	3973
4310-10	3675

-- '1015-10' é uma linha circular
-- '6000-10' é uma linha regular
select veiculo_id, count(1) from  trusted.positions as p
--where linha_lt ='6000-10'
--where linha_lt ='1015-10'
where linha_lt ='2290-10'
group by p.veiculo_id
order by count(1) desc

#melhor org das colunas
select veiculo_ts, veiculo_id,
       distance_to_first_stop, distance_to_last_stop,
       is_circular, linha_sentido, 
       veiculo_lat, veiculo_long, 
       first_stop_lat, first_stop_lon,  last_stop_lat, last_stop_lon, first_stop_id, last_stop_id, lt_destino, lt_origem
       from  trusted.positions
--where linha_lt = '6000-10' and veiculo_id = '63541'
--where linha_lt = '1015-10' and veiculo_id = '16450'
where linha_lt = '2290-10' and veiculo_id = '41539'
order by veiculo_ts 


-- ordem original das colunas
select extracao_ts, veiculo_id, linha_lt, linha_code, linha_sentido,
        lt_destino, lt_origem, veiculo_prefixo, veiculo_acessivel, veiculo_ts,
        veiculo_lat, veiculo_long, is_circular, first_stop_id, first_stop_lat,
        first_stop_lon, last_stop_id, last_stop_lat, last_stop_lon,
        distance_to_first_stop, distance_to_last_stop from trusted.positions as p
where linha_lt = '6000-10'-- '1015-10'
and veiculo_id = '63541'-- 16450

select count(*) from trusted.positions
order by veiculo_ts 

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



