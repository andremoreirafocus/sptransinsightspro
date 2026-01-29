### Este projeto é responsável por gerar os dados da camada refined através de duas funções:

* **`extract_trips_for_all_Lines_and_vehicles(config)`**: Mapeia cada veículo ativo a uma jornada específica (`trip_id`), cruzando dados em tempo real com o planejamento estático (GTFS).
* **`update_latest_positions(config)`**: Mantém o estado mais recente de cada veículo na frota para monitoramento imediato.

### 🗄️ Definições de Tabelas (Schema `refined`)

#### A. Viagens Finalizadas (`refined.finished_trips`)
Armazena o histórico consolidado de jornadas concluídas para análise de eficiência e performance usando como fonte a tabela de viagens finalizadas

#### B. Últimas posições (`refined.latest_positions`)
Alimenta via CTAS a tabela que é fonte dp mapa de frota utilizando latitude e longitude para exibir a posição exata de cada ônibus no momento da consulta.


## Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Configurações do .env:
FINISHED_TRIPS_TABLE_NAME=<table_name_for_finished_trips_including_schema_in_refined>
POSITIONS_TABLE_NAME=<table_name_for_positions_in_trusted>
LATEST_POSITIONS_TABLE_NAME=<table_name_for_positions_in_refined>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

## Para executar: 
Criar tabelas conforme instruções abaixo
python ./main.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

## Configurações de Banco de dados que devem ser feitas antes da execução:
CREATE SCHEMA refined;

```sql
CREATE TABLE refined.finished_trips (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION
);
```
Para as consultas de construção das trips terem melhor performance:
```sql
CREATE INDEX idx_positions_linha_veiculo_ts
ON trusted.positions (
    linha_lt,
    veiculo_id,
    veiculo_ts ASC
);
```

## A consulta abaixo cria a tabela refined.latest_positions a cada execução
```sql
CREATE TABLE refined.latest_positions AS
WITH latest_snapshot AS (
    -- 1. Captura o timestamp exato do último lote de extração
    SELECT MAX(extracao_ts) AS max_ts
    FROM trusted.positions
)
-- 2. Projeta os dados e calcula o trip_id com o mapeamento 1->0 e 2->1
SELECT 
    p.veiculo_id, 
    p.veiculo_lat,  
    p.veiculo_long, 
    p.linha_lt, 
    p.linha_sentido,
    -- Concatenação da linha com o sentido mapeado
    p.linha_lt || '-' || (
        CASE 
            WHEN p.linha_sentido = 1 THEN '0' 
            WHEN p.linha_sentido = 2 THEN '1' 
            ELSE NULL -- Garante integridade para valores inesperados
        END
    ) AS trip_id
FROM trusted.positions p
JOIN latest_snapshot ls ON p.extracao_ts = ls.max_ts;
```

# A tabela abaixo nao precisa ser criada, pois é criada via CTAS
```sql
CREATE TABLE refined.latest_positions (
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

#Tabela usada apenas em testes de algoritmo experimental
```sql
CREATE TABLE trusted.ongoing_trips (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION
);
