## Objetivo deste subprojeto
Disponibilizar as posições instantâneas mais recentes dos ônibus na camada refined para consumo pelo dashboard.
A implementação final é feita via a DAGs refinedlivedata e updatelatestpositions do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática - via arquivo config.py - de acordo com o ambiente de execução, seja produção, via Airflow, ou desenvolvimento, local.

## O que este subprojeto faz
- lê as masis recentes posições instantâneas armazenadas na tabela de posições armazendas sptrans no bucket da camada trusted no serviço de object storage
- salva estes dados na camada refined implementada no banco de dados analítico de baixa latência, para consumo da camada de visualização

## Pré-requisitos
- Disponibilidade do buckets da camada trusted, previamente criado no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- Criação do arquivo de configurações

## Configurações
TRUSTED_BUCKET = "trusted"
APP_FOLDER = "sptrans"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
POSITIONS_TABLE_NAME="positions"
LATEST_POSITIONS_TABLE_NAME="refined.latest_positions"
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python ./refinedfinishedtrips-v2.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

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