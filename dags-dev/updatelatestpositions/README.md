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

# A tabela abaixo precisa ser criada, pois é criada via CTAS
```sql
CREATE TABLE refined.latest_positions (
    id BIGSERIAL PRIMARY KEY,
    veiculo_ts TIMESTAMPTZ,        -- ta: Timestamp UTC
    veiculo_id INTEGER,            -- p: id do veiculo
    veiculo_lat DOUBLE PRECISION,  -- py: Latitude
    veiculo_long DOUBLE PRECISION,  -- px: Longitude
    linha_lt TEXT,                 -- c: Letreiro completo
    linha_sentido INTEGER,         -- sl: Sentido
    trip_id TEXT
);
```