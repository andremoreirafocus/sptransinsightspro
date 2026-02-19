## Objetivo deste subprojeto
Sincronizar a tabela de detalhes de viagem gerada na camada trusted a partir da extração de dados do GFTS da SPTRANS com a camada refined para servir à camada de visualização.
A implementação final é feita via a DAG gtfs do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática - via arquivo config.py - de acordo com o ambiente de execução, seja produção, via Airflow, ou desenvolvimento, local.

## O que este subprojeto faz
- Lê da camada trusted a tabela de detalhes de viagem gerada a partir da extração de dados do GFTS da SPTRANS.
- Replica este conteúdo para uma tabela de detalhes de viagens na camada refined para servir à camada de visualização.


## Pré-requisitos
- Disponibilidade do bucket da camada trusted, previamente criado no serviço de object storage
- Disponibiliadde da tabela trip_details na camada trusted 
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- Criação do arquivo de configurações

## Configurações
TRUSTED_BUCKET = "trusted"
GTFS_FOLDER="gtfs"
TRIP_DETAILS_TABLE_NAME="refined.trip_details"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
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
Criar tabelas conforme instruções abaixo
python ./refinedsynctripdetails-v1.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

## Configurações de Banco de dados que devem ser feitas antes da execução:
## Para criar as tabelas e índices necessários ao subprojeto:

Database commands:

docker exec -it postgres bash
psql -U postgres -W

```sql
CREATE DATABASE sptrans_insights;
```

\c sptrans_insights

```sql
CREATE TABLE refined.trip_details (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,             
    first_stop_id INTEGER,
    first_stop_name TEXT,
    first_stop_lat DOUBLE PRECISION,
    first_stop_lon DOUBLE PRECISION,
    last_stop_id INTEGER,
    last_stop_name TEXT,
    last_stop_lat DOUBLE PRECISION,
    last_stop_lon DOUBLE PRECISION,
    trip_linear_distance DOUBLE PRECISION,
    is_circular BOOLEAN
);

-- Optimized Search Index for PowerBI
-- This supports searching for a specific route/direction 
-- and narrowing it down by bus.
CREATE INDEX idx_trip_lookup 
ON refined.trip_details (trip_id);

```
