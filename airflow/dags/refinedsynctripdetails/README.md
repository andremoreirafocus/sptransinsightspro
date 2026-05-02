## Objetivo deste subprojeto
Sincronizar a tabela de detalhes de viagem gerada na camada trusted a partir da extração de dados do GFTS da SPTRANS com a camada refined para servir à camada de visualização.
A implementação final é feita via a DAG refinedsynctripdetails do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.

## O que este subprojeto faz
- Lê da camada trusted a tabela de detalhes de viagem gerada a partir da extração de dados do GFTS da SPTRANS.
- Aplica uma transformação leve para a camada refined antes da persistência, preservando a tabela trusted como fonte canônica de referência.
- Salva este resultado em uma tabela de detalhes de viagens na camada refined para servir à camada de visualização.

### Tratamento de linhas circulares na camada refined
Para linhas circulares, a camada refined não replica cegamente os extremos da tabela trusted.

Como a visualização consome metadados de origem e destino por direção operacional, o pipeline ajusta os registros circulares:
- em viagens `-0`, preserva os campos de `first_stop_*`
- em viagens `-1`, preserva os campos de `last_stop_*`
- no extremo oposto, substitui o nome por `Parada intermediaria`
- no extremo oposto, define `id`, `lat` e `lon` como nulos

Assim:
- a camada trusted continua com metadados canônicos para processamento
- a camada refined passa a expor metadados mais aderentes à semântica esperada pelo dashboard


## Pré-requisitos
- Disponibilidade do bucket da camada trusted, previamente criado no serviço de object storage
- Disponibiliadde da tabela trip_details na camada trusted 
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- Arquivo `.env` com as credenciais necessárias
- Um template está disponível em `.env.example`
- Criação do arquivo de configurações

## Configurações
As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`

### Local/dev
- `general` vem do arquivo `dags-dev/refinedsynctripdetails/config/refinedsynctripdetails_general.json`
- `.env` em `dags-dev/refinedsynctripdetails/.env` é usado apenas para credenciais de conexão

Credenciais esperadas no `.env`:
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Chaves esperadas em `general`
```json
{
  "storage": {
    "trusted_bucket": "trusted",
    "gtfs_folder": "gtfs"
  },
  "tables": {
    "trip_details_table_name": "refined.trip_details"
  }
}
```

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `refinedsynctripdetails_general` (JSON)
- Credenciais via Connections (MinIO e Postgres)

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
Crie `dags-dev/refinedsynctripdetails/.env` com base em `.env.example` preenchendo todos os campos.
Criar tabelas conforme instruções abaixo:

```shell
python ./refinedsynctripdetails-v1.py
```

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
