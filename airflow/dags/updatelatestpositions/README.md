## Objetivo deste subprojeto
Disponibilizar as posições instantâneas mais recentes dos ônibus na camada refined para consumo pelo dashboard.
A implementação final é feita via a DAG updatelatestpositions do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.

## O que este subprojeto faz
- lê as masis recentes posições instantâneas armazenadas na tabela de posições armazendas sptrans no bucket da camada trusted no serviço de object storage
- salva estes dados na camada refined implementada no banco de dados analítico de baixa latência, para consumo da camada de visualização

## Pré-requisitos
- Disponibilidade do buckets da camada trusted, previamente criado no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- Criação do arquivo de configurações

## Configurações
As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`

### Local/dev
- `general` vem do arquivo `dags-dev/updatelatestpositions/config/updatelatestpositions_general.json`
- `.env` em `dags-dev/updatelatestpositions/.env` é usado apenas para credenciais de conexão

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
    "app_folder": "sptrans"
  },
  "tables": {
    "positions_table_name": "positions",
    "latest_positions_table_name": "refined.latest_positions"
  }
}
```

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `updatelatestpositions_general` (JSON)
- Credenciais via Connections (MinIO e Postgres)

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python ./updatelatestpositions-v2.py

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
