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
- Arquivo `.env` com as credenciais necessárias
- Um template está disponível em `.env.example`
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

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Configurações de Banco de dados que devem ser feitas antes da execução:
Antes da execução desta pipeline, a tabela `refined.latest_positions` deve existir no banco `sptrans_insights`.

O caminho operacional recomendado para criação dos artefatos de banco necessários é executar o bootstrap PostgreSQL do projeto:

```bash
./automation/bootstrap_postgres.sh
```

Esse script aplica os arquivos SQL localizados em `/database/bootstrap/postgres/`.

### Schema de referência da tabela `refined.latest_positions`

O comando abaixo é mantido como referência documental da estrutura esperada da tabela:

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

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `updatelatestpositions_general` (JSON)
- Credenciais via Connections (MinIO e Postgres)

Antes da execução da DAG no Airflow, a tabela `refined.latest_positions` já deve estar criada conforme instruções acima.

## Instruções para execução em modo local
Crie `dags-dev/updatelatestpositions/.env` com base em `.env.example` preenchendo todos os campos:
Com a tabela já criada conforme instruções acima, execute:

```shell
python ./updatelatestpositions-v<version number>.py
```

Exemplo: 
```shell
python ./updatelatestpositions-v2.py
```
