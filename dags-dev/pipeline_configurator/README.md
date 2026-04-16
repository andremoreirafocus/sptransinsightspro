## Objetivos deste subprojeto
Centralizar o carregamento e a validação de configurações das pipelines, garantindo consistência entre ambiente local (dev) e produção (Airflow).
O `pipeline_configurator` fornece um contrato canônico de configuração, validação por schema (Pydantic) e carregamento padronizado de credenciais.

## O que este subprojeto faz
- define um ponto único de entrada para carregar configurações de pipelines
- valida o conteúdo de `general` com schemas Pydantic específicos de cada pipeline
- normaliza a saída com o contrato canônico:
  - `general`
  - `connections`
  - artefatos de validação declarados em `data_validations` dentro do `general` (opcionais): `raw_data_json_schema` e `data_expectations`
- resolve automaticamente o ambiente de execução (local vs Airflow)
- evita duplicação de lógica de configuração dentro das pipelines

## Pré-requisitos
- Arquivos de configuração JSON por pipeline (`{pipeline}_general.json`)
- Schemas Pydantic específicos por pipeline (`{pipeline}_config_schema.py`)
- Credenciais configuradas no `.env` (local/dev) ou no Airflow (Connections/Variables)

## Configurações
O módulo é exposto em `pipeline_configurator/config.py` através da função `get_config`.

### Local/dev
- `general` vem de `dags-dev/{pipeline}/config/{pipeline}_general.json`
- artefatos de validação listados em `data_validations` vêm de `dags-dev/{pipeline}/config/{pipeline}_{name}.json`
- credenciais são lidas do `.env` em `dags-dev/{pipeline}/.env`

### Airflow (produção)
- `general` vem da Variable `{pipeline}_general`
- artefatos de validação listados em `data_validations` vêm da Variable `{pipeline}_{name}`
- credenciais são lidas via Connections do Airflow (ex.: MinIO, Postgres, HTTP)

## Contrato canônico
Exemplo de saída para uma pipeline com `data_validations` declarado:
```json
{
  "general": { ... },
  "connections": {
    "object_storage": { ... },
    "database": { ... },
    "http": { ... }
  },
  "raw_data_json_schema": { ... },
  "data_expectations": { ... }
}
```

As chaves de validação (`raw_data_json_schema`, `data_expectations`, etc.) são carregadas automaticamente a partir da seção `data_validations` do `general`. Pipelines sem essa seção recebem apenas `general` e `connections`.

## Exemplo de uso
```python
from pipeline_configurator.config import get_config
from mypipeline.config.mypipeline_config_schema import GeneralConfig

PIPELINE_NAME = "mypipeline"

pipeline_config = get_config(
    PIPELINE_NAME,
    None,  # env override (None = auto)
    GeneralConfig,
    http_conn_name=None,
    object_storage_conn_name="minio_conn",
    database_conn_name="postgres_conn",
)
```
