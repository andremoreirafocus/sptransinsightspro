## Objetivos desta camada
Centralizar integrações técnicas reutilizáveis pelas pipelines, mantendo o código de infraestrutura separado da lógica de negócio.
Esta camada fornece funções utilitárias para acesso a storage, bancos e motores analíticos.

## O que esta camada contém
- **Object Storage**: funções para leitura, listagem e gravação de objetos (`object_storage.py`)
- **DuckDB**: helper para criar conexões em memória e ler dados em formato Parquet (`duck_db_v3.py`)
- **PostgreSQL / SQL**: helpers para execução de queries e inserções (`pg_db_v2.py`, `sql_db_v2.py`)

## Observações
- Esta camada não contém regras de negócio.
- As pipelines consomem esses módulos passando as conexões já resolvidas pelo `pipeline_configurator`.
