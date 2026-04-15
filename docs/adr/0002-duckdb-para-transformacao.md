# ADR-0002: DuckDB como motor de transformação

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

A camada trusted armazena dados em formato Parquet no MinIO (S3-compatible). Os pipelines de transformação precisam executar operações analíticas sobre esses arquivos: joins com tabelas GTFS, filtragens por janela de tempo, agregações por veículo/linha/direção, cálculos de distância e geração de relatórios.

As opções avaliadas foram: processar os dados inteiramente em Pandas (carregando DataFrames na memória), adotar um motor SQL distribuído (Presto/Trino, Spark) ou usar um motor analítico embarcado que consiga consultar Parquet diretamente no object storage.

O volume de dados é da ordem de dezenas de milhares de registros por execução (posições de ônibus a cada 2 minutos), não de bilhões de registros.

## Decisão

Usar **DuckDB** como motor de transformação para todas as operações que envolvem leitura de Parquet da camada trusted.

DuckDB é um banco de dados OLAP embarcado (sem servidor) capaz de:
- Consultar arquivos Parquet diretamente via `httpfs`/`s3` sem precisar baixá-los localmente.
- Executar SQL completo (joins, window functions, aggregations) em memória.
- Integrar-se nativamente com Pandas (`DuckDBPyRelation.df()`).
- Funcionar como biblioteca Python (`import duckdb`), sem processo externo.

A abstração compartilhada em `infra/duck_db_v3.py` encapsula a criação da conexão configurada com credenciais MinIO, expondo uma interface simples que pode ser substituída por fakes nos testes.

## Alternativas consideradas

**Pandas puro:** Simples, sem dependências adicionais. Porém, carrega tabelas inteiras na memória e expressar joins e aggregations em operações de DataFrame é mais verboso e menos legível do que SQL. Para o volume atual seria viável, mas o código seria significativamente mais difícil de manter.

**Apache Spark:** Solução horizontal para processamento distribuído de grande escala. Para o volume deste projeto, o overhead operacional (cluster, JVM, deploy no Docker Compose) seria desproporcional ao benefício. O tempo de startup de uma sessão Spark (15–30s) inviabilizaria DAGs que precisam executar em segundos.

**Presto / Trino:** Motor SQL distribuído altamente performático para consultas federadas em larga escala. Requer um servidor separado (ou cluster), configuração de catálogos, gestão do processo — complexidade de infraestrutura incompatível com os requisitos do projeto.

**DuckDB (escolhido):** Embarcado, sem servidor, consulta Parquet em S3 nativo, SQL completo, integração direta com Pandas. Não escala horizontalmente, mas os volumes deste projeto não exigem isso.

## Consequências

**Positivo:**
- Zero overhead de infraestrutura: DuckDB roda no mesmo processo Python que o Airflow operator, sem containers adicionais.
- Leitura de Parquet via S3 sem materialização local: apenas as colunas e partições necessárias são transferidas.
- SQL expressivo para joins com GTFS: o código de transformação é legível como uma query, não como encadeamento de operações de DataFrame.
- Testável via `FakeDuckDBConnection`: qualquer função que aceite `duckdb_client` pode ser testada com um fake sem infraestrutura real.

**Negativo / Tradeoffs:**
- Single-node: se o volume de dados crescer para dezenas de gigabytes por execução, DuckDB pode se tornar um gargalo de memória. A migração para Spark ou Trino exigiria refatoração das queries e da infraestrutura.
- Sem persistência entre execuções: cada conexão DuckDB é efêmera; estado entre runs precisaria ser materializado explicitamente em Parquet.
- Versão do DuckDB afeta compatibilidade do formato Parquet gerado; upgrades de versão precisam ser testados cuidadosamente.
