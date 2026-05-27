# Registros de Decisão Arquitetural (ADRs)

Um ADR documenta uma decisão arquitetural significativa: o contexto que a motivou, o que foi decidido, as alternativas descartadas e as consequências (positivas e negativas) resultantes.

O objetivo não é justificar escolhas depois do fato, mas registrar o raciocínio no momento da decisão, para que colaboradores futuros entendam *por que* o código é como é — não apenas *o que* ele faz.

## Como usar

- Cada ADR é imutável após aceito. Se uma decisão for revisada, um novo ADR supersede o anterior (marcar o antigo como `Supersedido por ADR-XXXX`).
- Para propor uma nova decisão, crie um arquivo seguindo o padrão `NNNN-titulo-em-kebab-case.md` com status `Proposto`, seguindo a estrutura dos ADRs existentes.
- Numere sequencialmente a partir do próximo número disponível.

---

## Índice de decisões

| ADR | Título | Status |
|-----|--------|--------|
| [ADR-0001](./0001-arquitetura-medalhao.md) | Arquitetura Medallion (Raw → Trusted → Refined) | Aceito |
| [ADR-0002](./0002-microservico-extracao-apscheduler.md) | Microserviço de extração com APScheduler | Aceito |
| [ADR-0003](./0003-modelagem-de-dados-e-arquitetura-para-deteccao-de-duracao-de-viagens.md) | Modelagem de dados e arquitetura para detecção de duração de viagens | Aceito |
| [ADR-0004](./0004-duckdb-para-transformacao.md) | DuckDB como motor de transformação | Aceito |
| [ADR-0005](./0005-framework-de-qualidade-de-dados.md) | Framework de qualidade de dados multi-camada | Aceito |
| [ADR-0006](./0006-validacao-orientada-a-configuracao.md) | Validação orientada a configuração (JSON Schema + Great Expectations) | Aceito |
| [ADR-0007](./0007-injecao-de-dependencia-para-testabilidade.md) | Injeção de dependência como estratégia de testabilidade | Aceito |
| [ADR-0008](./0008-workflow-de-promocao-de-pipelines.md) | Workflow de promoção de pipelines (dags-dev → airflow/dags) | Aceito |
| [ADR-0009](./0009-alertservice.md) | Design do alertservice (webhook, alertas cumulativos, SQLite) | Aceito |
| [ADR-0010](./0010-airflow-datasets-para-orquestracao.md) | Airflow Datasets para orquestração orientada a eventos | Proposto |
| [ADR-0011](./0011-logging-estruturado-com-contrato-canonico-e-transporte-desacoplado.md) | Logging estruturado com contrato canônico e transporte desacoplado | Aceito |
