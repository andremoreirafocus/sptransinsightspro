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
| [ADR-0002](./0002-duckdb-para-transformacao.md) | DuckDB como motor de transformação | Aceito |
| [ADR-0003](./0003-microservico-extracao-apscheduler.md) | Microserviço de extração com APScheduler | Aceito |
| [ADR-0004](./0004-framework-de-qualidade-de-dados.md) | Framework de qualidade de dados multi-camada | Aceito |
| [ADR-0005](./0005-validacao-orientada-a-configuracao.md) | Validação orientada a configuração (JSON Schema + Great Expectations) | Aceito |
| [ADR-0006](./0006-injecao-de-dependencia-para-testabilidade.md) | Injeção de dependência como estratégia de testabilidade | Aceito |
| [ADR-0007](./0007-workflow-de-promocao-de-pipelines.md) | Workflow de promoção de pipelines (dags-dev → airflow/dags) | Aceito |
