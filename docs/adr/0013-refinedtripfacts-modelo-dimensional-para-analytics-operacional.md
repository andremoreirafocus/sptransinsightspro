# ADR-0013: Pipeline `refinedtripfacts` e modelo dimensional para analytics operacional

**Data:** 2026-06-16  
**Status:** Aceito

## Contexto

O ADR-0012 estabeleceu o Metabase self-hosted como plataforma de BI, consumindo a camada refined no PostgreSQL analítico (`postgres`). Este ADR decide **o que** essa camada oferece para o analytics operacional: um modelo dimensional dedicado, materializado no próprio banco.

A razão central é **desacoplar o modelo analítico da ferramenta de BI**. O modelo dimensional vive na camada de dados governada (`refined`), e **não** dentro do modelo/semântica interna do Metabase. Construir a lógica dimensional dentro do Metabase (camada de modelo, campos calculados, perguntas salvas) acoplaria a implementação à ferramenta: lock-in, um modelo não versionado e não governado preso na ferramenta, e migração custosa caso a plataforma de BI mude. É o mesmo princípio anti-acoplamento que motivou o ADR-0012 (remover o lock-in do Power BI proprietário) — não se troca o lock-in do Power BI pelo do Metabase empurrando o modelo para dentro dele. A tabela de fato é um contrato analítico **agnóstico de ferramenta**, legível por qualquer consumidor.

Soma-se a isso a necessidade de modelagem: `refined.finished_trips` é a **saída operacional** do pipeline de detecção de viagens (`refinedfinishedtrips`) — carrega `trip_id` (mas não `route_id`/`direction`), não tem dimensão de tempo e não é indexado para BI. O analytics operacional pretendido (frequência por rota/hora, duração P50/P95, confiabilidade, velocidade, "hoje vs. mesmo dia da semana nas últimas semanas") exige uma tabela de fato com atributos analíticos pré-calculados e uma dimensão de tempo, otimizada para o fatiamento das consultas.

## Decisão

Construir o pipeline `refinedtripfacts`, disparado pelo Airflow Dataset `finished_trips_ready`, que produz **`refined.trip_facts`** (um registro por viagem finalizada) e **`refined.dim_time`** (esquema estrela), no PostgreSQL refined.

- A transformação é **set-based em SQL** (PG→PG no mesmo banco: `finished_trips` → `trip_facts`) — e não linha a linha em Python, já que origem e destino estão no mesmo banco —, executada por um serviço Python (`create_trip_facts`), seguindo o mesmo padrão dos demais pipelines (serviços + `pipeline_configurator` + camadas `infra`/`quality`/`observability` + orquestração via Airflow Datasets + logging estruturado + relatório de qualidade + governança de lineage).
- `trip_facts` **pré-calcula** `route_id`, `direction`, `duration` e as chaves de tempo SP `*_time_dim_key`; `dim_time` provê o agrupamento temporal (hora/dia da semana/fim de semana) sem conversão de timezone em tempo de consulta.
- `dim_time` é provisionada **sem lacunas** a partir do intervalo real (`MIN(trip_start_time)`/`MAX(trip_end_time)`) das viagens do batch; carga **idempotente** (`ON CONFLICT DO NOTHING`); validação de completude; testes de integração isolados em banco de testes dedicado (`sptrans_insights_test`).
- `trip_facts` é particionada por dia com retenção própria (via `pg_partman`), independente do ciclo de vida operacional de `finished_trips`.

## Alternativas consideradas

**Construir o modelo dimensional dentro do Metabase (descartado):** Modelar fato/dimensão na camada de modelo do próprio Metabase (campos calculados, perguntas/coleções, semantic layer). Descartado por acoplar o modelo analítico à ferramenta de BI (lock-in), manter o modelo fora da camada de dados governada e versionada, e contrariar o princípio anti-acoplamento do ADR-0012. O modelo dimensional deve ser um contrato no banco, reutilizável por qualquer consumidor.

**Adotar dbt para a transformação (descartado):** Conceitualmente, a transformação é praticamente um caso de manual de *incremental model* do dbt (`materialized='incremental'`, `unique_key`, predicado por `logic_date`, estratégia `merge`/`delete+insert` equivalente ao `ON CONFLICT DO NOTHING`). O dbt traria nativamente vários mecanismos que hoje são feitos sob medida:
- **Model contracts** (nomes + tipos de colunas) ≈ o contrato de schema e a governança de lineage (`validate_trip_facts_lineage`).
- **Relationship test** (toda `*_time_dim_key` existe em `dim_time`) ≈ o teste de cobertura sem lacunas.
- **Schema tests** (not_null, accepted_values) ≈ validações declarativas.
- **Targets/profiles**: a conexão centralizada e trocável que o projeto exige para os testes de integração é nativa (`--target test` vs `--target prod`).
- Lineage DAG e docs automáticos.

Por que ainda assim foi descartado:
- **Cobertura parcial do pipeline.** dbt cobriria o *core* de transformação + testes + lineage, mas **não** cobre a identidade dos pipelines deste projeto: observabilidade por eventos estruturados (`execution_started/aborted/phase_metrics`, eventos por atividade, `correlation_id`/`execution_id`, taxonomia Loki/Grafana), relatório de qualidade enviado ao `alertservice` com limiares WARN/FAIL, condições de negócio (`sem dados upstream → execution_aborted`) e o gatilho por Airflow Dataset. dbt emite `run_results.json`/`manifest.json`, não esse fluxo — integrá-lo exigiria interpretar os artefatos do dbt e traduzi-los para a taxonomia existente. Restaria, portanto, um *wrapper* Python para orquestração, observabilidade e relatório de qualidade — ou seja, **dois paradigmas para um único pipeline**.
- **Valor abaixo do limiar para um único modelo.** O valor do dbt cresce com a quantidade de modelos SQL e macros compartilhadas. Aqui há essencialmente **um** modelo (+ a dimensão `dim_time`). Como não haverá novos pipelines PG→PG, não existe pressão de escala ou reuso que justifique o custo.
- **Custo de consistência arquitetural.** Introduzir dbt para um pipeline quebra a uniformidade dos outros seis (padrão Python de serviços), e adiciona ferramenta, runtime, CI e integração com Airflow (cosmos/BashOperator) para servir exatamente uma transformação.

**Não ter tabela de fato dedicada — consultar `finished_trips` diretamente (descartado):** `finished_trips` é a saída operacional, sem `route_id`/`direction` nem dimensão de tempo. Os dashboards re-derivariam atributos e fariam conversão de timezone a cada consulta, ficariam acoplados ao schema interno do pipeline operacional e sobrecarregariam uma tabela operacional de alta escrita com um segundo padrão de acesso.

## Consequências

**Positivo:**
- Modelo analítico **agnóstico de ferramenta**: o esquema estrela é um contrato no banco, governado e versionado, sem lock-in à plataforma de BI — qualquer consumidor (Metabase hoje, outro amanhã) lê a mesma `trip_facts`/`dim_time`.
- Modelo dimensional pronto para BI: atributos pré-calculados + `dim_time` permitem fatiamento rápido e temporalmente correto, sem conversão de timezone em tempo de consulta.
- Separação de responsabilidades: a camada de serviço analítico (`trip_facts`/`dim_time`) é independente da saída operacional (`finished_trips`) — schema, índices, particionamento e ciclo de vida próprios.
- Como `trip_facts` é o armazém durável do histórico analítico, **`finished_trips` não precisa de retenção de longo prazo** — permanece um handoff operacional enxuto, com particionamento horário e retenção curta. O histórico de tendências vive na tabela analítica, não na operacional.
- Consistência arquitetural: segue o mesmo padrão dos demais pipelines (serviços Python + observabilidade estruturada + relatório de qualidade), sem caso especial.

**Negativo / Tradeoffs:**
- Reimplementamos manualmente o que o dbt ofereceria pronto: contrato de schema, drift de lineage, teste de cobertura sem lacunas e testes de integração. Mais código próprio para manter.
- A correção das semânticas SQL depende de uma camada de testes de integração contra PostgreSQL real (custo de infraestrutura de teste) em vez de `dbt test` declarativo.
- Duplicação parcial: a viagem finalizada existe em `finished_trips` (curto prazo) e em `trip_facts` (longo prazo) — custo aceito em troca do desacoplamento entre operação e serving analítico.

## Quando reconsiderar (a rejeição do dbt)

A escolha de não adotar dbt deve ser revista (novo ADR que a substitua) se:
- A camada refined evoluir para um **warehouse com muitos modelos SQL** (não apenas `trip_facts`), elevando o valor do dbt acima do limiar.
- O custo de **integrar os artefatos do dbt** (`run_results.json`/`manifest.json`) à observabilidade existente em todos os pipelines — hoje desproporcional para um único pipeline — passar a ser compensado pelo ganho, eliminando a objeção do paradigma duplo.
