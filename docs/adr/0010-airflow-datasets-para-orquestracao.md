# ADR-0010: Airflow Datasets para orquestração orientada a eventos

**Data:** 2026-05-04  
**Status:** Proposto

## Contexto

O projeto já utiliza o Airflow como camada de orquestração dos pipelines, mas parte das dependências entre DAGs ainda era modelada por cron schedules independentes, e não por eventos explícitos de publicação de dados.

Esse desenho funciona quando a relação entre pipelines é apenas temporal. Porém, para alguns fluxos do projeto, a dependência real é semântica: uma DAG downstream só deveria iniciar quando a DAG upstream concluir com sucesso e publicar um novo artefato de dados útil para consumo.

Os casos mais claros são:

- `gtfs` produz `trip_details`, consumido por `refinedsynctripdetails`
- `transformlivedata` produz posições transformadas na camada trusted, consumidas por:
  - `updatelatestpositions`
  - `refinedfinishedtrips`

Quando a coordenação é feita apenas por cron:

- DAGs downstream podem executar sem que um novo dado upstream tenha sido publicado
- a manutenção passa a depender de alinhar schedules manualmente entre pipelines relacionadas
- a modelagem da dependência fica implícita no tempo, e não explícita no grafo de orquestração

Além disso, o projeto já separa corretamente:

- lógica de negócio e execução nas camadas de serviços/pipeline
- responsabilidades específicas de Airflow nas wrappers versionadas em `dags-dev/` e `airflow/dags/`

Essa separação deve ser preservada.

## Decisão

Adotar **Airflow Datasets** para representar dependências orientadas a eventos entre DAGs quando a relação correta entre elas for “novo dado upstream publicado com sucesso”.

### Regras da decisão

1. O Dataset deve ser emitido apenas na camada de orquestração do Airflow.
- A emissão do evento fica na wrapper da DAG, não em funções de serviço nem na lógica central do pipeline.

2. A lógica de negócio não deve conhecer Airflow Datasets.
- Serviços e pipelines continuam reutilizáveis e executáveis fora do Airflow.

3. DAGs downstream orientadas a evento devem consumir o Dataset em vez de depender de cron schedule.

4. Backfills não devem, por padrão, emitir o mesmo Dataset da DAG principal.
- Isso evita fan-out involuntário para DAGs downstream durante reprocessamentos históricos.

### Cadeias de orquestração cobertas

- `gtfs` publica `gtfs://trip_details_ready`
- `refinedsynctripdetails` consome esse Dataset

- `transformlivedata` publica `sptrans://trusted/transformed_positions_ready`
- `updatelatestpositions` consome esse Dataset
- `refinedfinishedtrips` consome esse Dataset

## Alternativas consideradas

**Cron schedules independentes**

É a solução mais simples operacionalmente, mas mantém a dependência entre DAGs implícita no tempo. Isso aumenta o acoplamento de manutenção entre schedules e permite execuções downstream sem novo dado upstream.

**Trigger explícito entre DAGs (ex: TriggerDagRunOperator ou chamada de API)**

Explicita a relação entre DAGs, mas introduz acoplamento mais forte entre orquestradores específicos e nomes de DAG, além de tornar a semântica menos aderente ao modelo nativo de datasets do Airflow.

**Mover a emissão do evento para a lógica interna do pipeline**

Foi descartado porque mistura responsabilidade de orquestração com lógica de execução. Isso reduziria reusabilidade, testabilidade e portabilidade das funções centrais para execução fora do Airflow.

## Consequências

**Positivo:**

- Dependências entre DAGs passam a ser explícitas no modelo de orquestração.
- DAGs downstream executam em resposta à publicação bem sucedida de novos dados upstream, e não apenas por coincidência de cron.
- Maior freshness dos artefatos downstream.
- Menor necessidade de manutenção manual de schedules acoplados entre pipelines relacionadas.
- Preserva a separação arquitetural: Dataset permanece na wrapper de Airflow, sem contaminar serviços e pipelines.

**Negativo / Tradeoffs:**

- A topologia de eventos passa a depender mais fortemente de recursos específicos do Airflow.
- O grafo de datasets precisa ser documentado claramente para evitar opacidade operacional.
- Estratégias de backfill exigem atenção explícita para não disparar pipelines downstream indevidamente.
- Parte do comportamento operacional deixa de ser visível apenas pelo cron e passa a exigir leitura do grafo de datasets.
