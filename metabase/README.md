# Metabase (BI como código)

Ativos que são fonte da verdade para a plataforma de BI Metabase self-hosted (ADR-0012). O
Metabase consome a camada governada `refined` do PostgreSQL analítico (`postgres` /
`sptrans_insights`) e é o **dono das perguntas (queries) executáveis do dashboard**.

## Por que estes arquivos vivem aqui (e não no pipeline)

O ADR-0013 desacopla o **modelo analítico** (o star schema `refined.trip_facts` — um contrato
agnóstico de ferramenta, versionado na camada de dados) da **ferramenta de BI**. A metade
simétrica dessa regra: as **queries executáveis dos painéis são responsabilidade do Metabase**,
não do pipeline. Elas carregam filtros, parâmetros, seletores de rota, ordenação e lógica de
exibição que pertencem à pergunta, não ao contrato de dados.

Mantê-los aqui (em vez de dentro de `dags-dev/refinedtripfacts/`) significa:

- propriedade explícita — o pipeline é dono do modelo + uma prova de conceito; o Metabase é
  dono das perguntas;
- eles nunca são arrastados para o runtime do Airflow pelo `scripts/promote_pipeline.py` (esse
  rsync sincroniza a pasta inteira do pipeline para `airflow/dags/`); estas queries não têm
  nada a ver com o runtime das DAGs.

## Relação com o `queries/` do pipeline

`dags-dev/refinedtripfacts/queries/` contém **SQL de prova de conceito, não autoritativo e sem
filtros**, que provou que o modelo dimensional consegue responder a todos os painéis. Os
arquivos aqui são as perguntas nativas **autoritativas**: adicionam os field filters do
Metabase, as variáveis e as estatísticas exatas que os painéis exigem. Onde os dois divergirem,
o **documento de design** (`.plans/metabase-dashboard-panel-design.md`) é a fonte da verdade e
estes arquivos o implementam; os arquivos de PoC não são editados para acompanhar o dashboard.

## Layout

```
metabase/
  README.md
  dashboard_queries/   # SQL autoritativo das perguntas nativas do Metabase (P0–P10)
```

### `dashboard_queries/` → mapa de painéis

| Arquivo | Painel(éis) | Âncora (lógica de data) |
| --- | --- | --- |
| `latest_batch_freshness.sql`                  | P0          | `logic_date` (apenas freshness operacional) |
| `today_kpis.sql`                              | P1, P2, P3  | **conclusão** da viagem (`ended_at_time_dim_key`), fixado em `current_date` |
| `frequency_by_route_hour_direction.sql`       | P4, P5      | **início** da viagem (`started_at_time_dim_key`) |
| `median_and_p95_duration_by_route.sql`        | P6          | **início** da viagem |
| `duration_today_vs_same_weekday_baseline.sql` | P7          | **início** da viagem, janela fixa de 4 semanas do mesmo dia da semana |
| `reliability_by_route.sql`                    | P8          | **início** da viagem |
| `avg_speed_by_route_and_hour.sql`             | P9          | **início** da viagem |
| `route_summary_with_trip_details.sql`         | P10         | **início** da viagem, faz join com `trip_details` por `trip_id` |

> **Pendente — painel de `refined.latest_positions` (P11).** Um painel de posição da frota ao
> vivo sobre `refined.latest_positions` é necessário, mas **ainda não foi desenhado**. Seu
> design é de responsabilidade de `.plans/metabase-dashboard-panel-design.md` e é pré-requisito
> antes da implementação do dashboard; o acesso de leitura / visibilidade do datasource para
> `refined.latest_positions` são tratados em
> `.plans/metabase-complementary-implementation_plan_pending.md`. O arquivo de query
> correspondente é adicionado aqui assim que o painel for desenhado.

## Notas de configuração das perguntas nativas

- **Field filters** (`[[ AND {{var}} ]]`) exigem **nomes de tabela totalmente qualificados**
  (sem aliases) para serem resolvidos — as queries aqui são escritas dessa forma de propósito.
- Parâmetros padrão entre os painéis: `date_range` (Field Filter em
  `refined.dim_time.date_actual`, padrão *Previous 30 days*), `route`, `direction`,
  `is_weekend`, `is_circular` e um número `min_trips` para guarda de baixa amostragem (padrão
  `5`). O comentário no cabeçalho de cada arquivo lista os mapeamentos exatos que ele espera.
- O **Report Timezone** deve ser `America/Sao_Paulo` para que os painéis de `current_date`/"hoje"
  (P1–P3, P7) sejam ancorados no dia local de São Paulo, e não em UTC — ver
  `.plans/metabase-complementary-implementation_plan_pending.md`.

## Relacionados

- ADR-0012 — Migração do Power BI para o Metabase self-hosted
- ADR-0013 — Modelo dimensional `refinedtripfacts` (desacoplamento do modelo em relação à ferramenta de BI)
- `automation/bootstrap_metabase.sh` — Provisionamento do Metabase + grant de leitura
- `database/bootstrap/postgres/004_metabase.sql` — Role de leitura / grants de `SELECT` em `refined`
