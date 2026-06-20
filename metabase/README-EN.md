# Metabase (BI-as-code)

Source-of-truth assets for the self-hosted Metabase BI platform (ADR-0012). Metabase
consumes the governed `refined` layer of the analytical PostgreSQL (`postgres` /
`sptrans_insights`) and owns the **executable dashboard questions**.

## Why these live here (and not in the pipeline)

ADR-0013 decouples the **analytical model** (the `refined.trip_facts` star schema — a
tool-agnostic contract, versioned in the data layer) from the **BI tool**. The symmetric
half of that rule: the **executable panel queries are a Metabase concern**, not a pipeline
concern. They carry filters, parameters, route selectors, sorting and display logic that
belong to the question, not to the data contract.

Keeping them here (rather than under `dags-dev/refinedtripfacts/`) means:

- ownership is explicit — the pipeline owns the model + a proof-of-concept; Metabase owns
  the questions;
- they are never swept into the Airflow runtime by `scripts/promote_pipeline.py` (that
  rsync syncs the whole pipeline folder into `airflow/dags/`); these queries have nothing
  to do with the DAG runtime.

## Relationship to the pipeline's `queries/`

`dags-dev/refinedtripfacts/queries/` holds **non-authoritative, filter-free proof-of-concept
SQL** that proved the dimensional model can answer every panel. The files here are the
**authoritative** native questions: they add the Metabase field filters, variables and the
exact statistics the panels require. Where the two differ, the **design doc**
(`.plans/metabase-dashboard-panel-design.md`) is the source of truth and these files
implement it; the PoC files are not edited to track the dashboard.

## Layout

```
metabase/
  README.md
  dashboard_queries/   # authoritative Metabase native-question SQL (P0–P10)
```

### `dashboard_queries/` → panel map

| File | Panel(s) | Anchor (date logic) |
| --- | --- | --- |
| `latest_batch_freshness.sql`                  | P0          | `logic_date` (operational freshness only) |
| `today_kpis.sql`                              | P1, P2, P3  | trip **completion** (`ended_at_time_dim_key`), fixed to `current_date` |
| `frequency_by_route_hour_direction.sql`       | P4, P5      | trip **start** (`started_at_time_dim_key`) |
| `median_and_p95_duration_by_route.sql`        | P6          | trip **start** |
| `duration_today_vs_same_weekday_baseline.sql` | P7          | trip **start**, fixed 4-week same-weekday window |
| `reliability_by_route.sql`                    | P8          | trip **start** |
| `avg_speed_by_route_and_hour.sql`             | P9          | trip **start** |
| `route_summary_with_trip_details.sql`         | P10         | trip **start**, joins `trip_details` on `trip_id` |

> **Pending — `refined.latest_positions` panel (P11).** A live fleet-position panel over
> `refined.latest_positions` is required but **not yet designed**. Its design is owned by
> `.plans/metabase-dashboard-panel-design.md` and is a prerequisite before the dashboard is
> implemented; reader access / datasource visibility for `refined.latest_positions` are
> handled by `.plans/metabase-complementary-implementation_plan_pending.md`. The query file
> for it lands here once the panel is designed.

## Native-question setup notes

- **Field filters** (`[[ AND {{var}} ]]`) require **fully-qualified table names** (no
  aliases) to resolve — the queries here are written that way deliberately.
- Standard parameters across panels: `date_range` (Field Filter on `refined.dim_time.date_actual`,
  default *Previous 30 days*), `route`, `direction`, `is_weekend`, `is_circular`, and a
  `min_trips` number for low-sample guarding (default `5`). Each file's header comment lists
  the exact mappings it expects.
- **Report Timezone** must be `America/Sao_Paulo` so `current_date`/"today" panels (P1–P3, P7)
  anchor on São Paulo local day rather than UTC — see
  `.plans/metabase-complementary-implementation_plan_pending.md`.

## Related

- ADR-0012 — Power BI → self-hosted Metabase migration
- ADR-0013 — `refinedtripfacts` dimensional model (decoupling the model from the BI tool)
- `automation/bootstrap_metabase.sh` — Metabase provisioning + reader grant
- `database/bootstrap/postgres/004_metabase.sql` — reader role / `SELECT` grants on `refined`
