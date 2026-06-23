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
exact statistics the panels require. These files are the authoritative implementation of the
panels; the PoC files are not edited to track the dashboard.

## `dashboard_queries/` → panel map

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
| `live_fleet_positions.sql`                    | P11         | live vehicle snapshot; map by `veiculo_lat`/`veiculo_long` |
| `live_fleet_positions_freshness.sql`          | P11         | live snapshot; companion card with active-vehicle count and freshness |

> **P11 — `refined.latest_positions` panel.** The live fleet-position panel reads
> `refined.latest_positions`; the authoritative query implementations live at
> `metabase/dashboard_queries/live_fleet_positions.sql` and
> `metabase/dashboard_queries/live_fleet_positions_freshness.sql`.

## Native-question setup notes

- **Field filters** (`[[ AND {{var}} ]]`) require **fully-qualified table names** (no
  aliases) to resolve — the queries here are written that way deliberately.
- Standard parameters across panels: `date_range` (Field Filter on `refined.dim_time.date_actual`,
  default *Previous 30 days*), `route`, `direction`, `is_weekend`, `is_circular`, and a
  `min_trips` number for low-sample guarding (default `5`). Each file's header comment lists
  the exact mappings it expects.
- The read-only `sptrans_insights` datasource (scoped to the `refined` schema) and the timezone
  are **provisioned automatically** by `automation/bootstrap_metabase.sh` — no manual setup is
  required. The reader role's session timezone is set to `America/Sao_Paulo`, so native-SQL
  `current_date`/"today" panels (P1–P3, P7) anchor on the São Paulo local day rather than UTC;
  the Metabase **Report Timezone** (also `America/Sao_Paulo`) governs timestamp display.

## SPTrans Insights dashboard

The `SPTrans Insights` dashboard is provisioned automatically by `automation/bootstrap_metabase_dashboard.sh` — no manual UI step is needed, except for the auto-refresh setting described below.

### Layout

The dashboard uses Metabase's 24-column grid and is composed of 14 cards organised into four regions:

| Region | Cards | Description |
|---|---|---|
| Left column (KPIs) | P0A, P0B, P1, P2, P3, P11B | Scalars stacked vertically |
| Content row 1 | P4, P9, P5 | Frequency by route/hour, speed, frequency by direction |
| Content row 2 | P6, P7, P8 | Median/P95 duration, historical comparison, duration consistency |
| Map row | P11A, P10 | Live fleet position map + route summary table |

### Global filters

5 filters available in the dashboard filter bar:

| Filter | Type | Affected cards |
|---|---|---|
| Date range | `date/all-options` (default: last 30 days) | P4, P5, P6, P8, P9, P10 |
| Route | `string/=` | P4, P5, P6, P7, P8, P9, P10 |
| Direction | `string/=` | P5, P6 |
| Weekend | `category` | P4, P5, P6 |
| Circular | `category` | P4, P5, P6, P8, P9, P10 |

P0A, P0B, P1, P2, P3, and P7 have fixed time windows and are not affected by global filters. P11A and P11B carry local panel filters (`linha_lt`, `linha_sentido`) that are not exposed in the global filter bar.

### Screenshots

**Upper half — filters, KPIs, and analytics panels**

![Dashboard — upper half](DashboardHigherPageScreenshot.png)

The screenshot shows the dashboard filtered to route `2008-10` over the previous 30 days. The filter bar has **Date range** (Previous 30 Days) and **Route** (2008-10) active; Direction, Weekend, and Circular filters are available but unset.

The left KPI column shows real-time operational figures: last batch timestamp (`Jun 23, 2026, 6:04 PM`), last batch trip volume (`124`), trips finished today (`21,526`), active routes (`1,128`), vehicles that finished trips today (`7,830`), and live vehicle count (`11,197`).

Content row 1 contains: the **Trips per route per hour** table (14 rows for direction 1, hours 9–23), the **Average speed by route and hour** line chart (speed between 9 and 14 km/h across the day), and the **Frequency by direction** bar chart (direction 1 ≈ 44 trips, direction 2 ≈ 66 trips).

Content row 2 shows: **Median and P95 duration** (median ≈ 18 min, P95 ≈ 28 min for route 2008-10), **Today vs historical baseline** (today's median = 19.6 min; baseline absent — no same-weekday data in the prior 4 weeks), and **Trip duration consistency by route** (consistency index ≈ 0.21 — coefficient of variation of trip duration).

---

**Lower half — live map and route summary**

![Dashboard — lower half](DashboardLowerPageScreenshot.png)

On the left, the **Live fleet positions** map shows the real-time distribution of vehicles in operation across São Paulo, with blue pins concentrated in the highest-density bus corridors.

On the right, the **Route summary** table details, per route and direction, the origin and destination terminals (`first_stop_name`, `last_stop_name`), the `is_circular` flag, total trips in the selected period (`total_trips`), median trip duration in minutes (`median_duration_minutes`), average speed (`avg_speed_kmh`), and duration consistency (`duration_consistency`). The table shows the first 11 of 177 rows for the selected period.

---

### Auto-refresh (one-time manual step)

Auto-refresh cannot be set via the API in Metabase v0.52.9. After provisioning, configure it once in the UI:

1. Open the `SPTrans Insights` dashboard
2. Click the clock/refresh icon in the top-right toolbar
3. Select **1 minute**

> After running `bootstrap_metabase_dashboard.sh`, log out and log back in to Metabase — a simple page refresh does not pick up the new configuration.

## Related

- ADR-0012 — Power BI → self-hosted Metabase migration
- ADR-0013 — `refinedtripfacts` dimensional model (decoupling the model from the BI tool)
- `automation/bootstrap_metabase.sh` — Metabase provisioning + reader grant
- `database/bootstrap/postgres/004_metabase.sql` — reader role / `SELECT` grants on `refined`
