# ADR-0001: Medallion Architecture (Raw → Trusted → Refined)

**Date:** 2026-04-15  
**Status:** Accepted

## Context

The project needs to ingest raw data from the SPTrans API every 2 minutes and make it available to a visualization layer (Power BI) with low latency. The data must be enriched with GTFS information, validated according to quality rules, and made available in different formats depending on the consumer:

- Complete historical data requires efficient analytical queries over large volumes.
- Real-time visualization requires responses with millisecond latency.
- If any transformation step fails, it must be possible to reprocess the original data without loss.

In a flat architecture, storing only the final processed state, a transformation bug or a business-rule change implies irrecoverable loss of the original data.

## Decision

Adopt three data layers with distinct responsibilities and technologies:

| Layer | Storage | Format | Responsibility |
|-------|---------|--------|----------------|
| **Raw** | MinIO (`raw`) | JSON (compressed with Zstandard) | Faithful preservation of the original API data |
| **Trusted** | MinIO (`trusted`) | Parquet (partitioned by year/month/day/hour) | Validated, enriched data ready for analysis |
| **Refined** | PostgreSQL | Tables partitioned by hour | Low-latency queries for visualization and APIs |

Additional internally generated data, such as quality reports and quarantined records, is stored in dedicated buckets (`metadata`, `quarantined`) in the same MinIO instance.

## Alternatives considered

**Flat architecture (final layer only):** Operationally simple, but without any reprocessing capability; any bug fix would require extracting data from the API again, which might no longer be possible.

**Two layers (raw + refined directly):** Removes the complexity of the trusted layer, but mixes responsibilities: the same table would need to be optimized both for reprocessing and for querying, which leads to poor compromises in both directions.

**Traditional data warehouse (for example, Redshift or BigQuery):** A managed single layer, but it introduces dependency on paid cloud services, increasing both operational cost and complexity for a project designed to run locally.

## Consequences

**Positive:**
- Full reprocessing is possible from the raw layer without depending on the API being available.
- Each layer uses the ideal technology for its access pattern: maximum compression in raw, analytical queries in trusted via DuckDB, and low latency in refined via PostgreSQL.
- Clear separation of ownership: transformation failures stay in the trusted layer and do not contaminate raw.
- Natural lineage: it is always possible to trace a refined record back to the original JSON file that produced it.

**Negative / Tradeoffs:**
- Storage is effectively tripled because the same data exists in three forms.
- Higher operational complexity: three distinct storage systems must be available (MinIO with two functional buckets plus PostgreSQL).
- Promotion pipelines between layers introduce latency: data ingested into raw only reaches refined after full execution of `transformlivedata` and `refinedfinishedtrips`.
