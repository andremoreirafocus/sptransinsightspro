# ADR-0012: Power BI to self-hosted Metabase migration

**Date:** 2026-05-29  
**Status:** Accepted

## Context

The project’s analytics visualization layer currently depends on Power BI, while data is already produced and made available in the local containerized environment.

The current implementation creates architectural and operational limitations:
- Recurring licensing cost remains for dashboard consumption and evolution.
- Evolution of the analytics layer remains dependent on an external proprietary platform and its roadmap.
- Full report authoring is effectively restricted to Power BI Desktop on Windows.
- In a future cloud runtime, there is potential additional cost from data egress (`data transfer out`) to consume data in an external tool.

On the other hand, the project already has:
- an analytical PostgreSQL database (`postgres`) with curated data in the refined layer;
- standardized operations through Docker Compose;
- versioned bootstrap and configuration artifacts.

## Decision

Replace Power BI with self-hosted Metabase as the standard analytics visualization platform.

The platform will operate under the following architectural boundaries:

- Metabase consumes curated data from the refined layer in the analytical PostgreSQL instance (`postgres`).
- The Airflow metadata/orchestration database (`airflow_postgres`) remains isolated from the BI layer.
- Platform adoption and dashboard migration are treated as separate workstreams: this ADR covers the platform decision; the migration plan covers functional dashboard conversion.

## Alternatives considered

**Keep Power BI as the primary platform**

Rejected because it keeps recurring licensing cost, external proprietary platform dependency, and the operational authoring limitation tied to Windows.

**Permanent hybrid strategy (Power BI + Metabase)**

Rejected as a target architecture because it keeps operational and governance duplication across dashboards, reducing simplification and standardization gains.

**Adopt another proprietary cloud BI platform as a direct replacement**

Rejected at this stage because it keeps external licensing dependency and does not meet the self-hosted operational control objective compatible with the project’s current stage.

## Consequences

**Positive:**
- Eliminates proprietary BI licensing cost.
- Reduces potential cloud egress cost by keeping analytical consumption within the same infrastructure.
- Removes dependency on an external proprietary platform for the evolution of the visualization layer.
- Enables web-based consumption and authoring, removing dependency on a heavy desktop client and reducing support/onboarding friction across Linux/macOS/Windows teams.
- Improves fit with the current stack through direct integration with the existing analytical PostgreSQL instance.
- Increases operational control and governance of the BI platform under the project’s direct responsibility.
- Improves operational reproducibility through versioned infrastructure and setup artifacts.
- Better prepares the platform for future AWS evolution with a reusable containerized runtime pattern.

**Negative / Tradeoffs:**
- The team assumes direct responsibility for BI platform operations, updates, and availability.
- Migration requires dashboard conversion effort and parity validation.
- Functional differences between Power BI and Metabase may require modeling and visualization adaptations for some dashboards.
