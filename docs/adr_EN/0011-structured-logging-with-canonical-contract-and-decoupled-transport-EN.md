# ADR-0011: Structured logging with canonical contract and decoupled transport

**Date:** 2026-05-12  
**Status:** Accepted

## Context

The platform components (microservices and pipelines) currently use Python's `logging` module with free-text messages and local file handlers. This design works for basic local debugging but creates relevant operational limitations:

1. **Low standardisation across components**: each service defines its log format and content independently, making correlation difficult.
2. **Limited querying**: free-text logs reduce the ability to filter by semantic fields (`execution_id`, `correlation_id`, `event`, `status`).
3. **Local coupling**: dependency on local files makes observability harder in containerised environments.
4. **Setup duplication**: logging configuration is repeated at multiple points across the monorepo.

As the project continues to evolve with containerised execution and progressive integration with cloud services, a more consistent, reusable, and portable operational logging foundation is required.

## Decision

Adopt a structured logging model with a single canonical contract, implemented as a shared library in the infrastructure layer, emitting logs to `stdout` (one JSON per line), with transport decoupled from the application.

### 1. Canonical log contract

Every operational log must follow a standardised minimal contract, serialised as one JSON line.

Required fields:
- `timestamp`
- `level`
- `service`
- `component`
- `event`
- `message`

Recommended fields:
- `execution_id`
- `correlation_id`
- `status`
- `error_type`
- `error_message`
- `metadata` (object)

Conventions:
- timestamp in UTC
- standardised levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- stable `event` naming (snake_case)

### 2. Shared library in `infra`

Create a logging library in the infrastructure layer to:
- standardise the emission of structured logs
- avoid ad-hoc setup per component
- guarantee reuse across microservices and pipelines

The services/pipelines layer must not know the details of log transport or storage.

### 3. Transport decoupled from the application

The library must not implement adapters for specific observability backends.

Strategy:
- the application emits structured logs to `stdout`
- collection and forwarding happen outside the application (e.g. Promtail in the containerised environment)
- in cloud environments, capture and delivery follow the runtime/platform (e.g. CloudWatch via Lambda/ECS)

This preserves portability: the logging contract and API remain stable, while the backend and transport mechanism vary per environment without coupling to business code.

### 4. Initial containerised observability stack

Adopt Grafana + Loki + Promtail as the centralised log solution for the current containerised environment, prioritising operational simplicity and low maintenance cost at this stage of the project.  
In this design, Promtail acts as the external transport/collection agent, outside the application.

## Alternatives considered

**Keep text-based logging to local files**

Rejected because it maintains low standardisation and low cross-component correlation capability.

**ELK/OpenSearch as the initial solution**

Rejected at this stage due to greater operational complexity relative to the current maturity of the project.

**Couple logging directly to a specific backend**

Rejected because it would reduce portability and make migration between environments harder.

## Consequences

**Positive:**
- Standardised operational logs across the entire project.
- Better search, filtering, and cross-execution correlation capability.
- Reduced logging code duplication.
- Reusable architectural foundation for future observability evolution.

**Negative / Tradeoffs:**
- Initial migration effort for existing components.
- Contract discipline required to avoid field drift.
- Label/field governance needed to prevent excessive cardinality.

## Implementation status

Initial local implementation completed for structured log observability in the containerised environment:
- `loki` added to `docker-compose.yml` with configuration in `observability/loki/loki-config.yml`
- `promtail` added to `docker-compose.yml` with configuration in `observability/promtail/promtail-config.yml`
- `grafana` added to `docker-compose.yml` with Loki datasource provisioned in `observability/grafana/provisioning/datasources/loki.yml`
