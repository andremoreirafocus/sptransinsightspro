# ADR-0003: Data modeling and architecture for trip-duration detection

**Date:** 2026-05-11  
**Status:** Accepted

## Context

The central objective of the positions flow is to calculate the duration of trips performed by buses throughout the day. To achieve that, storing historical positions is not enough: it is necessary to identify, reliably, when each trip starts and when it ends.

The operational definition adopted for this problem depends on the behavior of the bus relative to the terminals of its route. In practical terms, start and end detection requires identifying the moments when the vehicle approaches the first stop and the last stop of the corresponding route.

Because the position of those terminals varies according to the route and the operational direction of the trip, the pipeline needed an intermediate structure that could relate each operational trip to the coordinates of its first and last stop. That information needed to be available at transformation time so distance calculation could be done once and propagated to the following stages.

## Decision

Create a trip-details table derived from GTFS specifically to support the detection of trip starts and trip ends.

This table should concentrate, for each operational trip, the attributes required to enrich bus positions with terminal context:

- trip identifier (`trip_id`)
- first-stop position
- last-stop position

This will make it possible for the transformation pipeline to enrich each position record with:

- first-stop coordinates
- last-stop coordinates
- distance between the current bus position and the first stop
- distance between the current bus position and the last stop

As a result, the trusted-layer positions table should be intentionally produced as an enriched and denormalized dataset, ready to be consumed by the completed-trip detection stage.

## Rationale

Concentrating trip details only on the attributes needed for terminal context is beneficial because the problem to solve is not to reconstruct the full GTFS semantics during trip detection, but to allow the downstream pipeline to identify efficiently when a trip started and when it ended.

This modeling brings the following benefits:

- it avoids repeating GTFS joins and context reconstruction in the trip-detection stage when that work can be resolved earlier
- it anticipates the enrichment cost into the positions transformation stage, where it is calculated only once per position
- it allows the completed-trip identification pipeline to focus only on analyzing how terminal distances vary over time in order to detect starts, ends, and intervals between trips
- it simplifies downstream reasoning: the refined stage operates on already contextualized positions instead of having to combine telemetry, GTFS, and detection rules at the same time
- it reduces the coupling between trip-detection logic and the raw structure of GTFS tables

In other words, the decision was not to model GTFS generically and only later decide how to use it, but to prepare explicitly the attributes that make trip detection viable and efficient in the rest of the architecture.

## Consequences

**Positive:**
- the positions transformation pipeline should produce data ready for analyzing trip evolution without requiring additional enrichment in refined
- the completed-trip identification pipeline should be able to focus on detecting starts, ends, and idle time between trips without repeating GTFS integration logic
- the computational and cognitive cost of trip detection becomes more clearly distributed across layers: terminal context in trusted, trip-fact calculation in refined

**Negative / Tradeoffs:**
- the trusted layer carries enrichment specific to the trip-detection problem instead of only a neutral structural transformation
- the trip-details table is intentionally oriented to the business problem and therefore does not replace other analytical needs that may exist over GTFS data
- future changes in how trip starts and trip ends are detected may require revisiting the attributes maintained in trip details

## Alternatives considered

**Calculate terminal distances only in the completed-trip identification pipeline:** this would keep transformation simpler, but it would push both the computational cost and the complexity of reconstructing GTFS context for every position into the detection stage.

**Use broader GTFS tables directly during trip detection:** this would preserve more generality, but it would increase downstream complexity and mix structural enrichment with trip-fact calculation.

**Model the trusted layer without terminal denormalization:** this would reduce per-record redundancy, but it would make the refined stage more expensive and less focused by requiring additional joins and calculations exactly where the objective should be only to detect completed trips.
