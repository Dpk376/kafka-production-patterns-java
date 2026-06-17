# ADR 0004: DLQ per Topic instead of Shared DLQ

## Status
Accepted

## Context
When messages repeatedly fail processing, they are routed to a Dead Letter Queue (DLQ). We can either route all failed messages across the application to a single shared DLQ topic, or create a specific DLQ topic for each primary topic (e.g., `orders.DLT`).

## Decision
We will use a **DLQ per topic** design (e.g., `<original-topic>.DLT`).

## Rationale
- **Schema Enforcement**: A shared DLQ mixes messages with different schemas. If topics use strict schema validation (e.g., Avro/Protobuf via Schema Registry), producing mixed types to a single DLQ breaks.
- **Access Control**: Topic-level ACLs are easier to maintain when DLQs mirror their primary topics.
- **Replayability**: Replaying messages from a specific DLQ back to its specific source topic is simpler than filtering a massive shared DLQ.

## Consequences
- Increased number of topics to manage in the cluster.
- Dashboards and monitoring must aggregate metrics across multiple DLQ topics.
