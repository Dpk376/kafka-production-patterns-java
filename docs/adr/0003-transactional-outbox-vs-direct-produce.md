# ADR 0003: Transactional Outbox for External Sinks

## Status
Accepted

## Context
When a service consumes a Kafka message, updates a local database, and produces a new Kafka message, ensuring consistency between the database update and the outgoing message is difficult. Direct producing is prone to dual-write failures (e.g., database commits but Kafka produce fails). 

## Decision
For exactly-once semantics involving an external sink (PostgreSQL), we will implement the **Transactional Outbox** pattern rather than relying solely on direct Kafka producing.

## Rationale
- **Kafka's EOS Boundaries**: Kafka's Exactly-Once Semantics (`isolation.level=read_committed` and transactional producers) only provide guarantees for Kafka-to-Kafka topologies. They do not magically extend to PostgreSQL.
- **Dual-Write Prevention**: The Outbox pattern persists the business state and the outgoing message in the same ACID database transaction. A separate relay process reads the outbox table and reliably publishes to Kafka, ensuring eventual consistency without data loss.

## Consequences
- Increased architectural complexity due to the outbox table and relay mechanism.
- Slight increase in end-to-end latency since messages are stored in the database before being published to Kafka.
- Storage costs for the outbox table until records are published and pruned.
