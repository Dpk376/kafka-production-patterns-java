# ADR 0002: Manual Acknowledgment over Auto-Commit

## Status
Accepted

## Context
Kafka consumers can automatically commit offsets at a set interval (`enable.auto.commit=true`) or rely on application logic to manually acknowledge offsets (`enable.auto.commit=false`). We must choose the default configuration for the patterns demonstrated in this repository.

## Decision
We will disable auto-commit (`enable.auto.commit=false`) and use `AckMode.MANUAL` or `AckMode.MANUAL_IMMEDIATE` across all consumer implementations.

## Rationale
- **At-least-once Guarantee**: Auto-commit can acknowledge a message before it is successfully processed if the application crashes after polling but before finishing work. Manual acks ensure a message is only committed *after* the business logic and side effects are durably stored.
- **Control**: Idempotent and transactional processing patterns require explicit boundaries of when a message is considered "done". Manual acks provide this control.

## Consequences
- Developers must explicitly call `Acknowledgment.acknowledge()` or rely on Spring Kafka's container to ack after the listener method completes successfully.
- If processing fails and is not caught, the message will not be acked, leading to redelivery.
