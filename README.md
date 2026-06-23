# Kafka Production Patterns

This repository serves as a **Staff-level reference** for building robust, observable, and resilient event-driven architectures with Apache Kafka and Spring Boot in Java.

It is designed to teach and prove five core production patterns:
1. **Idempotent Consumers** (At-Least-Once processing with Exactly-Once side effects)
2. **Exactly-Once Semantics** (Transactional Read-Process-Write & the Transactional Outbox)
3. **Dead-Letter-Queue Design** (Retry, Classify, Route, Replay)
4. **Consumer-Lag Observability** (Metrics, Dashboards, Alerts)
5. **Transactional Inbox** (Decoupled ingestion and asynchronous idempotency)

## Architecture
See the `docs/HLD.md` and `docs/LLD.md` for a comprehensive overview of the architecture, data flows, and trade-offs.

## Stack
| Component | Version | Notes |
|-----------|---------|-------|
| Java      | 21      | Using records and sealed classes |
| Spring Boot| 3.3.4  | Web, Data JPA, Actuator |
| Spring Kafka | 3.2.x | |
| Apache Kafka | 3.7+ | KRaft mode (No ZooKeeper) |
| PostgreSQL | 16 | Primary data store and idempotency table |
| Micrometer / Prometheus | | Observability pipeline |
| Grafana | | Dashboards |

## Quickstart

**1. Start the Infrastructure**
```bash
make up
```
*(Starts Kafka, PostgreSQL, Prometheus, and Grafana via Docker Compose)*

**2. Verify the Build**
```bash
make verify
```

## Structure
- `docs/`: Architecture Decision Records (ADRs), HLD, LLD, and Runbooks.
- `common/`: Shared config, headers, and exceptions.
- `idempotent-consumer/`: Pattern 1 implementation.
- `exactly-once/`: Pattern 2 implementation.
- `dead-letter-queue/`: Pattern 3 implementation.
- `lag-observability/`: Pattern 4 implementation.
- `transactional-inbox/`: Pattern 5 implementation.
- `load-harness/`: Traffic generator for load testing.

## Contributing
See `CONTRIBUTING.md` for guidelines on conventional commits, branch naming, and code quality expectations.
