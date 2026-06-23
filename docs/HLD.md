# High-Level Design: Kafka Production Patterns

## 1. Context and Goals
This reference repository demonstrates five production-grade Apache Kafka patterns in Java using Spring Boot. It provides a foundation for robust, observable, and resilient event-driven architectures.

### Goals
- Prove **Idempotent Consumers** to handle redeliveries safely.
- Prove **Exactly-Once Semantics (EOS)** for Kafka-to-Kafka and Kafka-to-Database workloads.
- Implement a robust **Dead-Letter-Queue (DLQ)** with intelligent routing and replay.
- Expose **Consumer-Lag Observability** for operational readiness.
- Prove **Transactional Inbox** for decoupled ingestion and async idempotency.

### Non-Goals
- Cross-cluster replication (e.g., MirrorMaker 2).
- Schema evolution policies and Schema Registry deployment.
- High-performance tuning (e.g., batching optimizations for million-message throughput).
- Kubernetes deployment manifests (Docker Compose is used for local validation).
- **Kafka Security (TLS/SASL/ACLs)**: This repo demonstrates logic patterns, not infrastructure hardening.
- **Distributed Tracing (e.g., Micrometer Tracing)**: Correlation IDs and span propagation are omitted to keep the code focused.

---

## 2. Pattern Map

| Pattern | Problem Solved | Failure Prevented | Guarantee Provided |
|---------|----------------|-------------------|--------------------|
| **Idempotent Consumer** | Duplicate message deliveries due to network retries or consumer rebalances. | Unintended duplicate state changes (e.g., charging a user twice). | At-least-once delivery with exactly-once side effects. |
| **Exactly-Once Semantics** | Inconsistent state during read-process-write cycles and dual-write anomalies. | Partial commits or lost messages when components fail mid-transaction. | Exactly-once processing (Kafka-to-Kafka) and eventual consistency (Outbox). |
| **Dead-Letter-Queue** | Poison pill messages blocking partitions. | Infinite retry loops causing massive consumer lag. | Fault tolerance, topic unblocking, and replayability. |
| **Lag Observability** | Invisible buildup of unprocessed messages. | Silent system failure and breached SLAs. | Operational visibility and alerting on processing delays. |
| **Transactional Inbox** | Slow processing blocking partitions and duplicate messages. | Rebalance loops from slow execution and idempotency failures. | Decoupled fast ingestion and async exactly-once business execution. |

---

## 3. Delivery Semantics Table

| Semantics | Description | Demonstrated In | Configuration Flags (Key) |
|-----------|-------------|-----------------|---------------------------|
| **At-Most-Once** | Message may be lost but never redelivered. | N/A | `enable.auto.commit=true`, `acks=0` |
| **At-Least-Once** | Message is never lost but may be redelivered. | `idempotent-consumer` | `enable.auto.commit=false`, `acks=all`, Manual Ack |
| **Exactly-Once (Kafka)**| Message processed exactly once end-to-end. | `exactly-once` | `transactional.id`, `isolation.level=read_committed` |

*Note: Kafka EOS strictly applies to Kafka-to-Kafka pipelines. For external sinks like PostgreSQL, we implement the Transactional Outbox pattern.*

---

## 4. Context Diagram
*(Diagrams are generated using Mermaid)*

```mermaid
graph TD
    subgraph Infrastructure
        K[Kafka Broker - KRaft]
        DB[(PostgreSQL 16)]
        P[Prometheus]
        G[Grafana]
    end

    subgraph Producers
        LH[Load Harness]
    end

    subgraph Consumers
        IC[Idempotent Consumer]
        EO[Exactly-Once Service]
        DLQ[DLQ Service]
        IB[Inbox Service]
    end

    LH -- Produces --> K
    K -- Consumes --> IC
    K -- Consumes --> EO
    K -- Consumes --> DLQ
    K -- Consumes --> IB

    IC -- Reads/Writes --> DB
    EO -- Outbox writes --> DB
    IB -- Inbox writes --> DB

    IC -. Metrics .-> P
    EO -. Metrics .-> P
    DLQ -. Metrics .-> P
    IB -. Metrics .-> P
    K -. Metrics .-> P

    P -. Queries .-> G
```

---

## 5. Topic Design

Topics are designed with high availability and scalability in mind:

| Topic Pattern | Partitions | RF | Key Strategy | Retention | Rationale |
|---------------|------------|----|--------------|-----------|-----------|
| `*.events` | 3 | 1 (local) | Business ID (e.g., `orderId`) | 7 Days | Keys guarantee ordering per entity. Partitions allow concurrency. |
| `*.DLT` | 3 | 1 (local) | Same as original | 30 Days | Extended retention for operational recovery and manual replay. |

*Why Keys Matter*: Kafka guarantees ordering only within a single partition. Using a stable business key (like an Order ID) ensures all events for that entity are processed in strict chronological order, which is critical for state machines and idempotency.

---

## 6. Data-Flow Diagrams

### Happy-Path Consume + Commit
```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer
    participant DB as PostgreSQL
    B->>C: Poll() returns Record
    C->>DB: Begin TX
    C->>DB: Apply Business Logic
    C->>DB: Commit TX
    C->>B: Manual Acknowledge (Commit Offset)
```

### Duplicate-Delivery Dedup Path (Idempotent Consumer)
```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer
    participant DB as PostgreSQL
    B->>C: Poll() returns Record (Duplicate)
    C->>DB: Begin TX
    C->>DB: Insert Dedup Key
    DB-->>C: Unique Constraint Violation
    C->>DB: Rollback TX
    Note over C: Consumer MUST verify violation<br>is specifically the dedup key
    C->>B: Swallow Exception & Manual Acknowledge
```

### Transactional Read-Process-Write (Kafka-to-Kafka)
```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer
    participant P as Producer
    B->>C: Poll() returns Record
    C->>P: Begin Kafka TX
    C->>P: Produce Derived Record
    C->>P: Send Offsets to TX
    P->>B: Commit TX
```

### Transactional Outbox (EOS for External Sinks)
```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer
    participant DB as PostgreSQL
    participant R as Outbox Relay
    
    B->>C: Poll() returns Record
    C->>DB: Begin DB TX
    C->>DB: Apply Business Logic
    C->>DB: Insert Outbox Event
    C->>DB: Commit DB TX
    C->>B: Manual Acknowledge
    
    loop Async Polling/Tail
        R->>DB: Fetch Unpublished Events
        R->>B: Publish to Kafka
        B-->>R: Ack
        R->>DB: Mark as Published
    end
```

### Transactional Inbox (Decoupled Idempotency)
```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer
    participant DB as PostgreSQL
    participant P as Poller / Processor
    
    B->>C: Poll() returns Record
    C->>DB: Insert Inbox Message (Dedup Key)
    Note right of DB: Unique Constraint Violation<br/>silently swallowed if duplicate
    C->>B: Manual Acknowledge
    
    loop Async Polling
        P->>DB: Fetch Unprocessed FOR UPDATE SKIP LOCKED
        P->>DB: Apply Business Logic (Tx)
        P->>DB: Mark as Processed (Tx)
    end
```

### Retry -> Backoff -> DLQ
```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer
    participant D as DLT (Topic)
    B->>C: Poll() returns Poison Pill
    C->>C: Process Fails (Exception)
    C->>C: Wait (Exponential Backoff)
    C->>C: Retry 1..N
    C->>D: Produce to DLT with Error Headers
    C->>B: Acknowledge Original Record
```

---

## 7. Trade-offs

- **Throughput Cost of EOS**: Enabling transactional producers introduces overhead (fetching producer IDs, committing transaction markers). This reduces overall maximum throughput compared to at-least-once semantics.
- **Latency Cost of Idempotency**: Synchronous database checks for deduplication (e.g., verifying a unique constraint) add a database roundtrip to the critical processing path.
- **Storage Cost**: Implementing the `processed_message` table for idempotency and the `outbox_event` table for the Outbox pattern increases the storage footprint in PostgreSQL. Old records require a cron-based cleanup mechanism.
