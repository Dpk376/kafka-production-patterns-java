# ADR 0001: Use KRaft instead of ZooKeeper

## Status
Accepted

## Context
Apache Kafka has historically relied on Apache ZooKeeper for cluster metadata management. With KIP-500, Kafka introduced KRaft (Kafka Raft Metadata mode) to remove the ZooKeeper dependency, simplifying architecture and improving scalability. We need to decide the deployment mode for our reference repository.

## Decision
We will use Apache Kafka in KRaft mode for our `docker-compose.yaml` and all integration tests. ZooKeeper will not be included.

## Rationale
- **Simplicity**: Operating a single process (broker acting as controller and broker) via Docker Compose reduces infrastructure footprint.
- **Future-proofing**: ZooKeeper is deprecated and removed in Kafka 4.0. Using KRaft prepares the repository for modern production deployments.
- **Startup Time**: KRaft-based clusters start faster, which significantly improves the developer loop and Testcontainers integration test execution time.

## Consequences
- We will not use ZooKeeper-specific CLI tools or APIs.
- The reference repository will only be compatible with Kafka versions supporting KRaft (3.3+).
