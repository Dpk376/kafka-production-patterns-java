package com.example.kafka.idempotent.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;

@Entity
@Table(name = "processed_message")
@SuppressWarnings("NullAway")
public class ProcessedMessage {

  @Id
  @Column(name = "dedup_key", nullable = false, unique = true)
  private String dedupKey;

  @Column(name = "processed_at", nullable = false)
  private OffsetDateTime processedAt;

  protected ProcessedMessage() {
    // JPA required no-args constructor
  }

  public ProcessedMessage(String dedupKey, OffsetDateTime processedAt) {
    this.dedupKey = dedupKey;
    this.processedAt = processedAt;
  }

  public String getDedupKey() {
    return dedupKey;
  }

  public OffsetDateTime getProcessedAt() {
    return processedAt;
  }
}
