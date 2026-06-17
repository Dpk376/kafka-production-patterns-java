package com.example.kafka.dlq.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "dead_letter")
@SuppressWarnings("NullAway")
public class DeadLetter {

  @Id private UUID id;

  @Column(name = "topic_name", nullable = false)
  private String topic;

  @Column(name = "payload", columnDefinition = "TEXT", nullable = false)
  private String payload;

  @Column(name = "exception_message", columnDefinition = "TEXT")
  private String exceptionMessage;

  @Column(name = "created_at", nullable = false)
  private OffsetDateTime createdAt;

  protected DeadLetter() {
    // JPA
  }

  public DeadLetter(String topic, String payload, String exceptionMessage) {
    this.id = UUID.randomUUID();
    this.topic = topic;
    this.payload = payload;
    this.exceptionMessage = exceptionMessage;
    this.createdAt = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public UUID getId() {
    return id;
  }

  public String getTopic() {
    return topic;
  }

  public String getPayload() {
    return payload;
  }

  public String getExceptionMessage() {
    return exceptionMessage;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }
}
