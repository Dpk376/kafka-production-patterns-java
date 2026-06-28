package com.example.kafka.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "outbox_event")
@SuppressWarnings("NullAway") // JPA initializes fields via reflection
public class OutboxEvent {

  @Id
  @Column(name = "event_id")
  private UUID eventId;

  @Column(name = "aggregate_type", nullable = false)
  private String aggregateType;

  @Column(name = "aggregate_id", nullable = false)
  private String aggregateId;

  @Column(name = "topic", nullable = false)
  private String topic;

  @Column(name = "message_key", nullable = false)
  private String messageKey;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
  private String payload;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "published_at")
  private OffsetDateTime publishedAt;

  protected OutboxEvent() {}

  public OutboxEvent(
      UUID eventId,
      String aggregateType,
      String aggregateId,
      String topic,
      String messageKey,
      String payload) {
    this.eventId = eventId;
    this.aggregateType = aggregateType;
    this.aggregateId = aggregateId;
    this.topic = topic;
    this.messageKey = messageKey;
    this.payload = payload;
    this.createdAt = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public void markPublished() {
    this.publishedAt = OffsetDateTime.now(ZoneOffset.UTC);
  }

  // Getters

  public UUID getEventId() {
    return eventId;
  }

  public String getAggregateType() {
    return aggregateType;
  }

  public String getAggregateId() {
    return aggregateId;
  }

  public String getTopic() {
    return topic;
  }

  public String getMessageKey() {
    return messageKey;
  }

  public String getPayload() {
    return payload;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public OffsetDateTime getPublishedAt() {
    return publishedAt;
  }
}
