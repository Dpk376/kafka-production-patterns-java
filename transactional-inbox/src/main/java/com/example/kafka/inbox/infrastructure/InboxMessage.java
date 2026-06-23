package com.example.kafka.inbox.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "inbox_message")
public class InboxMessage {

  @Id
  @Column(name = "message_id", updatable = false, nullable = false)
  private String messageId;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
  private String payload;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @org.springframework.lang.Nullable
  @Column(name = "processed_at")
  private OffsetDateTime processedAt;

  @SuppressWarnings("NullAway")
  protected InboxMessage() {}

  public InboxMessage(String messageId, String payload) {
    this.messageId = messageId;
    this.payload = payload;
    this.createdAt = OffsetDateTime.now(java.time.ZoneId.systemDefault());
    this.processedAt = null;
  }

  public String getMessageId() {
    return messageId;
  }

  public String getPayload() {
    return payload;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  @org.springframework.lang.Nullable
  public OffsetDateTime getProcessedAt() {
    return processedAt;
  }

  public void markProcessed() {
    this.processedAt = OffsetDateTime.now(java.time.ZoneId.systemDefault());
  }
}
