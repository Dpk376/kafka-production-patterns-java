package com.example.kafka.common.idempotent.infrastructure;

import java.time.OffsetDateTime;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
  int deleteByProcessedAtBefore(OffsetDateTime threshold);
}
