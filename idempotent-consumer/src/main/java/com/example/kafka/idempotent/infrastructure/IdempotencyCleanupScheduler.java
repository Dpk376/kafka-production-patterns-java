package com.example.kafka.idempotent.infrastructure;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@EnableScheduling
public class IdempotencyCleanupScheduler {

  private static final Logger log = LoggerFactory.getLogger(IdempotencyCleanupScheduler.class);
  private final ProcessedMessageRepository processedMessageRepository;

  @Value("${idempotency.cleanup.retention-days:7}")
  private int retentionDays;

  public IdempotencyCleanupScheduler(ProcessedMessageRepository processedMessageRepository) {
    this.processedMessageRepository = processedMessageRepository;
  }

  @Scheduled(cron = "0 0 2 * * *") // Run daily at 2 AM
  @Transactional
  public void cleanupOldIdempotencyKeys() {
    OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minusDays(retentionDays);
    int deletedCount = processedMessageRepository.deleteByProcessedAtBefore(threshold);
    if (deletedCount > 0) {
      log.info("Cleaned up {} expired idempotency keys older than {}", deletedCount, threshold);
    }
  }
}
