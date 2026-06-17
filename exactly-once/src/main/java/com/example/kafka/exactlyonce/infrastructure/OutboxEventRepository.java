package com.example.kafka.exactlyonce.infrastructure;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

  /**
   * Finds unpublished events. Uses SELECT ... FOR UPDATE SKIP LOCKED to prevent multiple instances
   * of the relay from picking up the same records, ensuring exactly-once processing safely.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints({
    @QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2") // SKIP LOCKED
  })
  @Query("SELECT e FROM OutboxEvent e WHERE e.publishedAt IS NULL ORDER BY e.createdAt ASC")
  List<OutboxEvent> findUnpublishedAndLock(Pageable pageable);
}
