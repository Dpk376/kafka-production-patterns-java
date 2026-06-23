package com.example.kafka.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.kafka.inbox.domain.OrderEvent;
import com.example.kafka.inbox.infrastructure.InboxMessage;
import com.example.kafka.inbox.infrastructure.InboxMessageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class TransactionalInboxIntegrationTest {

  @Container
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
  }

  @Autowired private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired private InboxMessageRepository repository;

  @Autowired private ObjectMapper objectMapper;

  @Test
  void shouldProcessMessageExactlyOnceViaInboxWhenDuplicateEventsAreReceived() throws Exception {
    // Given
    String orderId = UUID.randomUUID().toString();
    OrderEvent event = new OrderEvent(orderId, "C123", 100.0, "PENDING");
    String payload = objectMapper.writeValueAsString(event);

    // When - sending two identical messages (simulating a duplicate from Producer retry)
    // Both messages have the same key (orderId). The consumer uses the key as the dedup key.
    var unused1 = kafkaTemplate.send("orders.events", orderId, payload);
    var unused2 = kafkaTemplate.send("orders.events", orderId, payload);

    // Then - verify the record was saved exactly once in the inbox and then processed
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              // Should only be 1 record despite 2 messages sent
              assertThat(repository.count()).isEqualTo(1);
            });
            
    // Wait for the scheduler to pick it up and process it
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              InboxMessage message = repository.findById(orderId).orElseThrow();
              assertThat(message.getProcessedAt()).isNotNull();
            });
  }
}
