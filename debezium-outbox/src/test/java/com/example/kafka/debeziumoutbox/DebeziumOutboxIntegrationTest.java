package com.example.kafka.debeziumoutbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.kafka.debeziumoutbox.application.OrderApplicationService;
import com.example.kafka.debeziumoutbox.infrastructure.OutboxEventRepository;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
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
class DebeziumOutboxIntegrationTest {

  @Container
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withCommand("postgres", "-c", "wal_level=logical");

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
  }

  @Autowired private OrderApplicationService applicationService;

  @Autowired private OutboxEventRepository outboxEventRepository;

  @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;

  private KafkaMessageListenerContainer<String, String> container;
  private BlockingQueue<ConsumerRecord<String, String>> records;

  @BeforeEach
  void setUp() {
    // Set up test consumer to read from the outbox topic
    var consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
    var cf = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
    var containerProps = new ContainerProperties("orders.v1");

    records = new LinkedBlockingQueue<>();
    containerProps.setMessageListener((MessageListener<String, String>) records::add);

    container = new KafkaMessageListenerContainer<>(cf, containerProps);
    container.start();
    ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  @AfterEach
  void tearDown() {
    if (container != null) {
      container.stop();
    }
  }

  @Test
  void shouldRelayOutboxEventToKafka() throws Exception {
    // Given
    UUID orderId = UUID.randomUUID();

    // When
    applicationService.createOrder(orderId, "C123");

    // Then - verify the outbox record was saved and eventually marked published
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              var events = outboxEventRepository.findAll();
              assertThat(events).hasSize(1);
              assertThat(events.get(0).getPublishedAt()).isNotNull();
            });

    // Verify Kafka received the message
    ConsumerRecord<String, String> received =
        records.poll(10, java.util.concurrent.TimeUnit.SECONDS);
    assertThat(received).isNotNull();
    assertThat(received.key()).isEqualTo(orderId.toString());
    assertThat(received.value()).contains("C123");
  }
}
