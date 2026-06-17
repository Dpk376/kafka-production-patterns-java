package com.example.kafka.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
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
class DlqIntegrationTest {

  @Container
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
  }

  @Autowired private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;

  private KafkaMessageListenerContainer<String, String> container;
  private BlockingQueue<ConsumerRecord<String, String>> dltRecords;

  @BeforeEach
  void setUp() {
    var consumerProps = KafkaTestUtils.consumerProps("dlt-test-group", "true", embeddedKafkaBroker);
    var cf = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
    var containerProps = new ContainerProperties("orders.v1.DLT");

    dltRecords = new LinkedBlockingQueue<>();
    containerProps.setMessageListener((MessageListener<String, String>) dltRecords::add);

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
  void shouldRoutePoisonPillToDlqImmediatelyWithoutRetries() throws Exception {
    // Given
    String payload = "POISON_PAYLOAD";

    // When
    kafkaTemplate.send("orders.v1", "key1", payload).get();

    // Then
    ConsumerRecord<String, String> received = dltRecords.poll(10, TimeUnit.SECONDS);
    assertThat(received).isNotNull();
    assertThat(received.value()).isEqualTo(payload);

    // Assert the exception headers are present
    byte[] exceptionHeader =
        received.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value();
    assertThat(new String(exceptionHeader, StandardCharsets.UTF_8))
        .contains("Simulated invalid state or malformed JSON");
  }
}
