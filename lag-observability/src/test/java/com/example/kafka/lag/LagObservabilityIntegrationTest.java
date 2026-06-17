package com.example.kafka.lag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class LagObservabilityIntegrationTest {

  @Autowired private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired private TestRestTemplate restTemplate;

  @Test
  void shouldExposeConsumerLagMetrics() {
    // Given - send 10 messages rapidly
    for (int i = 0; i < 10; i++) {
      kafkaTemplate.send("orders.v1", "key-" + i, "payload");
    }

    // Then - verify that Micrometer exposes the lag metric
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              String metrics = restTemplate.getForObject("/actuator/prometheus", String.class);
              // The exact metric name depends on the micrometer mapping, usually:
              // kafka_consumer_fetch_manager_records_lag_max
              assertThat(metrics).contains("kafka_consumer_fetch_manager_records_lag");
            });
  }
}
