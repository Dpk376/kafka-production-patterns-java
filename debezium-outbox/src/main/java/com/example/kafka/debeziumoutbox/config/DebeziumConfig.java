package com.example.kafka.debeziumoutbox.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DebeziumConfig {

  @Value("${spring.datasource.url}")
  private String datasourceUrl = "";

  @Value("${spring.datasource.username}")
  private String datasourceUsername = "";

  @Value("${spring.datasource.password}")
  private String datasourcePassword = "";

  @Bean
  public io.debezium.config.Configuration debeziumConfiguration() {
    String hostname = "localhost";
    int port = 5432;
    String dbName = "kafka_patterns";

    if (datasourceUrl.startsWith("jdbc:postgresql://")) {
      java.net.URI uri = java.net.URI.create(datasourceUrl.replaceFirst("jdbc:", ""));
      hostname = uri.getHost();
      port = uri.getPort() == -1 ? 5432 : uri.getPort();
      if (uri.getPath() != null && uri.getPath().length() > 1) {
        dbName = uri.getPath().substring(1);
      }
    }

    return io.debezium.config.Configuration.create()
        .with("name", "debezium-outbox-connector")
        .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")

        // Use durable Kafka offset storage
        .with("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore")
        .with("offset.storage.topic", "debezium-outbox-offsets")
        .with("offset.storage.partitions", "1")
        .with("offset.storage.replication.factor", "1")
        .with("bootstrap.servers", "localhost:9092")
        .with("offset.flush.interval.ms", "60000")
        .with("database.hostname", hostname)
        .with("database.port", String.valueOf(port))
        .with("database.user", datasourceUsername)
        .with("database.password", datasourcePassword)
        .with("database.dbname", dbName)
        .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("key.converter.schemas.enable", "false")
        .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("value.converter.schemas.enable", "false")
        .with("topic.prefix", "debezium")
        .with("database.server.name", "outbox-server")
        .with("plugin.name", "pgoutput") // default for PG 10+
        .with("slot.name", "debezium_outbox_slot")
        .with("publication.name", "debezium_outbox_pub")
        .with("table.include.list", "public.outbox_event")
        .with("tombstones.on.delete", "false")
        .build();
  }
}
