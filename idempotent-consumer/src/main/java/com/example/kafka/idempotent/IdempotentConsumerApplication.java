package com.example.kafka.idempotent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = {"com.example.kafka.idempotent", "com.example.kafka.common"})
@EnableJpaRepositories(basePackages = {"com.example.kafka.idempotent", "com.example.kafka.common"})
public class IdempotentConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(IdempotentConsumerApplication.class, args);
  }
}
