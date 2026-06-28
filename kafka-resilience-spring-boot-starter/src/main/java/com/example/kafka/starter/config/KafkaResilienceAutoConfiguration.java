package com.example.kafka.starter.config;

import com.example.kafka.common.idempotent.application.IdempotentMessageProcessor;
import com.example.kafka.common.idempotent.infrastructure.IdempotencyCleanupScheduler;
import com.example.kafka.common.idempotent.infrastructure.ProcessedMessageRepository;
import com.example.kafka.starter.aspect.IdempotentConsumerAspect;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@AutoConfiguration
@ConditionalOnClass(IdempotentMessageProcessor.class)
@EntityScan(basePackages = "com.example.kafka.common.idempotent.infrastructure")
@EnableJpaRepositories(basePackages = "com.example.kafka.common.idempotent.infrastructure")
public class KafkaResilienceAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public IdempotentMessageProcessor idempotentMessageProcessor(
      ProcessedMessageRepository repository) {
    return new IdempotentMessageProcessor(repository);
  }

  @Bean
  @ConditionalOnMissingBean
  public IdempotentConsumerAspect idempotentConsumerAspect(IdempotentMessageProcessor processor) {
    return new IdempotentConsumerAspect(processor);
  }

  @Bean
  @ConditionalOnMissingBean
  public IdempotencyCleanupScheduler idempotencyCleanupScheduler(
      ProcessedMessageRepository repository) {
    return new IdempotencyCleanupScheduler(repository);
  }
}
