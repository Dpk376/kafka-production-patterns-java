package com.example.kafka.starter.config;

import com.example.kafka.common.idempotent.application.IdempotentMessageProcessor;
import com.example.kafka.common.idempotent.infrastructure.IdempotencyCleanupScheduler;
import com.example.kafka.common.idempotent.infrastructure.ProcessedMessageRepository;
import com.example.kafka.common.outbox.OutboxEventRepository;
import com.example.kafka.starter.aspect.IdempotentConsumerAspect;
import com.example.kafka.starter.aspect.TransactionalOutboxAspect;
import com.example.kafka.starter.dlq.DlqReplayController;
import com.example.kafka.starter.dlq.DlqReplayService;
import com.example.kafka.starter.outbox.OutboxEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@AutoConfiguration
@ConditionalOnClass(IdempotentMessageProcessor.class)
@ComponentScan(basePackages = "com.example.kafka.starter.outbox")
@EntityScan(
    basePackages = {
      "com.example.kafka.common.idempotent.infrastructure",
      "com.example.kafka.common.outbox"
    })
@EnableJpaRepositories(
    basePackages = {
      "com.example.kafka.common.idempotent.infrastructure",
      "com.example.kafka.common.outbox"
    })
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

  @Bean
  @ConditionalOnMissingBean
  public OutboxEventPublisher outboxEventPublisher(
      OutboxEventRepository repository, ObjectMapper objectMapper) {
    return new OutboxEventPublisher(repository, objectMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  public TransactionalOutboxAspect transactionalOutboxAspect(OutboxEventPublisher publisher) {
    return new TransactionalOutboxAspect(publisher);
  }

  @Bean
  @ConditionalOnMissingBean
  public DlqReplayService dlqReplayService(
      org.springframework.kafka.core.ConsumerFactory<byte[], byte[]> consumerFactory,
      org.springframework.kafka.core.KafkaTemplate<byte[], byte[]> kafkaTemplate) {
    return new DlqReplayService(consumerFactory, kafkaTemplate);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  public DlqReplayController dlqReplayController(DlqReplayService service) {
    return new DlqReplayController(service);
  }
}
