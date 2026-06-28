package com.example.kafka.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated Kafka listener method should be executed idempotently. The method
 * will be wrapped by an aspect that ensures exactly-once side effects via a database constraint on
 * the deduplication key.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface IdempotentConsumer {

  /**
   * Spring Expression Language (SpEL) expression to compute the deduplication key. If left blank,
   * the aspect will attempt to extract the ConsumerRecord from the method arguments and construct a
   * key using: topic-partition-offset.
   */
  String keyExpression() default "";
}
