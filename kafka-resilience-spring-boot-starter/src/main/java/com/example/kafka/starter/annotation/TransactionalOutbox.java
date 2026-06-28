package com.example.kafka.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.transaction.annotation.Transactional;

/**
 * Indicates that the return value of this method should be published to the Kafka Outbox. The
 * method must return an object (the event payload) or a collection of objects. It is automatically
 * wrapped in a database transaction.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Transactional
public @interface TransactionalOutbox {

  /** The target Kafka topic. */
  String topic();

  /** The aggregate type (e.g., "Order"). */
  String aggregateType();

  /**
   * SpEL expression to extract the aggregate ID from the returned event. e.g. "#result.orderId()"
   */
  String aggregateIdExpression() default "";

  /** SpEL expression to extract the message key for Kafka partitioning. e.g. "#result.orderId()" */
  String messageKeyExpression() default "";
}
