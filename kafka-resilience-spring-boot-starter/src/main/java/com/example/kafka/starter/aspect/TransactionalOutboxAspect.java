package com.example.kafka.starter.aspect;

import com.example.kafka.starter.annotation.TransactionalOutbox;
import com.example.kafka.starter.outbox.OutboxEventPublisher;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

@Aspect
public class TransactionalOutboxAspect {

  private final OutboxEventPublisher publisher;
  private final ExpressionParser parser = new SpelExpressionParser();

  public TransactionalOutboxAspect(OutboxEventPublisher publisher) {
    this.publisher = publisher;
  }

  @Around("@annotation(outboxAnnotation)")
  @Nullable
  public Object publishToOutbox(ProceedingJoinPoint joinPoint, TransactionalOutbox outboxAnnotation)
      throws Throwable {

    // Proceed with the business logic method execution
    Object result = joinPoint.proceed();

    if (result == null) {
      return null; // Nothing to publish
    }

    // Evaluate the aggregate ID and Message Key using the returned result
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setVariable("result", result);

    String aggregateId = outboxAnnotation.aggregateType(); // default fallback
    if (StringUtils.hasText(outboxAnnotation.aggregateIdExpression())) {
      aggregateId =
          parser
              .parseExpression(outboxAnnotation.aggregateIdExpression())
              .getValue(context, String.class);
    }

    String messageKey = aggregateId; // default to aggregateId for partitioning
    if (StringUtils.hasText(outboxAnnotation.messageKeyExpression())) {
      messageKey =
          parser
              .parseExpression(outboxAnnotation.messageKeyExpression())
              .getValue(context, String.class);
    }

    publisher.publish(
        outboxAnnotation.topic(),
        outboxAnnotation.aggregateType(),
        aggregateId,
        messageKey,
        result);

    return result;
  }
}
