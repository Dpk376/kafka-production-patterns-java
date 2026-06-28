package com.example.kafka.starter.aspect;

import com.example.kafka.common.idempotent.application.IdempotentMessageProcessor;
import com.example.kafka.starter.annotation.IdempotentConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

@Aspect
public class IdempotentConsumerAspect {

  private final IdempotentMessageProcessor processor;
  private final ExpressionParser parser = new SpelExpressionParser();
  private final DefaultParameterNameDiscoverer nameDiscoverer =
      new DefaultParameterNameDiscoverer();

  public IdempotentConsumerAspect(IdempotentMessageProcessor processor) {
    this.processor = processor;
  }

  @Around("@annotation(idempotentAnnotation)")
  public Object processIdempotently(
      ProceedingJoinPoint joinPoint, IdempotentConsumer idempotentAnnotation) throws Throwable {

    String dedupKey = resolveDedupKey(joinPoint, idempotentAnnotation);

    // We use an array to capture the return value from the join point
    final Object[] result = new Object[1];

    processor.process(
        dedupKey,
        () -> {
          try {
            result[0] = joinPoint.proceed();
          } catch (RuntimeException e) {
            throw e;
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        });

    return result[0];
  }

  private String resolveDedupKey(ProceedingJoinPoint joinPoint, IdempotentConsumer annotation) {
    if (StringUtils.hasText(annotation.keyExpression())) {
      MethodSignature signature = (MethodSignature) joinPoint.getSignature();
      MethodBasedEvaluationContext context =
          new MethodBasedEvaluationContext(
              joinPoint.getTarget(), signature.getMethod(), joinPoint.getArgs(), nameDiscoverer);
      return parser.parseExpression(annotation.keyExpression()).getValue(context, String.class);
    }

    // Fallback: search for ConsumerRecord in args
    for (Object arg : joinPoint.getArgs()) {
      if (arg instanceof ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
      }
    }

    throw new IllegalArgumentException(
        "Cannot determine deduplication key. Provide a keyExpression or a ConsumerRecord argument.");
  }
}
