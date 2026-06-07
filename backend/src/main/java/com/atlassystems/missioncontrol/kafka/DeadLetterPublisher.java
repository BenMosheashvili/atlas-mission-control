package com.atlassystems.missioncontrol.kafka;
import lombok.RequiredArgsConstructor; import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
@Slf4j @Component @RequiredArgsConstructor
public class DeadLetterPublisher {
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private static final String DLQ = "infra.node.metrics.dlq";
    public void publish(ConsumerRecord<?,?> r, Throwable cause) {
        log.error("[DLQ] partition={} offset={} reason={}", r.partition(), r.offset(), cause.getMessage());
        kafkaTemplate.send(DLQ, String.valueOf(r.key()), r.value());
    }
}
