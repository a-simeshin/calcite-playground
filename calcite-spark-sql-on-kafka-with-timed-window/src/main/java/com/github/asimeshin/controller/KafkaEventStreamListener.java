package com.github.asimeshin.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
public class KafkaEventStreamListener {

    @KafkaListener(topics = "TRANSFER.TOPIC")
    public void listen(ConsumerRecord<byte[], Event> eventRecord, Acknowledgment ack) {
        try {
            final Event value = eventRecord.value();
        } catch (Exception e) {
            log.error("Unable to process event", e);
        } finally {
            ack.acknowledge();
        }
    }

    public record Event(String userFrom, String userTo, Double payment, String currency) {}
}
