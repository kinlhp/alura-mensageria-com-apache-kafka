package com.kinlhp.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class AllRecordsConsumerLogServiceMain {
    private static final String TOPIC = "ECOMMERCE.*";

    public static void main(String[] args) {
        final var instance = new AllRecordsConsumerLogServiceMain();
        new KafkaConsumerService<>(AllRecordsConsumerLogServiceMain.class, TOPIC, Object.class, instance::doNothing)
                .poll();
    }

    private void doNothing(final ConsumerRecord<String, Object> record) {
    }
}
