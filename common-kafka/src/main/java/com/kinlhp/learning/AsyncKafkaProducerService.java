package com.kinlhp.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

public class AsyncKafkaProducerService<K, V> extends AbstractKafkaProducer<K, V> {

    public AsyncKafkaProducerService(final Class<?> clientId) {
        super(clientId);
    }

    @Override
    public void send(final String topic, final K key, final V value, final Headers headers) {
        try (final var producer = new KafkaProducer<K, V>(withProperties())) {
            final var producerRecord = new ProducerRecord<>(topic, key, value);
            headers.forEach(header -> producerRecord.headers().add(header));
            System.out.printf("Sending asynchronously [key: %s]%n", key);
            producer.send(producerRecord, withCallback());
            System.out.printf("Sent asynchronously successfully [key: %s]%n", key);
        }
    }
}
