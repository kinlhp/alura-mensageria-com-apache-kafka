package com.kinlhp.learning;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

public class KafkaProducerService<K, V> extends AbstractKafkaProducer<K, V> {

    public KafkaProducerService(final Class<?> clientId) {
        super(clientId);
    }

    @Override
    public void send(final String topic, final K key, final V value, final Headers headers) {
        try (final var producer = new KafkaProducer<K, V>(withProperties())) {
            final var producerRecord = new ProducerRecord<>(topic, key, value);
            headers.forEach(header -> producerRecord.headers().add(header));
            System.out.printf("Sending synchronously [key: %s]%n", key);
            producer.send(producerRecord, withCallback()).get();
            System.out.printf("Sent synchronously successfully [key: %s]%n", key);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
