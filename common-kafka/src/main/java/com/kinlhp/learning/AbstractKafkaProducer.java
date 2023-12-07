package com.kinlhp.learning;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;

// TODO: Transformar numa interface fluente:
// {,a}sync().send(value).with(key).and(headers).to(topic)
public abstract class AbstractKafkaProducer<K, V> {
    private final Class<?> clientId;

    public AbstractKafkaProducer(final Class<?> clientId) {
        this.clientId = clientId;
    }

    public final void send(final String topic, final K key, final V value, final Entry<String, byte[]>... headers) {
        final var mappedHeaders = Arrays.stream(headers)
                .map(header -> new RecordHeader(header.getKey(), header.getValue()))
                .toArray(Header[]::new);
        send(topic, key, value, mappedHeaders);
    }

    public final void send(final String topic, final K key, final V value, final Header... headers) {
        send(topic, key, value, () -> Arrays.stream(headers).iterator());
    }

    public final void send(final String topic, final K key, final V value, final Iterable<Header> headers) {
        send(topic, key, value, new RecordHeaders(headers));
    }

    public abstract void send(final String topic, final K key, final V value, final Headers headers);

    protected Properties withProperties() {
        final var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "acer:9092,acer:19092,acer:29092,acer:39092,acer:49092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", clientId.getSimpleName(), UUID.randomUUID()));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    protected final Callback withCallback() {
        return (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }
            infoOf(metadata);
        };
    }

    private void infoOf(final RecordMetadata metadata) {
        System.out.printf("[%s] Record sent successfully: ", clientId.getSimpleName());
        final var info = Map.of(
                "topic", metadata.topic(),
                "partition", metadata.partition(),
                "offset", metadata.offset(),
                "timestamp", metadata.timestamp()
        );
        System.out.println(info);
    }
}
