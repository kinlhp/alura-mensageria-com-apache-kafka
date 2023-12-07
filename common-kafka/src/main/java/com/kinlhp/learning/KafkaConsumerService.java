package com.kinlhp.learning;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerService<K, V> {
    private final Class<?> groupId;
    private final String topic;
    private final Class<V> valueType;
    private final Consumer<ConsumerRecord<K, V>> action;

    public KafkaConsumerService(final Class<?> groupId, final String topic, final Class<V> valueType, final Consumer<ConsumerRecord<K, V>> action) {
        this.groupId = groupId;
        this.topic = topic;
        this.valueType = valueType;
        this.action = action;
    }

    public void poll() {
        try (final var consumer = new KafkaConsumer<K, V>(withProperties())) {
            // consumer.subscribe(Collections.singleton(topic));
            consumer.subscribe(Pattern.compile(topic));
            final var deadLetterProducer = new KafkaProducerService<String, byte[]>(KafkaConsumerService.class);
            try (final var serializer = new JsonSerializer<V>()) {
                while (true) {
                    final var records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        System.out.printf("%d records caught%n", records.count());
                        records.forEach(record -> {
                            infoOf(record);
                            try {
                                action.accept(record);
                            } catch (final Exception e) {
                                e.printStackTrace();
                                final var headers = com.kinlhp.learning.Headers.of(record.headers()).append(this.getClass());
                                deadLetterProducer.send("ECOMMERCE_DEAD_LETTER", record.key().toString(), serializer.serialize(null, record.value()), headers);
                            }
                        });
                    }
                }
            }
        }
    }

    private Properties withProperties() {
        final var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "acer:9092,acer:19092,acer:29092,acer:39092,acer:49092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId.getSimpleName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", groupId.getSimpleName(), UUID.randomUUID()));
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");// (trade-off) Consumes event, advances offset and commits 1 by 1
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(JsonDeserializer.VALUE_TYPE_CLASS_CONFIG, valueType.getName());
        return properties;
    }

    private void infoOf(final ConsumerRecord<K, V> record) {
        System.out.printf("[%s] Record received: ", groupId.getSimpleName());
        final var info = Map.of(
                "topic", record.topic(),
                "partition", record.partition(),
                "offset", record.offset(),
                "timestamp", record.timestamp(),
                "key", record.key(),
                "value", record.value(),
                "headers", Arrays.stream(record.headers().toArray()).map(header -> Map.entry(header.key(), new String(header.value()))).collect(Collectors.toList())
        );
        System.out.println(info);
    }
}
