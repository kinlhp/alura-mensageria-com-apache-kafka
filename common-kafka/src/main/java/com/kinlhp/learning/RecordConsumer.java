package com.kinlhp.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordConsumer<K, V> {

    default Class<? extends RecordConsumer<K, V>> getGroupId() {
        return (Class<? extends RecordConsumer<K, V>>) getClass();
    }

    Class<V> getValueType();

    String getTopic();

    void process(ConsumerRecord<K, V> action);
}
