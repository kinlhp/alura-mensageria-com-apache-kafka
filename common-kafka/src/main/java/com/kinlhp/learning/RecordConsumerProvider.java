package com.kinlhp.learning;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class RecordConsumerProvider<K, V> implements Callable<Void> {

    private final Supplier<RecordConsumer<K, V>> factory;

    public RecordConsumerProvider(final Supplier<RecordConsumer<K, V>> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() {
        final var recordConsumer = factory.get();
        new KafkaConsumerService<>(
                recordConsumer.getGroupId(),
                recordConsumer.getTopic(),
                recordConsumer.getValueType(),
                recordConsumer::process
        )
                .poll();
        return null;
    }
}
