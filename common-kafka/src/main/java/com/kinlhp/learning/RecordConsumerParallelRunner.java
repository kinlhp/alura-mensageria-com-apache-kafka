package com.kinlhp.learning;

import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class RecordConsumerParallelRunner<K, V> {

    private final RecordConsumerProvider<K, V> provider;

    public RecordConsumerParallelRunner(final Supplier<RecordConsumer<K, V>> factory) {
        this.provider = new RecordConsumerProvider<>(factory);
    }

    public void run(final int nThreads) {
        final var threadPool = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            threadPool.submit(provider);
        }
    }
}
