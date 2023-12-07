package com.kinlhp.learning;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

public interface Headers {

    static Appender of(final org.apache.kafka.common.header.Headers headers) {
        return context -> {
            final var appended = Stream.concat(
                    Arrays.stream(headers.toArray()),
                    Stream.of(new RecordHeader(
                            "Correlation-ID",
                            Map.of(context.getSimpleName(), Instant.now().getEpochSecond()).toString().getBytes(StandardCharsets.UTF_8)
                    ))
            ).toList();
            return new RecordHeaders(appended);
        };
    }

    interface Appender {

        org.apache.kafka.common.header.Headers append(Class<?> context);
    }
}
