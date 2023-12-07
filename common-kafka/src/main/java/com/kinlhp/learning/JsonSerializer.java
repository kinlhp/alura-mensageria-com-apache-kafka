package com.kinlhp.learning;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(final String topic, final T data) {
        try {
            return data == null
                    ? null
                    : new ObjectMapper().writeValueAsBytes(data);
        } catch (final JsonProcessingException e) {
            final var message = String.format("Error when serializing %s to byte[]", data.getClass().getSimpleName());
            throw new SerializationException(message, e);
        }
    }
}
