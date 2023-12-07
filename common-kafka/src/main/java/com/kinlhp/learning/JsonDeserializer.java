package com.kinlhp.learning;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    public static final String VALUE_TYPE_CLASS_CONFIG = "value.type";
    private Class<T> valueType;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        Deserializer.super.configure(configs, isKey);
        this.valueType = valueTypeFrom(configs);
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        try {
            return new ObjectMapper().readValue(data, valueType);
        } catch (final Exception e) {
            throwException(e);
        }
        // ignoring
        return null;
    }

    private Class<T> valueTypeFrom(final Map<String, ?> configs) {
        try {
            final String valueType = String.valueOf(configs.get(VALUE_TYPE_CLASS_CONFIG));
            // noinspection unchecked
            return (Class<T>) Class.forName(valueType);
        } catch (final Exception e) {
            throwException(e);
        }
        // ignoring
        return null;
    }

    private void throwException(final Exception exception) {
        final var message = String.format("Error when deserializing byte[] to %s", valueType == null ? null : valueType.getSimpleName());
        throw new SerializationException(message, exception);
    }
}
