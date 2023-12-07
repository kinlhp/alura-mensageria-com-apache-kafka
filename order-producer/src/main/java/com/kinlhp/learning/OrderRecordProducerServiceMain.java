package com.kinlhp.learning;

import java.math.BigDecimal;
import java.util.UUID;

import org.apache.kafka.common.header.internals.RecordHeaders;

public class OrderRecordProducerServiceMain {
    private static final String topicNewOrder = "ECOMMERCE_NEW_ORDER";
    private static final String topicSendEmail = "ECOMMERCE_SEND_EMAIL";
    private static final int recordsToSend = 10;

    public static void main(final String[] args) {
        final var orderProducer = new AsyncKafkaProducerService<String, Order>(OrderRecordProducerServiceMain.class);
        final var emailProducer = new AsyncKafkaProducerService<String, Email>(OrderRecordProducerServiceMain.class);
        final var instance = new OrderRecordProducerServiceMain();
        final var customer = UUID.randomUUID() + "@kinlhp.com";
        for (int i = 0; i < recordsToSend; i++) {
            final var orderId = UUID.randomUUID();

            final var headers = Headers.of(new RecordHeaders()).append(OrderRecordProducerServiceMain.class);

            final var order = instance.orderOf(customer, orderId);
            orderProducer.send(topicNewOrder, customer, order, headers);

            final var email = instance.emailOf(customer, orderId);
            emailProducer.send(topicSendEmail, customer, email, headers);
        }
    }

    private Order orderOf(final String customerEmail, final UUID orderId) {
        final var amount = BigDecimal.valueOf(Math.random() * 100);
        return new Order(customerEmail, orderId, amount);
    }

    private Email emailOf(final String customerEmail, final UUID orderId) {
        final var subject = String.format("%s, thank you for your order!", customerEmail.split("@")[0]);
        final var body = String.format("We are processing your order %s!", orderId);
        return new Email(subject, body);
    }
}
