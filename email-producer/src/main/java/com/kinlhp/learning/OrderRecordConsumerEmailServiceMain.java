package com.kinlhp.learning;

import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderRecordConsumerEmailServiceMain implements RecordConsumer<String, Order> {
    private static final String topicNewOrder = "ECOMMERCE_NEW_ORDER";
    private static final String topicSendEmail = "ECOMMERCE_SEND_EMAIL";
    private static final AbstractKafkaProducer<String, Email> producer = new AsyncKafkaProducerService<>(OrderRecordConsumerEmailServiceMain.class);

    public static void main(String[] args) {
        new RecordConsumerParallelRunner<>(OrderRecordConsumerEmailServiceMain::new).run(1);
    }

    @Override
    public Class<Order> getValueType() {
        return Order.class;
    }

    @Override
    public String getTopic() {
        return topicNewOrder;
    }

    @Override
    public void process(final ConsumerRecord<String, Order> record) {
        final var order = record.value();
        System.out.printf("Processing order: %s%n", order);
        final var email = emailOf(order.customerEmail(), order.orderId());
        final var headers = com.kinlhp.learning.Headers.of(record.headers()).append(this.getClass());
        producer.send(topicSendEmail, order.customerEmail(), email, headers);
    }

    private Email emailOf(final String customerEmail, final UUID orderId) {
        final var subject = String.format("%s, thank you for your order!", customerEmail.split("@")[0]);
        final var body = String.format("We are processing your order %s!", orderId);
        return new Email(subject, body);
    }
}
