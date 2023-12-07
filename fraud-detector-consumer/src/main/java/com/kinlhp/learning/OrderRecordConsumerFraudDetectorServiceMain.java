package com.kinlhp.learning;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderRecordConsumerFraudDetectorServiceMain implements RecordConsumer<String, Order> {
    private static final String topicNewOrder = "ECOMMERCE_NEW_ORDER";
    private static final String topicOrderProcessed = "ECOMMERCE_ORDER_PROCESSED";
    private static final String topicOrderRejected = "ECOMMERCE_ORDER_REJECTED";
    private static final AbstractKafkaProducer<String, Order> producer = new AsyncKafkaProducerService<>(OrderRecordConsumerFraudDetectorServiceMain.class);
    private final FilesystemDatabase database;

    OrderRecordConsumerFraudDetectorServiceMain() {
        database = new FilesystemDatabase("fraud");
        final var sql = "CREATE TABLE IF NOT EXISTS orderr(id CHARACTER(36) PRIMARY KEY, is_fraud BOOLEAN NOT NULL)";// order is a keyword (https://sqlite.org/lang_keywords.html)
        database.createTable(sql);
    }

    public static void main(String[] args) {
        new RecordConsumerParallelRunner<>(OrderRecordConsumerFraudDetectorServiceMain::new).run(1);
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
        System.out.printf("Processing order: %s%n", record.value());
        final var order = record.value();
        final var headers = com.kinlhp.learning.Headers.of(record.headers()).append(this.getClass());
        if (wasProcessed(order)) {
            System.out.printf("Order %s for user %s was already processed", order.orderId(), order.customerEmail());
            return;
        }
        if (isFraud(order)) {
            System.out.printf("Order %s is a fraud [%s]%n", order.orderId(), new String(headers.lastHeader("Correlation-ID").value()));
            insert(order, true);
            producer.send(topicOrderRejected, record.key(), order, headers);
        } else {
            System.out.printf("Order %s processed [%s]%n", order.orderId(), new String(headers.lastHeader("Correlation-ID").value()));
            insert(order, false);
            producer.send(topicOrderProcessed, record.key(), order, headers);
        }
    }

    private boolean wasProcessed(final Order order) {
        final var sql = "SELECT id FROM orderr WHERE id = ? LIMIT 1";// order is a keyword (https://sqlite.org/lang_keywords.html)
        try {
            return database.executeQuery(sql, order.orderId().toString()).next();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private boolean isFraud(final Order order) {
        try {
            System.out.print("Checking for fraud: ");
            Thread.sleep(5000);// simulates a processing time
        } catch (final InterruptedException e) {
            // ignoring: non-blocking process
            e.printStackTrace();
            return true;
        }
        return BigDecimal.valueOf(75).compareTo(order.amount()) < 0;
    }

    private void insert(final Order order, final boolean isFraud) {
        final var sql = "INSERT INTO orderr (id, is_fraud) VALUES (?, ?)";// order is a keyword (https://sqlite.org/lang_keywords.html)
        try {
            database.execute(sql, order.orderId().toString(), String.valueOf(isFraud));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
