package com.kinlhp.learning;

import java.sql.SQLException;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderRecordConsumerCustomerServiceMain implements RecordConsumer<String, Order> {
    private static final String topicNewOrder = "ECOMMERCE_NEW_ORDER";
    private final FilesystemDatabase database;

    OrderRecordConsumerCustomerServiceMain() {
        database = new FilesystemDatabase("customer");
        final var sql = "CREATE TABLE IF NOT EXISTS customer(id CHARACTER(36) PRIMARY KEY, email VARCHAR(64) NOT NULL)";
        database.createTable(sql);
    }

    public static void main(String[] args) {
        new RecordConsumerParallelRunner<>(OrderRecordConsumerCustomerServiceMain::new).run(1);
    }

    @Override
    public String getTopic() {
        return topicNewOrder;
    }

    @Override
    public Class<Order> getValueType() {
        return Order.class;
    }

    @Override
    public void process(final ConsumerRecord<String, Order> record) {
        System.out.printf("Processing order: %s%n", record.value());
        final var email = record.value().customerEmail();
        if (isNew(email)) {
            insert(email);
            System.out.printf("Customer %s created%n", email);
        }
    }

    private boolean isNew(final String email) {
        System.out.printf("Checking if the customer %s exists: ", email);
        final var sql = "SELECT id FROM customer WHERE email = ? LIMIT 1";
        try (final var resultSet = database.executeQuery(sql, email)) {
            final var exists = resultSet.next();
            System.out.printf("Customer %s %s exists%n", email, exists ? "already" : "not");
            return !exists;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void insert(final String email) {
        System.out.printf("Creating customer %s: ", email);
        final var sql = "INSERT INTO customer (id, email) values (?, ?)";
        try {
            database.execute(sql, UUID.randomUUID().toString(), email);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
