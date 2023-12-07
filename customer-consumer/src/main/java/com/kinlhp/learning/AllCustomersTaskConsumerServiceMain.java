package com.kinlhp.learning;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class AllCustomersTaskConsumerServiceMain {
    private static final String topicAllCustomersTask = "ECOMMERCE_ALL_CUSTOMERS_TASK";
    private static final AbstractKafkaProducer<String, Customer> producer = new KafkaProducerService<>(AllCustomersTaskConsumerServiceMain.class);
    private final Connection connection;

    public AllCustomersTaskConsumerServiceMain() throws SQLException {
        final var jdbcUrl = "jdbc:sqlite:target/customer.db";
        this.connection = DriverManager.getConnection(jdbcUrl);
        createTable();
    }

    public static void main(String[] args) throws SQLException {
        final var instance = new AllCustomersTaskConsumerServiceMain();
        new KafkaConsumerService<>(AllCustomersTaskConsumerServiceMain.class, topicAllCustomersTask, String.class, instance::process)
                .poll();
    }

    private void process(final ConsumerRecord<String, String> record) {
        System.out.println("Processing all customers task");
        final var destinationTopic = record.value();
        final var headers = com.kinlhp.learning.Headers.of(record.headers()).append(this.getClass());
        try {
            for (final Customer customer : getAllCustomers()) {
                producer.send(destinationTopic, customer.email(), customer, headers);
            }
            System.out.printf("All customers task processed with correlation ID %s [key: %s, destinationTopic: %s]", headers.lastHeader("Correlation-ID"), record.key(), destinationTopic);
        } catch (SQLException e) {
            // ignoring: non-blocking process
            e.printStackTrace();
        }
        throw new RuntimeException("Intentionally throws a exception!!!");
    }

    private void createTable() throws SQLException {
        final var sql = "CREATE TABLE IF NOT EXISTS customer(id CHARACTER(36) PRIMARY KEY, email VARCHAR(64) NOT NULL)";
        connection.createStatement().execute(sql);
    }

    private Collection<Customer> getAllCustomers() throws SQLException {
        System.out.print("Getting all customers: ");
        final var sql = "SELECT email FROM customer";
        final var preparedStatement = connection.prepareStatement(sql);
        final var resultSet = preparedStatement.executeQuery();
        final var customers = new ArrayList<Customer>();
        while (resultSet.next()) {
            final var email = resultSet.getString("email");
            customers.add(new Customer(email));
        }
        System.out.printf("%d customers found%n", customers.size());
        return customers;
    }
}
