package com.kinlhp.learning;

import java.io.IOException;
import java.io.Serial;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.kafka.common.header.internals.RecordHeaders;

public class OrderServlet extends HttpServlet {
    private static final String topicNewOrder = "ECOMMERCE_NEW_ORDER";
    private static final AbstractKafkaProducer<String, Order> orderProducer = new AsyncKafkaProducerService<>(OrderServlet.class);
    @Serial
    private static final long serialVersionUID = 5256808296351000137L;

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        System.out.print("Received POST request on /api/v1/orders");
        final var customer = req.getParameter("customer");
        final var amount = req.getParameter("amount");
        final String orderId = req.getParameter("order-id");
        System.out.printf(" from customer %s with amount %s and order ID %s%n", customer, amount, orderId);

        final var headers = Headers.of(new RecordHeaders()).append(this.getClass());

        final var order = orderOf(customer, new BigDecimal(amount), UUID.fromString(orderId));

        if (insert(order)) {
            orderProducer.send(topicNewOrder, customer, order, headers);
            System.out.printf("[New order: %s]%n", orderId);
        } else {
            System.out.printf("[Idempotency hit for order: %s]%n", orderId);
        }

        respond(resp, order, headers);
    }

    private Order orderOf(final String customerEmail, final BigDecimal amount, final UUID orderId) {
        return new Order(customerEmail, orderId, amount);
    }

    private boolean insert(final Order order) {
        // code smell
        final var database = createTable();
        if (wasProcessed(database, order)) {
            System.out.printf("Order %s was already processed", order.orderId());
            return false;
        }
        final var sql = "INSERT INTO orderr (id) VALUES (?)";// order is a keyword (https://sqlite.org/lang_keywords.html)
        try {
            database.execute(sql, order.orderId().toString());
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private FilesystemDatabase createTable() {
        final var database = new FilesystemDatabase("order");
        final var sql = "CREATE TABLE IF NOT EXISTS orderr(id CHARACTER(36) PRIMARY KEY)";// order is a keyword (https://sqlite.org/lang_keywords.html)
        database.createTable(sql);
        return database;
    }

    private boolean wasProcessed(final FilesystemDatabase database, final Order order) {
        final var sql = "SELECT id FROM orderr WHERE id = ? LIMIT 1";// order is a keyword (https://sqlite.org/lang_keywords.html)
        try {
            return database.executeQuery(sql, order.orderId().toString()).next();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void respond(final HttpServletResponse resp, final Order order, final org.apache.kafka.common.header.Headers headers) throws IOException {
        final var message = String.format(
                "Order from customer %s sent successfully with id %s and amount %s and correlation ID %s%n",
                order.customerEmail(),
                order.orderId(),
                order.amount(),
                new String(headers.lastHeader("Correlation-ID").value())
        );
        System.out.println(message);
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().println(message);
    }
}
