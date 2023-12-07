package com.kinlhp.learning;

import java.io.IOException;
import java.io.Serial;
import java.util.Map;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class ReportServlet extends HttpServlet {
    private static final String topicAllCustomersTask = "ECOMMERCE_ALL_CUSTOMERS_TASK";
    private static final String topicCustomerReadingReport = "ECOMMERCE_CUSTOMER_READING_REPORT";
    private static final AbstractKafkaProducer<String, String> allCustomersTaskProducer = new AsyncKafkaProducerService<>(ReportServlet.class);
    @Serial
    private static final long serialVersionUID = 5977222984535159161L;

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        System.out.print("Received POST request on /api/v1/customers/reports");
        final var reportName = req.getParameter("report-name");
        System.out.printf(" for report %s%n", reportName);
        if ("ECOMMERCE_CUSTOMER_READING_REPORT".equals(reportName)) {
            final var headers = Headers.of(new RecordHeaders()).append(this.getClass());
            allCustomersTaskProducer.send(topicAllCustomersTask, ReportServlet.class.getSimpleName(), topicCustomerReadingReport, headers);
            respond(resp, reportName, headers.lastHeader("Correlation-ID"));
        }
    }

    private void respond(final HttpServletResponse resp, final String reportName, final Header correlationID) throws IOException {
        final var handledCorrelationID = Map.entry(correlationID.key(), new String(correlationID.value()));
        final var message = String.format("Command for all customer reading report sent successfully with report name %s and correlation ID %s%n", reportName, handledCorrelationID);
        System.out.println(message);
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().println(message);
    }
}
