package com.kinlhp.learning;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ReadingReportRecordConsumerServiceMain implements RecordConsumer<String, Customer> {
    private static final String topicCustomerReadingReport = "ECOMMERCE_CUSTOMER_READING_REPORT";
    private static final Path readingReportTemplate = getReadingReportTemplate();

    public static void main(String[] args) {
        new RecordConsumerParallelRunner<>(ReadingReportRecordConsumerServiceMain::new).run(5);
    }

    private static Path getReadingReportTemplate() {
        try {
            return Path.of(ClassLoader.getSystemResource("reading-report-template.md").toURI());
        } catch (final URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<Customer> getValueType() {
        return Customer.class;
    }

    @Override
    public String getTopic() {
        return topicCustomerReadingReport;
    }

    @Override
    public void process(final ConsumerRecord<String, Customer> record) {
        System.out.printf("Processing reading report for customer: %s%n", record.value());
        final var customer = record.value();
        try {
            IO.interpolate(readingReportTemplate).to(customer);
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
