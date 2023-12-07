package com.kinlhp.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailRecordConsumerEmailServiceMain implements RecordConsumer<String, Email> {
    private static final String topic = "ECOMMERCE_SEND_EMAIL";

    public static void main(String[] args) {
        new RecordConsumerParallelRunner<>(EmailRecordConsumerEmailServiceMain::new).run(5);
    }

    @Override
    public Class<Email> getValueType() {
        return Email.class;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void process(final ConsumerRecord<String, Email> record) {
        System.out.printf("[%s]Processing email: %s%n", Thread.currentThread().getName(), record.value());
        final var email = record.value();
        if (send(email)) {
            System.out.printf("Email \"%s\" sent%n", email.subject());
        } else {
            System.out.println("Fail");
        }
    }

    private boolean send(final Email email) {
        try {
            System.out.print("Sending email: ");
            Thread.sleep(1000);// simulates a processing time
        } catch (final InterruptedException e) {
            // ignoring: non-blocking process
            e.printStackTrace();
            return false;
        }
        System.out.print("Evaluating whether or not the email should be sent: ");
        final var should = email.subject().contains("0");
        System.out.println(should ? "should" : "shouldn't");
        return should;
    }
}
