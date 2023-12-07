package com.kinlhp.learning;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public interface IO {

    static Target interpolate(Path placeholder) {
        return customer -> {
            final var target = customer.getReadingReportPath();
            System.out.printf("Creating reading report to customer %s (path: %s)%n", customer.email(), target);
            try {
                Files.copy(placeholder, target, StandardCopyOption.REPLACE_EXISTING);
                Files.writeString(target, customer.email(), StandardOpenOption.APPEND);
                System.out.printf("Successfully created reading report to customer %s%n", customer.email());
            } catch (final Exception e) {
                System.out.printf("Failure attempt creating reading report to customer %s%n", customer.email());
                e.printStackTrace();
            }
        };
    }

    interface Target {

        void to(Customer customer) throws IOException;
    }
}
