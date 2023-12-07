package com.kinlhp.learning;

import java.nio.file.Path;
import java.nio.file.Paths;

public record Customer(String email) {

    public Path getReadingReportPath() {
        final var fileName = String.format("reading-report-%s.md", email().split("@")[0]);
        return Paths.get("target", fileName);
    }
}
