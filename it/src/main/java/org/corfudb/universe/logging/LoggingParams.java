package org.corfudb.universe.logging;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Specifies policies to collect logs from docker containers
 */
@Builder
public class LoggingParams {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final String CORFU_DB_PATH = "corfudb";
    private static final String BASE_DIR = "/tmp/";

    @Default
    private final String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
    @NonNull
    private final String testName;
    @Default
    @Getter
    private final boolean enabled = false;

    @Getter
    @Default
    private final String baseDir = BASE_DIR;

    public Path getServerLogDir() {
        return Paths.get(baseDir, CORFU_DB_PATH, timestamp, testName);
    }
}
