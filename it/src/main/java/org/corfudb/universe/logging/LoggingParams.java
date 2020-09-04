package org.corfudb.universe.logging;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Specifies policies to collect logs from docker/vm/processes corfu servers
 */
@Builder
public class LoggingParams {

    @NonNull
    private final String testName;

    @Default
    @Getter
    private final boolean enabled = false;

    public Path getRelativeServerLogDir() {
        return Paths.get(testName);
    }
}
