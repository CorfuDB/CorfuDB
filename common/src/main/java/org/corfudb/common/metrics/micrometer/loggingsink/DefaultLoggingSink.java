package org.corfudb.common.metrics.micrometer.loggingsink;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.slf4j.Logger;

/**
 * A default logging sink. It prints the line in the default format.
 */
@AllArgsConstructor

public class DefaultLoggingSink implements LoggingSink {
    @NonNull
    private final Logger logger;

    @Override
    public void accept(String s) {
        logger.debug(s);
    }
}
