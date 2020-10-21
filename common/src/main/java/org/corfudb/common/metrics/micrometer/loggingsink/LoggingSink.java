package org.corfudb.common.metrics.micrometer.loggingsink;

import java.util.function.Consumer;

/**
 * An interface for a logging sink. A sink is used to format the lines
 * before printing them to the log.
 */
public interface LoggingSink extends Consumer<String> {

}
