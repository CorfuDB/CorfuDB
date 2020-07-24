package org.corfudb.common.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

public class LoggerUtil {

    private LoggerUtil() {
        //prevent creating class util instances
    }

    /**
     * Setup logback logger
     * - pick the correct logging level before outputting error messages
     * - add serverEndpoint information
     *
     * @param logLevel log level
     */
    public static void configureLogger(String logLevel) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        Level level = Level.toLevel(logLevel);
        root.setLevel(level);
    }

    public static void stopLogger() {
        // Flush the async appender before exiting to prevent the loss of logs
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.stop();
    }
}
