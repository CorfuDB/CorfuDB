package org.corfudb.test.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.slf4j.LoggerFactory;

public class TestLogger {

    private MemoryAppender<ILoggingEvent> appender;

    public TestLogger(int elements) {

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(lc);
        encoder.setPattern("%d{HH:mm:ss.SSS} %highlight(%-5level) "
            + "[%thread] %cyan(%logger{15}) - %msg%n %ex{3}");
        encoder.start();

        appender = new MemoryAppender<>(elements, encoder);
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        appender.setContext(lc);
        appender.start();
        logger.addAppender(appender);
    }

    public void reset() {
        appender.reset();
    }

    public Iterable<byte[]> getEventsAndReset() {
        return appender.getEventsAndReset();
    }

}
