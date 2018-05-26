package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ServerThreadFactory implements ThreadFactory {

    private static final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

    private final Thread.UncaughtExceptionHandler handler;

    private final String threadPrefix;

    final AtomicInteger threadNumber = new AtomicInteger(0);

    public ServerThreadFactory(String threadPrefix, Thread.UncaughtExceptionHandler handler) {
        this.threadPrefix = threadPrefix;
        this.handler = handler;
    }

    @Override
    public Thread newThread(Runnable run) {
        Thread thread = defaultFactory.newThread(run);
        thread.setName(threadPrefix + threadNumber.getAndIncrement());
        thread.setUncaughtExceptionHandler(handler);
        thread.setDaemon(true);
        return thread;
    }

    public static class ExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("handleUncaughtException[{}]: Uncaught {}:{}",
                    t.getName(),
                    e.getClass().getSimpleName(),
                    e.getMessage(),
                    e);
        }
    }
}
