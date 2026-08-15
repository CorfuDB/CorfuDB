package org.corfudb.util;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 3/24/16.
 */
@Slf4j
public class LambdaUtils {

    private LambdaUtils() {
        // prevent instantiation of this class
    }

    /**
     * Suppresses all exceptions. This is used for scheduling tasks to ScheduledExecutorService.
     * ScheduledExecutorService crashes in case a scheduled thread throws an Exception.
     *
     * @param runnable Task whose exceptions are to be suppressed.
     */
    public static void runSansThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            log.error("runSansThrow: Suppressing exception while executing runnable: ", e);
        }
    }

    public interface ThrowableConsumer<T> {
        void accept(T t) throws Exception;
    }
}
