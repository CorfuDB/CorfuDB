package org.corfudb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Created by mwei on 9/15/15.
 */
public class CFUtils {

    /** Generates a completable future which times out.
     * inspired by NoBlogDefFound: http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     * @param duration  The duration to timeout after.
     * @param <T>       Ignored, since the future will always timeout.
     * @return          A completable future that will time out.
     */
    public static <T> CompletableFuture<T> failAfter(Duration duration) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        scheduler.schedule(() -> {
            final TimeoutException ex = new TimeoutException("Timeout after " + duration.toMillis() + " ms");
            return promise.completeExceptionally(ex);
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    /** Takes a completable future, and ensures that it completes within a certain duration. If it does
     * not, it is cancelled and completes exceptionally with TimeoutException.
     *  inspired by NoBlogDefFound: http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     * @param future    The completable future that must be completed within duration.
     * @param duration  The duration the future must be completed in.
     * @param <T>       The return type of the future.
     * @return          A completable future which completes with the original value if completed within
     *                  duration, otherwise completes exceptionally with TimeoutException.
     */
    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration) {
        final CompletableFuture<T> timeout = failAfter(duration);
        return future.applyToEither(timeout, Function.identity());
    }

    private static final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("failAfter-%d")
                            .build());
}
