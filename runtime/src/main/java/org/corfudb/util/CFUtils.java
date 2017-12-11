package org.corfudb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

/**
 * Created by mwei on 9/15/15.
 */
public class CFUtils {

    private static final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("failAfter-%d")
                            .build());

    @SuppressWarnings("unchecked")
    public static <T,
            A extends Throwable,
            B extends Throwable,
            C extends Throwable,
            D extends Throwable> T getUninterruptibly(Future<T> future,
                                                      Class<A> throwableA,
                                                      Class<B> throwableB,
                                                      Class<C> throwableC,
                                                      Class<D> throwableD)
            throws A, B, C, D {
            try {
                return future.get();
            } catch (InterruptedException e) {
                throw new UnrecoverableCorfuInterruptedError("Interrupted while completing future", e);
            } catch (ExecutionException ee) {
                if (throwableA.isInstance(ee.getCause())) {
                    throw (A) ee.getCause();
                }
                if (throwableB.isInstance(ee.getCause())) {
                    throw (B) ee.getCause();
                }
                if (throwableC.isInstance(ee.getCause())) {
                    throw (C) ee.getCause();
                }
                if (throwableD.isInstance(ee.getCause())) {
                    throw (D) ee.getCause();
                }
                throw new RuntimeException(ee.getCause());
            }
    }

    public static <T,
            A extends Throwable,
            B extends Throwable,
            C extends Throwable> T getUninterruptibly(Future<T> future,
                                                      Class<A> throwableA,
                                                      Class<B> throwableB,
                                                      Class<C> throwableC)
            throws A, B, C {
        return getUninterruptibly(future, throwableA, throwableB, throwableC,
                RuntimeException.class);
    }

    public static <T,
            A extends Throwable,
            B extends Throwable> T getUninterruptibly(Future<T> future,
                                                      Class<A> throwableA,
                                                      Class<B> throwableB)
            throws A, B {
        return getUninterruptibly(future, throwableA, throwableB, RuntimeException.class,
                RuntimeException.class);
    }

    public static <T, A extends Throwable> T getUninterruptibly(Future<T> future,
                                                                Class<A> throwableA)
            throws A {
        return getUninterruptibly(future, throwableA, RuntimeException.class,
                RuntimeException.class, RuntimeException.class);
    }

    public static <T> T getUninterruptibly(Future<T> future) {
        return getUninterruptibly(future, RuntimeException.class, RuntimeException.class,
                RuntimeException.class, RuntimeException.class);
    }

    /** A static timeout exception that we complete futures exceptionally with. */
    static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

    /**
     * Generates a completable future which times out.
     * inspired by NoBlogDefFound: http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param duration The duration to timeout after.
     * @param <T>      Ignored, since the future will always timeout.
     * @return A completable future that will time out.
     */
    public static <T> CompletableFuture<T> failAfter(Duration duration) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        scheduler.schedule(() -> promise.completeExceptionally(TIMEOUT_EXCEPTION),
                                        duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    /**
     * Schedules a runnable after a given time
     *
     * @param duration The duration to timeout after.
     */
    public static void runAfter(Duration duration, Runnable toRun) {
        final CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.schedule(toRun::run, duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Takes a completable future, and ensures that it completes within a certain duration.
     * If it does not, it is cancelled and completes exceptionally with TimeoutException.
     * inspired by NoBlogDefFound: www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param future   The completable future that must be completed within duration.
     * @param duration The duration the future must be completed in.
     * @param <T>      The return type of the future.
     * @return         A completable future which completes with the original value if completed
     *                 within duration, otherwise completes exceptionally with TimeoutException.
     */
    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration) {
        final CompletableFuture<T> timeout = failAfter(duration);
        return future.applyToEither(timeout, Function.identity());
    }
}
