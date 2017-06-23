package org.corfudb.util.retry;

import java.util.function.Consumer;

import lombok.SneakyThrows;


/**
 * The IRetry interface is used to support retry loops by using different strategies
 * for the pause between the retries. It is done by a builder, that needs the
 * correct strategy and the lambda to be executed. The code in the run function is responsible
 * to handle the exceptions and to decide whether a retry is needed or the exception should be
 * thrown outside.
 *
 * <p>Created by mwei on 9/1/15
 */

public interface IRetry<E extends Exception, F extends Exception, G extends Exception,
        H extends Exception, O, A extends IRetry> {


    static <R, Z extends IRetry> IRetry<RuntimeException, RuntimeException, RuntimeException,
            RuntimeException, R, Z> build(Class<Z> retryType, IRetryable<RuntimeException,
            RuntimeException, RuntimeException, RuntimeException, R> runFunction) {
        return build(retryType, RuntimeException.class, RuntimeException.class,
                RuntimeException.class, RuntimeException.class, runFunction);
    }

    static <T extends Exception, R, Z extends IRetry> IRetry<T, RuntimeException, RuntimeException,
            RuntimeException, R, Z> build(Class<Z> retryType, Class<? extends T> firstExceptionType,
                                          IRetryable<T, RuntimeException, RuntimeException,
                                                  RuntimeException, R> runFunction) {
        return build(retryType, firstExceptionType, RuntimeException.class, RuntimeException.class,
                RuntimeException.class, runFunction);
    }

    static <T extends Exception, U extends Exception, R, Z extends IRetry> IRetry<T, U,
            RuntimeException, RuntimeException, R, Z> build(Class<Z> retryType,
                                                            Class<? extends T> firstExceptionType,
            Class<? extends U> secondExceptionType, IRetryable<T, U, RuntimeException,
            RuntimeException, R> runFunction) {
        return build(retryType, firstExceptionType, secondExceptionType, RuntimeException.class,
                RuntimeException.class, runFunction);
    }

    static <T extends Exception, U extends Exception, V extends Exception, R,
            Z extends IRetry> IRetry<T, U, V, RuntimeException, R,
            Z> build(Class<Z> retryType,Class<? extends T> firstExceptionType,
                                                Class<? extends U> secondExceptionType,
            Class<? extends V> thirdExceptionType, IRetryable<T, U, V,
            RuntimeException, R> runFunction) {
        return build(retryType, firstExceptionType, secondExceptionType, thirdExceptionType,
                RuntimeException.class, runFunction);
    }

    @SneakyThrows
    static <T extends Exception, U extends Exception, V extends Exception, W extends Exception, R,
            Z extends IRetry>
                IRetry<T, U, V, W, R, Z> build(Class<Z> retryType,
                                               Class<? extends T> firstExceptionType,
                                               Class<? extends U> secondExceptionType,
                                               Class<? extends V> thirdExceptionType,
                                               Class<? extends W> fourthExceptionType, IRetryable<T,
            U, V,
            W, R> runFunction) {
        return (IRetry<T, U, V, W, R, Z>) retryType.getConstructor(IRetryable.class)
                .newInstance(runFunction);
    }


    /**
     * Run the retry. This function may never return.
     */
    default O run()
            throws E, F, G, H, InterruptedException {
        while (true) {
            try {
                return getRunFunction().retryFunction();
            } catch (RetryNeededException ex) {
                // retry requested
                nextWait();
            }
        }
    }

    /**
     * Get the function that needs to be retried.
     */
    IRetryable<E, F, G, H, O> getRunFunction();


    /**
     * Configure settings for the underlying class.
     * The consumer will be given access to the underlying retry type to configure
     * type-specific options.
     *
     * @param settingsFunction A consumer with access to the underlying retry type.
     * @return An IRetry, to support a fluent API interface.
     */
    @SuppressWarnings("unchecked")
    default IRetry<E, F, G, H, O, A> setOptions(Consumer<A> settingsFunction) {
        settingsFunction.accept((A) this);
        return this;
    }

    /**
     * Apply the retry logic.
     */
    void nextWait() throws InterruptedException;



}
