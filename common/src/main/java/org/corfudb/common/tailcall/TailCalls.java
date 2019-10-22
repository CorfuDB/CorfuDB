package org.corfudb.common.tailcall;

/**
 * A convenience class to easily operate with TailCall instances.
 */
public class TailCalls {
    private TailCalls(){
        // Singleton.
    }

    /**
     * Receive a tail call and pass it along.
     * @param nextCall Next tail call to execute.
     * @param <T> A type of object that returns when the tail call function executes.
     * @return A next instance of tail call.
     */
    public static <T> TailCall<T> call(final TailCall<T> nextCall) {
        return nextCall;
    }

    /**
     * Return a special version of TailCall that indicates recursion termination.
     * @param value A base result of a recursive call.
     * @param <T> A type of value.
     * @return An instance of a done TailCall.
     */
    public  static <T> TailCall<T> done(final T value) {
        return new TailCall<T>() {
            @Override
            public boolean isComplete() {
                return true;
            }

            @Override
            public T result() {
                return value;
            }

            @Override
            public TailCall<T> apply() {
                throw new IllegalStateException();
            }
        };
    }
}