package org.corfudb.common.tailcall;

import java.util.Optional;
import java.util.stream.Stream;

/**
 *
 * An interface that represents a tail-recursive call. A tail-recursive function is a function,
 * whose very last call is a call to itself. This interface allows a conversion of a
 * regular recursive call into a tail call to make a recursion practical for the large inputs
 * (avoids a StackOverflowException), aka Tail-Call Optimization.
 *
 * @author pzaytsev
 */
@FunctionalInterface
public interface TailCall<T> {
    /**
     * Applies a function and returns immediately with an next instance of TailCall
     * ready for execution.
     * @return A result of a call wrapped into the TailCall.
     */
    TailCall<T> apply();

    /**
     * Returns false when the recursion is not done yet.
     * @return False.
     */
    default boolean isComplete() {
        return false;
    }

    /**
     * Returns a result stored in the TailCall.
     * @return A result.
     */
    default T result() {
        throw new IllegalStateException();
    }

    /**
     * Lazily iterates over the pending tail call recursions until it
     * reaches the end.
     * @return An optional result of a recursive calls.
     */
    default Optional<T> invoke() {
        return Stream.iterate(this, TailCall::apply)
                .filter(TailCall::isComplete)
                .findFirst()
                .map(TailCall::result);
    }
}