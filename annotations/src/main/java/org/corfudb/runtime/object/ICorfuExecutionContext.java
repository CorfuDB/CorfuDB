package org.corfudb.runtime.object;

import org.corfudb.annotations.DontInstrument;

/**
 * Lets SMR objects define how they want to internally represent
 * changes that are brought on by the version locked object.
 *
 * @param <T> The underlying object type
 */
@FunctionalInterface
public interface ICorfuExecutionContext<T> {

    /**
     * Default (catch-all) context.
     */
    Context DEFAULT = new Context();

    /**
     * Non-committed (optimistic) context.
     */
    Context OPTIMISTIC = new Context();

    /**
     * Any sort of state associated with the context.
     */
    class Context { }

    /**
     * Returns the underlying object T which has been prepared to
     * execute under the specified context.
     *
     * For the data-structures that do not care about different contexts,
     * they should simply return "this".
     *
     * @param context context under which the operations will be applied
     * @return the underlying object for the specified context
     */
    @DontInstrument
    T getContext(Context context);
}
