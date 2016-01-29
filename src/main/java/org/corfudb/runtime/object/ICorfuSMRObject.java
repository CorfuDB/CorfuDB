package org.corfudb.runtime.object;

import org.corfudb.runtime.exceptions.UnprocessedException;

import java.lang.annotation.Inherited;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Created by mwei on 1/7/16.
 */
public interface ICorfuSMRObject<T> {

    // These calls are dynamically overridden by the proxy, and should not be
    // implemented by client classes.

    /** Get the initial SMR object.
     *
     * @param arguments The arguments used by the constructor.
     * @return          The initial state of the SMR object.
     */
    default T initialObject(Object... arguments) { throw new UnprocessedException(); }

    /** Get the current SMR object.
     *
     * @return          The current state of the SMR object.
     */
    default T getSMRObject() { throw new UnprocessedException(); }

    /** Returns whether the object is in a transaction. During this time, speculative
     * updates may be applied until either the commit or abort hook is called.
     * @return          True, if the object is in a transaction. False, otherwise.
     */
    default boolean isInTransaction() {
        return TransactionalContext.isInTransaction();
    }

}
