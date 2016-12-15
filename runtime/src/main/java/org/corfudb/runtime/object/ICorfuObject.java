package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.UUID;

/**
 * Created by mwei on 3/30/16.
 */
@Deprecated
public interface ICorfuObject {

    /**
     * Returns whether the object is in a transaction. During this time, speculative
     * updates may be applied until either the commit or abort hook is called.
     *
     * @return True, if the object is in a transaction. False, otherwise.
     */
    default boolean isInTransaction() {
        return TransactionalContext.isInTransaction();
    }

    // These calls are dynamically overridden by the proxy, and should not be
    // implemented by client classes.

    /**
     * Get the stream ID of the object.
     *
     * @return The stream ID of the object.
     */
    default UUID getStreamID() {
        throw new UnprocessedException();
    }

    /**
     * Get the current runtime.
     *
     * @return The runtime of the object.
     */
    default CorfuRuntime getRuntime() {
        throw new UnprocessedException();
    }

    /**
     * Get the underlying proxy.
     *
     * @return The underlying proxy for this object.
     */
    default CorfuObjectProxy getProxy() {
        throw new UnprocessedException();
    }

    /**
     * Get the mode in which Mutators/Accessors should be accessed in.
     * @return  A thread local representing the current execution context.
     */
    default ThreadLocal<Boolean> getMethodAccessMode() { throw new UnprocessedException(); }
}
