package org.corfudb.runtime.object;

import org.corfudb.runtime.exceptions.UnprocessedException;

/**
 * Created by mwei on 1/7/16.
 */
@Deprecated
public interface ICorfuSMRObject<T> extends ICorfuObject {

    // These calls are dynamically overridden by the proxy, and should not be
    // implemented by client classes.

    /**
     * Get the initial SMR object.
     *
     * @param arguments The arguments used by the constructor.
     * @return The initial state of the SMR object.
     */
    default T initialObject(Object... arguments) {
        throw new UnprocessedException();
    }

    /**
     * Get the current SMR object.
     *
     * @return The current state of the SMR object.
     */
    default T getSMRObject() {
        throw new UnprocessedException();
    }

    @FunctionalInterface
    interface SMRHandlerMethod {
        void handle(String method, Object[] args, Object state);
    }

}
