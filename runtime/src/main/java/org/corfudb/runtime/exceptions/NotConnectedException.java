package org.corfudb.runtime.exceptions;

import org.corfudb.runtime.CorfuRuntime;

/** {@link NotConnectedException} is thrown when a {@link org.corfudb.runtime.CorfuRuntime} is
 *  not yet connected to a Corfu cluster.
 *
 *  <p>Usually this is due to not calling {@link CorfuRuntime#connect()} before performing data
 *  operations.
 */
public class NotConnectedException extends RuntimeException {

    /**
     * Construct a new {@link NotConnectedException}.
     */
    public NotConnectedException() {
        super("Not connected - did you call CorfuRuntime::connect?");
    }
}
