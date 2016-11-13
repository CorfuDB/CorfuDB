package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;

import java.util.UUID;

/** A Corfu container object is a container for other Corfu objects.
 * It has explicit access to its own stream ID, and a runtime, allowing it
 * to manipulate and return other Corfu objects.
 *
 * Created by mwei on 11/12/16.
 */
public interface ICorfuContainer {

    /** Get a reference to a CorfuRuntime.
     *
     * @return  A CorfuRuntime which allows the object to manipulate
     *          the state of the log.
     */
    CorfuRuntime getRuntime();
    CorfuRuntime setRuntime();

    /** Get the stream ID that this container belongs to.
     *
     * @return
     */
    UUID getStreamID();
}
