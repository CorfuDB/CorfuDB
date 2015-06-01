package org.corfudb.runtime.view;

import org.corfudb.runtime.stream.IStream;

import java.util.UUID;

/**
 * This interface represents a CorfuDB instance.
 *
 * A CorfuDB instance contains the following:
 *
 * * A Configuration Master, used for configuring the instance.
 * * A Sequencer, used for issuing tokens (which may be streaming or non streaming).
 * * A WriteOnceAddressSpace, the persistent storage area for the log.
 */
public interface ICorfuDBInstance {

    /**
     * Gets a configuration master for this instance.
     * @return  The configuration master for this instance.
     */
    IConfigurationMaster getConfigurationMaster();

    /**
     * Gets a streaming sequencer for this instance.
     * @return  The streaming sequencer for this instance.
     */
    IStreamingSequencer getStreamingSequencer();

    /**
     * Gets a sequencer (regular) for this instance.
     * @return  The sequencer for this instance.
     */
    ISequencer getSequencer();

    /**
     * Gets a write-once address space for this instance.
     * @return  A write-once address space for this instance.
     */
    IWriteOnceAddressSpace getAddressSpace();

    /**
     * Gets a unique identifier for this instance.
     * @return  A unique identifier for this instance.
     */
    UUID getUUID();

    /**
     * Gets the current view of this instance.
     * @return  A view of this instance.
     */
    CorfuDBView getView();

    /**
     * Opens a stream given its identifier using this instance, or creates it
     * on this instance if one does not exist.
     * @param id    The unique ID of the stream to be opened.
     * @return      The stream, if it exists. Otherwise, a new stream is created
     *              using this instance.
     */
    IStream openStream(UUID id);

    /**
     * Delete a stream given its identifier using this instance.
     * @param id    The unique ID of the stream to be deleted.
     * @return      True, if the stream was successfully deleted, or false if there
     *              was an error deleting the stream (does not exist).
     */
    boolean deleteStream(UUID id);
}
