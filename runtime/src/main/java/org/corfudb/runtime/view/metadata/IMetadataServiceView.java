package org.corfudb.runtime.view.metadata;

import java.util.UUID;

/**
 * This interface defines the operations provided by the metedata service.
 */

public interface IMetadataServiceView {

    /**
     * Create a stream
     * @param stream The stream to create
     * @throws StreamAlreadyExistsException This exception is thrown if a previous create opertaion
     * succeeded with the same stream uuid
     */
    void createStream(UUID stream) throws StreamAlreadyExistsException;

    /**
     * Update a stream with a new checkpoint
     * @param stream The stream's address
     * @param checkpoint New checkpoint
     * @throws InvalidCheckpointException This exception is thrown when the new checkpoint is logically older than
     * the last successful checkpoint update (i.e. Only monotonic updates are allowed)
     */
    void updateStreamCheckpoint(UUID stream, Checkpoint checkpoint) throws InvalidCheckpointException;

    /**
     * Retreive a checkpoint for a stream
     * @param stream stream's UUID
     * @return Checkpoint data or null
     */
    Checkpoint getStreamCheckpoint(UUID stream);

    /**
     * Delete a stream
     * @param stream The stream to be deleted
     */
    void deleteStream(UUID stream);
}
