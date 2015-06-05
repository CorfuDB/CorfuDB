package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

/**
 * Created by mwei on 6/5/15.
 */
public interface ISMRCheckpoint<T> {

    /**
     * Get the checkpointed object
     * @return      The state captured by this checkpoint.
     */
    T getCheckpoint();

    /**
     * Get the checkpoint position
     * @return      A timestamp representing the version that the checkpoint represents,
     *              or null if the timestamp reflects the most current state.
     */
    ITimestamp getCheckpointPosition();

}
