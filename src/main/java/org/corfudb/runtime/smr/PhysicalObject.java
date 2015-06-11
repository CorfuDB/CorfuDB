package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

/**
 * Created by mwei on 6/5/15.
 */
public class PhysicalObject<T> implements ISMRCheckpoint<T>, ISMREngineCommand<T, T> {

    T physicalObject;

    public PhysicalObject(T object)
    {
        this.physicalObject = object;
    }

    /**
     * Get the checkpointed object
     *
     * @return The state captured by this checkpoint.
     */
    @Override
    public T getCheckpoint() {
        return physicalObject;
    }

    /**
     * Get the checkpoint position
     *
     * @return A timestamp representing the version that the checkpoint represents,
     * or null if the timestamp reflects the most current state.
     */
    @Override
    public ITimestamp getCheckpointPosition() {
        return null;
    }

    /**
     * Performs this operation on the given arguments.
     *
     * @param t                 the first input argument
     * @param ismrEngineOptions the second input argument
     */
    @Override
    public T apply(T t, ISMREngine.ISMREngineOptions<T> ismrEngineOptions) {
        ismrEngineOptions.setUnderlyingObject(physicalObject);
        return physicalObject;
    }
}
