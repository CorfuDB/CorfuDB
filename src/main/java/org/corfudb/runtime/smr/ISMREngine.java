package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An interface to an SMR engine
 * Created by mwei on 5/1/15.
 */
public interface ISMREngine<T> {

    interface ISMREngineOptions
    {

    }

    /**
     * Get the underlying object. The object is dynamically created by the SMR engine.
     * @return          The object maintained by the SMR engine.
     */
    T getObject();

    /**
     * Synchronize the SMR engine to a given timestamp, or pass null to synchronize
     * the SMR engine as far as possible.
     * @param ts        The timestamp to synchronize to, or null, to synchronize to the most
     *                  recent version.
     */
    void sync(ITimestamp ts);

    /**
     * Propose a new command to the SMR engine.
     * @param command   A lambda (BiConsumer) representing the command to be proposed.
     *                  The first argument of the lambda is the object the engine is acting on.
     *                  The second argument of the lambda contains some TX that the engine
     *
     * @return          A timestamp representing the timestamp that the command was proposed to.
     */
    ITimestamp propose(ISMREngineCommand<T> command);
}