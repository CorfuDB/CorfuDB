package org.corfudb.infrastructure.remotecorfutable.loglistener;

import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;

import java.util.Observable;
import java.util.UUID;

/**
 * This interface serves as the scheduler for the Log Listener.
 *
 * Created by nvaishampayan517 on 08/26/21
 */
public interface RemoteCorfuTableListeningService {

    /**
     * This method will be used to begin polling the specified stream.
     * @param streamId Stream to poll
     */
    void addStream(UUID streamId);

    /**
     * This method will indicate to the service to stop polling the specified stream.
     * @param streamId Stream to remove from service
     */
    void removeStream(UUID streamId);

    /**
     * This method will receive the next task to process from the listener.
     * @return SMROperation to apply to the database
     */
    SMROperation getTask();


    /**
     * Shutdown the service.
     */
    void shutdown();
}
