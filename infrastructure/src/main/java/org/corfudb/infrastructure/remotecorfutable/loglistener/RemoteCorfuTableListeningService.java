package org.corfudb.infrastructure.remotecorfutable.loglistener;

import java.util.Observable;
import java.util.UUID;

/**
 * This abstract class serves as the scheduler for the Log Listener.
 *
 * Created by nvaishampayan517 on 08/26/21
 */
public abstract class RemoteCorfuTableListeningService extends Observable {

    /**
     * This method will be used to begin polling the specified stream.
     * @param streamId Stream to poll
     */
    public abstract void addStream(UUID streamId);

    /**
     * This method will indicate to the service to stop polling the specified stream.
     * @param streamId Stream to remove from service
     */
    public abstract void removeStream(UUID streamId);


    /**
     * Shutdown the service.
     */
    public abstract void shutdown();
}
