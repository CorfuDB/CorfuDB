package org.corfudb.runtime;

import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.FullStateMessage;

import java.util.List;

public class RoutingQueueReceiverClient {
    /**
     * Receives batch of the delta messages.
     * Each message in the list represent one transaction. Processing of entire batch must be transient.
     *
     * @param messages list of transactional delta messages
     */
    void receiveDeltaMessages(List<RoutingTableEntryMsg> messages) {}

    /**
     * Asks application to receive new full state in synchronous mode.
     * Application must implement one of receiveNewState methods and can throw UnsupportedOperationException
     * if method is not supported.
     * <p>
     * Application must apply deltas to inconsistent set of full data to get consistent state.
     */
    void receiveNewState(String sourceSiteId, List<FullStateMessage> fullState) {}
}
