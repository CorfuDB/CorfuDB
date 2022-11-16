package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.runtime.collections.CorfuStreamEntries;

public class RoutingQueueClientEventListener extends LRClientEventListener {

    enum EventType {
        REQUEST_SNAPSHOT_SYNC

        // Note: Snapshot data end can just be an end marker in the stream of data instead of a separate event type
    };

    @Override
    public void onNext(CorfuStreamEntries entries) {

    }

    @Override
    public void onError(Throwable t) {

    }
}
