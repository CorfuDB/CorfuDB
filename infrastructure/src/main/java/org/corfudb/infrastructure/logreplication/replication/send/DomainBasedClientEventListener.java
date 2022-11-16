package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.runtime.collections.CorfuStreamEntries;

public class DomainBasedClientEventListener extends LRClientEventListener {

    enum EventType {
        DOMAIN_DESTINATION_UPDATE
    };

    @Override
    public void onNext(CorfuStreamEntries entries) {

    }

    @Override
    public void onError(Throwable t) {

    }
}
