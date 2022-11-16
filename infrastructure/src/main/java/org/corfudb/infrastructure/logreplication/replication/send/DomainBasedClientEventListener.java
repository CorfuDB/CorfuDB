package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import java.util.HashSet;
import java.util.Set;

public class DomainBasedClientEventListener implements LRClientEventListener {

    enum EventType {
        DOMAIN_DESTINATION_UPDATE
    };

    private Set<LogReplicationFSM> registeredFsmSet = new HashSet<>();

    @Override
    public void onNext(CorfuStreamEntries entries) {

    }

    @Override
    public void onError(Throwable t) {

    }

    @Override
    public void registerFsm(LogReplicationFSM fsm) {
        registeredFsmSet.add(fsm);
    }

    @Override
    public void unregisterFsm(LogReplicationFSM fsm) {
        registeredFsmSet.remove(fsm);
    }
}
