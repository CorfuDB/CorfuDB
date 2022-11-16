package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.infrastructure.logreplication.replication.fsm.AbstractLogReplicationFSM;
import org.corfudb.runtime.collections.StreamListener;
import java.util.HashSet;
import java.util.Set;

public abstract class LRClientEventListener implements StreamListener {

    Set<AbstractLogReplicationFSM> registeredFsmsSet = new HashSet<>();

    void registerFsm(AbstractLogReplicationFSM fsm) {
        registeredFsmsSet.add(fsm);
    }

    void unregisterFsm(AbstractLogReplicationFSM fsm) {
        registeredFsmsSet.remove(fsm);
    }
}
