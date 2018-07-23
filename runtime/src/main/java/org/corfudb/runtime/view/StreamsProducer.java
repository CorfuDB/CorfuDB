package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.UUID;

public class StreamsProducer implements Producer {
    final CorfuRuntime runtime;
    final UUID streamID;

    public StreamsProducer(CorfuRuntime rt, UUID id) {
        runtime = rt;
        streamID = id;
    }

    public long send(Object payload) {
        while(true) {
            try {
                return runtime.getStreamsView().append(payload, null, streamID);
            } catch (TransactionAbortedException e) {
                continue;
            }
        }
    }
}
