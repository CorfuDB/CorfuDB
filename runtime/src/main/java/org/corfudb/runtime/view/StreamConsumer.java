package org.corfudb.runtime.view;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

public class StreamConsumer implements Consumer {

    final UUID streamID;
    final CorfuRuntime rt;
    long lastAddress = -1;

    public StreamConsumer(UUID streamID, CorfuRuntime runtime) {
        this.streamID = streamID;
        this.rt = runtime;
    }

    @Override
    public List<ILogData> poll(long timeout) {
        List<ILogData> data = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime <= timeout) {

            Long globalOffset = rt.getSequencerView().query(streamID).getToken().getTokenValue();

            if (globalOffset - lastAddress > 0) {
                data.addAll(rt.getStreamsView().get(streamID).remainingUpTo(globalOffset));
                break;
            }
        }

        return data;
    }
}
