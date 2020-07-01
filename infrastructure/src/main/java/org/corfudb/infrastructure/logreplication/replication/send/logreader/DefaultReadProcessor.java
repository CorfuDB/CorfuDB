package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class represents the default read processor implementation to be used by the StreamsSnapshotReader
 */
public class DefaultReadProcessor implements ReadProcessor {

    private CorfuRuntime runtime;

    public DefaultReadProcessor(CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public List<byte[]> process(List<ILogData> logEntries) {
        // Not actual code just tmp
        return logEntries.stream().map(logEntry -> (byte[])logEntry.getPayload(runtime)).
                collect(Collectors.toList());
    }

    @Override
    public byte[] process(ILogData logEntry) {
        // Not actual code just tmp
        return (byte[])logEntry.getPayload(runtime);
    }
}
