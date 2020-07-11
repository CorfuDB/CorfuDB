package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.protocols.wireprotocol.ILogData;

import java.util.List;

/**
 * Interface for snapshot/log entry read pre-processing.
 */
public interface ReadProcessor {

    /**
     * Process a snapshot/log entry read for any transformation
     * before sending back to the application callback.
     *
     * @param logEntries list of log data
     *
     * @return
     */
    List<byte[]> process(List<ILogData> logEntries);

    /**
     *
     * @param logEntry
     * @return
     */
    byte[] process(ILogData logEntry);

}
