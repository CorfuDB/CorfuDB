package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalLogDataSizeException;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalTransactionStreamsException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.Set;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;

@Slf4j
public class StreamsReader {
    protected CorfuRuntime rt;

    /**
     * The max size of data for SMR entries in data message.
     */
    protected int maxDataSizePerMsg;

    @Setter
    protected long topologyConfigId;

    protected long preMsgTs;

    protected long currentMsgTs;

    @Getter
    public boolean hasNoiseData = false;

    @Getter
    public boolean hasInvalidDataSize = false;

    // A set of streams names to be replicated
    protected Set<String> streams;

    /**
     * Given a list of SMREntries, calculate the total size  in bytes.
     *
     * @param smrEntries
     * @return
     */
    public static int calculateSize(List<SMREntry> smrEntries) {
        int size = 0;
        for (SMREntry entry : smrEntries) {
            size += entry.getSerializedSize();
        }

        log.trace("current entry sizeInBytes {}", size);
        return size;
    }

    /**
     * Given an opaque entry, calculate the total size in bytes.
     *
     * @param opaqueEntry
     * @return
     */
    public static int calculateOpaqueEntrySize(OpaqueEntry opaqueEntry) {
        int size = 0;
        for (List<SMREntry> smrEntryList : opaqueEntry.getEntries().values()) {
            size += calculateSize(smrEntryList);
        }
        return size;
    }

    /**
     * Examing the currentEntrySize and decide if it can be appended to the current msg.
     * @param currentMsgSize
     * @param currentEntrySize
     * @return
     */
    public boolean shouldAppend(int currentMsgSize, int currentEntrySize) {

        // If the interested data size is bigger than the system supported, we should stop and report error
        if (currentEntrySize > MAX_DATA_MSG_SIZE_SUPPORTED) {
            log.error("The interested log entry data size {} is bigger than the supported size {}", currentEntrySize, MAX_DATA_MSG_SIZE_SUPPORTED);
            hasInvalidDataSize = true;
            return false;
        }

        // If the interested data size is bigger than the config the max size, log a warning.
        if (currentEntrySize > maxDataSizePerMsg) {
            log.warn("The current entry size {} is bigger than the configured maxDataSizePerMsg {}",
                    currentEntrySize, maxDataSizePerMsg);
        }

        // If there is not data in the current message, append the entry.
        // If there is enough space in the current message, append the entry
        if (currentMsgSize == 0 || currentEntrySize + currentMsgSize < maxDataSizePerMsg) {
            return true;
        }

        return false;
    }

    /**
     * If there are invalid streams throw a proper exception.
     */
    public void checkValidStreams() {
        if (hasNoiseData) {
            throw new IllegalTransactionStreamsException("The log entry has illegal streams");
        }

        if (hasInvalidDataSize) {
            throw new IllegalLogDataSizeException("The log data size is not supported");
        }
    }

    /**
     *
     * @param topologyConfigId
     */
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }
}
