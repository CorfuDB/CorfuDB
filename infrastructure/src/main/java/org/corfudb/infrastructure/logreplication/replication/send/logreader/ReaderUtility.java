package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;

import java.util.List;

@Slf4j
public class ReaderUtility {
    /**
     * Given a list of SMREntries, calculate the total size  in bytes.
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
}
