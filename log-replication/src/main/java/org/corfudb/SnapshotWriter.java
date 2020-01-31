package org.corfudb;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Open streams interested and append all entries
 */

public class SnapshotWriter {
    private Set<UUID> streamUUIDSet;
    HashMap<UUID, IStreamView> streamViewMap;
    CorfuRuntime rt;

    /**
     * clear all tables interested
     */
    void clearTables() {
        for (UUID stream : streamUUIDSet) {
            CorfuTable<String, String> corfuTable = rt.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamID(stream)
                    .open();
            corfuTable.clear();
            corfuTable.close();
        }
    }

    /**
     * open all streams interested
     */
    void openStreams() {
        for (UUID streamID : streamUUIDSet) {
            IStreamView sv = rt.getStreamsView().getUnsafe(streamID);
            streamViewMap.put(streamID, sv);
        }
    }

    void processSMREntry(SMREntry entry) {
        //streamViewMap.get(uuid).append(entry);
    }

    void processSMREntries(List<SMREntry> entries) {
        for (SMREntry entry : entries) {
            processSMREntry(entry);
        }
    }

    //If the message is out of order, buffer it?
    void processTxMessage(TxMessage msg) {

    }
}
