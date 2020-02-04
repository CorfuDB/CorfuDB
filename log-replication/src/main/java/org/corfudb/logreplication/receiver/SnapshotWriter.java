package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.logreplication.transmitter.DataTransmitter;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

/**
 * The AR will pass in FullSyncQue and DeltaQue and API for fullSyncDone()
 * Open streams interested and append all entries
 */

public class SnapshotWriter {
    private LogReplicationContext replicationContext;
    private Set<String> streams;
    CorfuRuntime rt;
    long proccessedMsgTs;
    HashMap<UUID, IStreamView> streamViewMap;
    private long srcGlobalSnapshot;
    private final MessageType MSG_TYPE = MessageType.SNAPSHOT_MESSAGE;

    SnapshotWriter(LogReplicationContext context) {
        replicationContext = context;
        rt = replicationContext.getCorfuRuntime();
        //streamUUIDs = streamUUIDs(replicationContext.getConfig().getStreamsToReplicate());
        // setup fullsyncUUID?
    }

    /**
     * clear all tables interested
     */
    void clearTables() {
        for (String stream : streams) {
            CorfuTable<String, String> corfuTable = rt.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamName(stream)
                    .open();
            corfuTable.clear();
            corfuTable.close();
        }
    }

    /**
     * open all streams interested
     */
    void openStreams() {
        for (String stream : streams) {
            UUID streamID = CorfuRuntime.getStreamID(stream);
            IStreamView sv = rt.getStreamsView().getUnsafe(streamID);
            streamViewMap.put(streamID, sv);
        }
    }

    void processSMREntries(UUID streamId, List<SMREntry> entries) {
        for (SMREntry entry : entries) {
            streamViewMap.get(streamId).append(entry);
        }
    }

    /**
     * if the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(MessageMetadata metadata) {
    }

    // If the message is out of order, buffer it. If buffer is overflown, thrown an exception.
    // Will define the exception type later.
    // will use Maithem's api to query the msg to get the uuid and
    // list of smr entries
    void processTxMessage(TxMessage msg) throws Exception {
        MessageMetadata metadata = msg.getMetadata();
        verifyMetadata(metadata);
        TxMessage currentMsg = null;

        //List<SMREntry> entries; //get the entries from the msg
        //processSMREntries(streamID, entries);
    }

    /**
     *
     */
    void setup(long snapshot) {
       srcGlobalSnapshot = snapshot;
    }

    /**
     * The fullSyncQue guarantee the ordering of the messages.
     */
    void processFullSyncQue(long srcGlobalSnapshot) {
        clearTables();
        setup(srcGlobalSnapshot);
        //get message, call processTxMessage()
    }
}
