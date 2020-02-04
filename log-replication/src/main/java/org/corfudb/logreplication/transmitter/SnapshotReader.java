package org.corfudb.logreplication.transmitter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.StreamViewSMRAdapter;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE;

@Slf4j
/**
 * The AR will call sync API and pass in the api for fullSyncDone(), handlerMsg()
 * Read streams one by one, get all the entries.
 */
public class SnapshotReader {
    //Todo: will change the max_batch_size while Maithem finish the new API
    private final int MAX_BATCH_SIZE = 1;
    private final MessageType MSG_TYPE = MessageType.SNAPSHOT_MESSAGE;
    private long fullSyncUUID;
    private long globalSnapshot;
    private Set<String> streams;
    private CorfuRuntime rt;
    private LogReplicationContext replicationContext;
    private long preMsgTs; //this timestamp is not used by receiver, for debugging usage for now
    private long currentMsgTs; //this timestamp is not used by receiver, for debugging usage for now

    /**
     * Set runtime and callback function to pass message to network
     * @param context
     */
    public SnapshotReader(LogReplicationContext context) {
        replicationContext = context;
        rt = replicationContext.getCorfuRuntime();
        streams = replicationContext.getConfig().getStreamsToReplicate();
        //should pass in fullSyncUUID here?
    }

    /**
     * For test case to use
     * @param rt
     * @param streams
     */
    public SnapshotReader(CorfuRuntime rt, Set<String> streams) {
        this.rt = rt;
        this.streams = streams;
    }

    /**
     * setup globalSnapshot
     */
    void setup(long fullSyncUUID) {
        this.fullSyncUUID = fullSyncUUID;
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        globalSnapshot = rt.getAddressSpaceView().getLogTail();
    }

    /**
     * get all entries for a stream up to the globalSnapshot
     * @param streamID
     * @return
     */
    List<SMREntry> readStream(UUID streamID) {
        StreamViewSMRAdapter smrAdapter =  new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(streamID));
        return smrAdapter.remainingUpTo(globalSnapshot);
    }

    TxMessage generateMessage(List<SMREntry> entries) {
        currentMsgTs = entries.get(entries.size() - 1).getGlobalAddress();
        TxMessage txMsg = new TxMessage(MSG_TYPE, currentMsgTs, preMsgTs, globalSnapshot);

        // todo: using Maithem API to generate msg data with entries.
        ByteBuf buf = Unpooled.buffer();
        entries.get(0).serialize(buf);
        txMsg.setData(buf.array());

        log.debug("Generate TxMsg {}", txMsg.getMetadata());
        //set data, will plug in meithem's new api
        return  txMsg;
    }

    /**
     * Given a stream name, get all entries for this stream,
     * put entries in a message and call the callback handler
     * to pass the message to the other site.
     * @param streamName
     */
    void sync(String streamName) {
        UUID streamID = CorfuRuntime.getStreamID(streamName);
        ArrayList<SMREntry> entries = new ArrayList<>(readStream(streamID));
        preMsgTs = Address.NON_ADDRESS;

        for (int i = 0; i < entries.size(); i += MAX_BATCH_SIZE) {
            List<SMREntry> msg_entries = entries.subList(i, i + MAX_BATCH_SIZE);
            TxMessage txMsg = generateMessage(msg_entries);
            replicationContext.getSnapshotListener().onNext(txMsg);
            preMsgTs = currentMsgTs;
            log.debug("Succesfully pass a TxMsg {}", txMsg.getMetadata());
        }

        log.info("Succesfully pass a stream {} for globalSnapshot {}", streamName, globalSnapshot);
        return;
    }

    /**
     * while sync finish put an event to the queue
     */
    public void sync(long fullSyncUUID) {
        setup(fullSyncUUID);
        try {
            for (String streamName : streams) {
                sync(streamName);
            }
        } catch (Exception e) {
            //handle exception
            log.warn("Sync call get an exception ", e);
            throw e;
        }

        // todo: update metadata to record a Snapshot Reader done
        //replicationContext.getSnapshotListener().complete();
        replicationContext.getDataTransmitter().getLogReplicationFSM().input(new LogReplicationEvent(SNAPSHOT_SYNC_COMPLETE ));
        log.info("Snapshot reader has complete full sync {} for globalSnapshot {}", fullSyncUUID, globalSnapshot);
    }
}
