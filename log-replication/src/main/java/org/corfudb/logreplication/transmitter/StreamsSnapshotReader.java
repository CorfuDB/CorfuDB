package org.corfudb.logreplication.transmitter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.StreamViewSMRAdapter;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
/**
 *  A class that represents the default implementation of a Snapshot Reader for Log Replication functionality.
 *
 *  This implementation provides transmit at the stream level (no coalesced state).
 *  It generates TxMessages which will be transmitted by the SnapshotListener (provided by the application).
 */
public class StreamsSnapshotReader implements SnapshotReader {
    //Todo: will change the max_batch_size while Maithem finish the new API
    private final int MAX_BATCH_SIZE = 1;
    private final MessageType MSG_TYPE = MessageType.SNAPSHOT_MESSAGE;
    private long globalSnapshot;
    private Set<String> streams;
    private CorfuRuntime rt;
    private long preMsgTs;
    private long currentMsgTs;
    private LogReplicationConfig config;

    /**
     * Set runtime and callback function to pass message to network
     */
    public StreamsSnapshotReader(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
        this.config = config;
    }
    /**
     * setup globalSnapshot
     */
    void setup() {
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
    void next(String streamName) {
        UUID streamID = CorfuRuntime.getStreamID(streamName);
        ArrayList<SMREntry> entries = new ArrayList<>(readStream(streamID));
        preMsgTs = Address.NON_ADDRESS;

        for (int i = 0; i < entries.size(); i += MAX_BATCH_SIZE) {
            List<SMREntry> msg_entries = entries.subList(i, i + MAX_BATCH_SIZE);
            TxMessage txMsg = generateMessage(msg_entries);

            //update preMsgTs only after process a msg successfully
            preMsgTs = currentMsgTs;
            log.debug("Successfully pass a TxMsg {}", txMsg.getMetadata());
        }

        log.info("Successfully pass a stream {} for globalSnapshot {}", streamName, globalSnapshot);
        return;
    }

    /**
     * while transmit finish put an event to the queue
     */
    public void sync() {
        setup();
        try {
            for (String streamName : streams) {
                next(streamName);
            }
        } catch (Exception e) {
            //handle exception
            log.warn("Sync call get an exception ", e);
            throw e;
        }

        //todo: update metadata to record a Snapshot Reader done
        log.info("Successfully do a transmit read for globalSnapshot {}", globalSnapshot);
    }

    @Override
    public SnapshotReadMessage read() {
        return null;
    }

    @Override
    public void reset(long snapshotTimestamp) {

    }
}
