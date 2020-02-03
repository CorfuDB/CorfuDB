package org.corfudb.logreplication.transmitter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.ArrayList;
import java.util.List;

import static org.corfudb.runtime.view.Address.MAX;
@Slf4j
public class LogEntryReader {
    private IStreamView stream;
    private long globalBaseSnapshot;
    private CorfuRuntime rt;
    private final int MAX_BATCH_SIZE = 1000;
    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;
    private LogReplicationContext context;
    private long preMsgTs;
    private long currentMsgTs;
    private long lastReadAddress;

    void initStream() {
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();
        stream = rt.getStreamsView()
                .getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, options);
    }

    public LogEntryReader(LogReplicationContext context) {
        initStream();
        this.context = context;
    }

    //poll txnStream
    List<ILogData> poll(long start) {
        stream.seek(start);
        return stream.remaining();
    }

    TxMessage generateMessage(List<ILogData> entries) {
        currentMsgTs = entries.get(entries.size() - 1).getGlobalAddress();
        TxMessage txMessage = new TxMessage(MSG_TYPE, currentMsgTs, preMsgTs, globalBaseSnapshot);
        //set data with Maithem's new api
        preMsgTs = currentMsgTs;
        return  txMessage;
    }

    List<TxMessage> nextMsgs(List inputs) {
        ArrayList entries = new ArrayList(inputs);
        List<TxMessage> msgList = new ArrayList<>();

        if (entries.isEmpty()) {
            return msgList;
        }

        for (int i = 0; i < entries.size(); i += MAX_BATCH_SIZE) {
            List<ILogData> msg_entries = entries.subList(i, i + MAX_BATCH_SIZE);
            TxMessage txMsg = generateMessage(entries);
            msgList.add(txMsg);
        }

        return msgList;
    }

    public void resetGlobalBaseSnapshot(long snapshot) {
        globalBaseSnapshot = snapshot;
        lastReadAddress = snapshot;
        preMsgTs = snapshot;
    }

    public void sync() {
        try {
            List entries = poll(lastReadAddress + 1);
            List<TxMessage> msgs = nextMsgs(entries);

            // call callback to process message

            // only update lastReadAddress after successful process message
            lastReadAddress = preMsgTs;
        } catch (Exception e) {

            log.warn("Sync caught an exception ", e);
            throw(e);
        }
    }
}
