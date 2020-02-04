package org.corfudb.logreplication.transmitter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.List;

@Slf4j
public class LogEntryReader {
    private IStreamView stream;
    private long globalBaseSnapshot;
    private CorfuRuntime rt;
    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;
    private LogReplicationContext context;
    private long preMsgTs;
    private long currentMsgTs;

    void initStream() {
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();
        stream = rt.getStreamsView()
                .getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, options);
    }

    public LogEntryReader(LogReplicationContext context) {
        //initStream();
        this.context = context;
    }

    //poll txnStream
    List<ILogData> poll(long start) {
        stream.seek(start);
        return stream.remaining();
    }

    TxMessage generateMessage(ILogData entry) {
        currentMsgTs = entry.getGlobalAddress();
        TxMessage txMessage = new TxMessage(MSG_TYPE, currentMsgTs, preMsgTs, globalBaseSnapshot);
        //set data with Maithem's new api
        preMsgTs = currentMsgTs;
        return  txMessage;
    }

    void nextMsgs(List<ILogData> inputs) {
        for (ILogData entry : inputs) {
            context.getSnapshotListener().onNext(generateMessage(entry));
        }
    }

    public void resetGlobalBaseSnapshot(long snapshot) {
        globalBaseSnapshot = snapshot;
        preMsgTs = snapshot;
    }

    public void sync() {
        try {
            nextMsgs(poll(preMsgTs + 1));
        } catch (Exception e) {
            log.warn("Sync caught an exception ", e);
            throw(e);
        }
    }
}
