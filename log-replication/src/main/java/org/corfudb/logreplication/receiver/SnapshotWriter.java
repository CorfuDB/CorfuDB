package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.transmitter.TxMessage;

import java.util.List;

public interface SnapshotWriter {

    void apply(TxMessage message);

    void apply(List<TxMessage> messages);
}
