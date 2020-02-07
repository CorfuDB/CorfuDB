package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.transmitter.TxMessage;

import java.util.List;

/**
 * The snapshot full sync engine will call the snapshot writer api apply to apply messages it has received.
 * It will guarantee the ordering of the message that pass to the snapshot writer.
 */
public interface SnapshotWriter {
    // The snapshot full sync engine will pass a message to the snapshot writer
    void apply(TxMessage message) throws Exception;

    // The snapshot full sync engine will pass a list of message to the snapshot writer
    void apply(List<TxMessage> messages) throws Exception;
}
