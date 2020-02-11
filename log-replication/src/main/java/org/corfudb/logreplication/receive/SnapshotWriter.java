package org.corfudb.logreplication.receive;

import org.corfudb.logreplication.message.DataMessage;

import java.util.List;

/**
 * The snapshot full sync engine will call the snapshot writer api apply to apply messages it has received.
 * It will guarantee the ordering of the message that pass to the snapshot writer.
 */
public interface SnapshotWriter {
    // The snapshot full sync engine will pass a message to the snapshot writer
    void apply(DataMessage message) throws Exception;

    void apply(List<DataMessage> messages) throws Exception;
}
