package org.corfudb.integration;

import lombok.Data;
import lombok.Setter;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.send.LogReplicationError;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

@Data
public class AckDataSender implements DataSender {

    private UUID snapshotSyncRequestId;
    private long baseSnapshotTimestamp;
    private SourceManager sourceManager;

    public AckDataSender() {
    }

    /*
     * ------------ SNAPSHOT SYNC METHODS --------------
     */
    @Override
    public boolean send(DataMessage message, UUID snapshotSyncId, boolean completed) {
        // Ack received from Log Replication Process for snapshot sync, to be sent to source site.
        if (completed) {
            assertThat(message.getMetadata().getMessageMetadataType()).isEqualTo(MessageType.SNAPSHOT_REPLICATED);
//            assertThat(message.getMetadata().getSnapshotRequestId()).isEqualTo(snapshotSyncRequestId);
//            assertThat(message.getMetadata().getSnapshotTimestamp()).isEqualTo(baseSnapshotTimestamp);

            // Emulate it was sent over the wire and arrived on the source side
            Executors.newSingleThreadExecutor().execute(() -> sourceManager.receive(message));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean send(List<DataMessage> messages, UUID snapshotSyncId, boolean completed) {
        messages.forEach(msg -> send(msg));
        return true;
    }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {
        fail("On Error received for snapshot entry sync");
    }

    /*
     * ------------ LOG ENTRY SYNC METHODS --------------
     */
    @Override
    public boolean send(DataMessage message) {
        // Emulate it was sent over the wire and arrived on the source side
        Executors.newSingleThreadExecutor().execute(() -> sourceManager.receive(message));
        return true;
    }

    @Override
    public boolean send(List<DataMessage> messages) {
        messages.forEach(msg -> send(msg));
        return true;
    }

    @Override
    public void onError(LogReplicationError error) {
        fail("On Error received for log entry sync");
    }
}
