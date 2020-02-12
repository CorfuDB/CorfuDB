package org.corfudb.logreplication.transmit;

import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

/**
 * The persistent table at the corfu reader cluster.
 * The table is used to record the snapshot timestamp and last ack
 * has received from the destination corfu writer cluster.
 * As one corfu reader cluster can have multiple destination corfu writer cluster,
 * Using the writer corfu cluster id to uniquely identify the table.
 *
 * Todo:
 * If this table is not registered at checkpoint compactor, this information can be trimmed.
 */
@Data
public class PersistedReaderMetadata {
    private final String TABLE_PREFIX_NAME = "READER-FOR-";
    //private final String LAST_SENT_SNAP_TS = "lastSentBaseSnapshotTimeStamp";
    //private final String LAST_ACK_SNAP_TS = "lastAckedTimeStamp";

    private long lastSentBaseSnapshotTimestamp; //used by fullsync transmit
    private long lastAckedTimestamp; //used by fullsync transmit

    private CorfuTable<String, Long> readerMetaDataTable;

    /**
     * Open an corfu table to record the lastSentBaseSnapshotTs and
     * lastAckedTs.
     * @param rt
     * @param dst the corfu writer cluster id.
     */
    public PersistedReaderMetadata(CorfuRuntime rt, UUID dst) {
        readerMetaDataTable = rt.getObjectsView()
                .build()
                .setStreamName(TABLE_PREFIX_NAME + dst.toString())
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                })
                .setSerializer(Serializers.JSON)
                .open();
    }

    /**
     * Set the snapshot fullsync timestamp when a snapshot read is done.
     * @param ts
     */
    public void setLastSentBaseSnapshotTimestamp(long ts) {
        readerMetaDataTable.put(PersistedMetaDatayType.LastSnapSync.getVal(), ts);
        lastSentBaseSnapshotTimestamp = ts;
    }

    /**
     * Record the ack from the writer, this information record the timestamp that
     * the writer has synced up to.
     * @param ts
     */
    public void setLastAckedTimestamp(long ts) {
        readerMetaDataTable.put(PersistedMetaDatayType.LastLogSync.getVal(), ts);
        lastAckedTimestamp = ts;
    }

    private enum PersistedMetaDatayType {
        LastSnapSync ("lastSnapSync"),
        LastLogSync ("lastLogSync");

        @Getter
        String val;
        PersistedMetaDatayType(String newVal) {
            val = newVal;
        }
    }
}
