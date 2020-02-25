package org.corfudb.logreplication.send;

import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Address;
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
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-READER-";

    private long lastSentBaseSnapshotTimestamp = Address.NON_ADDRESS; //used by fullsync send
    private long lastAckedTimestamp = Address.NON_ADDRESS; //used by fullsync send

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
                .setStreamName(getPersistedReaderMetadataTableName(dst))
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {})
                .setSerializer(Serializers.JSON)
                .open();
        if (readerMetaDataTable.isEmpty()) {
            setLastSentBaseSnapshotTimestamp(Address.NON_ADDRESS);
            setLastAckedTimestamp(Address.NON_ADDRESS);
        }
    }

    public static String getPersistedReaderMetadataTableName(UUID uuid) {
        return TABLE_PREFIX_NAME + uuid.toString();
    }

    /**
     * Set the snapshot sync timestamp when a snapshot read is done.
     * @param ts
     */
    public void setLastSentBaseSnapshotTimestamp(long ts) {
        readerMetaDataTable.put(PersistedMetaDataType.LastSnapSync.getVal(), ts);
        lastSentBaseSnapshotTimestamp = ts;
    }

    /**
     * Record the ack from the writer, this information record the timestamp that
     * the writer has synced up to.
     * @param ts
     */
    public void setLastAckedTimestamp(long ts) {
        readerMetaDataTable.put(PersistedMetaDataType.LastLogSync.getVal(), ts);
        lastAckedTimestamp = ts;
    }

    public enum PersistedMetaDataType {
        LastSnapSync ("lastSnapSync"),
        LastLogSync ("lastLogSync");

        @Getter
        String val;
        PersistedMetaDataType(String newVal) {
            val = newVal;
        }
    }
}
