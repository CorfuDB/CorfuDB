package org.corfudb.logreplication.receive;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

/**
 * The table persisted at the replication writer side.
 * It records the reader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
public class PersistedWriterMetadata {
    private final String TABLE_PREFIX_NAME = "WRITER-";
    private final String LAST_SNAPSHOT_TS_START = "lastSrcBaseSnapshotTimestamp_start";
    private final String LAST_SNAPSHOT_TS_DONE = "lastSrcBaseSnapshotTimestamp_done";
    private final String LAST_PROCESSED_LOG_TS = "lastProcessedLogTimestamp";

    @Getter
    private long lastSrcBaseSnapshotTimestamp;

    @Getter
    private long lastProcessedLogTimestamp;

    private CorfuTable<String, Long> writerMetaDataTable;

    public PersistedWriterMetadata(CorfuRuntime rt, UUID dst) {
        writerMetaDataTable = rt.getObjectsView()
                .build()
                .setStreamName(TABLE_PREFIX_NAME + dst.toString())
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                })
                .setSerializer(Serializers.JSON)
                .open();
        lastSrcBaseSnapshotTimestamp = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapDone);
        lastProcessedLogTimestamp = writerMetaDataTable.get(PersistedWriterMetadataType.LastLogProcessed);
    }

    public void setsrcBaseSnapshotStart(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), ts);
    }

    public void setsrcBaseSnapshotDone() {
        long ts = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart);
        writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapDone.getVal(), ts);
        lastSrcBaseSnapshotTimestamp = ts;
        lastProcessedLogTimestamp = ts;
    }

    public void setLastProcessedLogTimestamp(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastLogProcessed.getVal(), ts);
        lastProcessedLogTimestamp = ts;
    }

    enum PersistedWriterMetadataType {
        LastSnapStart("lastSnapStart"),
        LastSnapDone("lastSnapDone"),
        LastLogProcessed("lastLogProcessed");

        @Getter
        String val;
        PersistedWriterMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
