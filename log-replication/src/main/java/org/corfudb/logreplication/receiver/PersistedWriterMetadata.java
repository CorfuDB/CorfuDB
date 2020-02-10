package org.corfudb.logreplication.receiver;

import com.google.common.reflect.TypeToken;
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

    private long lastSrcBaseSnapshotTimestamp;
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
    }

    public void setsrcBaseSnapshotStart(long ts) {
        writerMetaDataTable.put(LAST_SNAPSHOT_TS_START, ts);
        lastSrcBaseSnapshotTimestamp = ts;
    }

    public void setsrcBaseSnapshotDone() {
        writerMetaDataTable.put(LAST_SNAPSHOT_TS_DONE, lastSrcBaseSnapshotTimestamp);
    }

    public void setLastProcessedLogTimestamp(long ts) {
        writerMetaDataTable.put(LAST_PROCESSED_LOG_TS, ts);
        lastProcessedLogTimestamp = ts;
    }
}
