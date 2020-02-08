package org.corfudb.logreplication.receiver;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

public class PersistedWriterMetadata {
    private final String TABLE_PREFIX_NAME = "WRITER-";
    private final String LAST_SNAPSHOT_TS = "lastSrcBaseSnapshotTimestamp";
    private final String LAST_PROCESSED_LOG_TS = "lastProcessedLogTimestamp";

    private long lastSrcBaseSnapshotTimestamp;
    private long lastProcessedLogTimestamp;


    private CorfuTable<String, Long> writerMetaDataTable;

    public PersistedWriterMetadata(CorfuRuntime rt, UUID src, UUID dst) {
        writerMetaDataTable = rt.getObjectsView()
                .build()
                .setStreamName(TABLE_PREFIX_NAME + src.toString() + "-" + dst.toString())
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                })
                .setSerializer(Serializers.JSON)
                .open();
    }

    public void setsrcBaseSnapshotTimestamp(long ts) {
        writerMetaDataTable.put(LAST_SNAPSHOT_TS, ts);
        lastSrcBaseSnapshotTimestamp = ts;
    }

    public void setLastProcessedLogTimestamp(long ts) {
        writerMetaDataTable.put(LAST_PROCESSED_LOG_TS, ts);
        lastProcessedLogTimestamp = ts;
    }
}
