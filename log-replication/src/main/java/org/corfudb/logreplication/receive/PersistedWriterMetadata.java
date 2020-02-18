package org.corfudb.logreplication.receive;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;

import java.util.Map;
import java.util.UUID;

/**
 * The table persisted at the replication writer side.
 * It records the reader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
public class PersistedWriterMetadata {
    private final String TABLE_PREFIX_NAME = "WRITER-";

    @Getter
    private long lastSrcBaseSnapshotTimestamp;

    @Getter
    private long lastProcessedLogTimestamp;

    private Map<String, Long> writerMetaDataTable;

    public PersistedWriterMetadata(CorfuRuntime rt, UUID dst) {
        writerMetaDataTable = rt.getObjectsView()
                .build()
                .setStreamName(TABLE_PREFIX_NAME + dst.toString())
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();

        lastSrcBaseSnapshotTimestamp = writerMetaDataTable.getOrDefault(PersistedWriterMetadataType.LastSnapDone, Address.NON_ADDRESS);
        lastProcessedLogTimestamp = writerMetaDataTable.getOrDefault(PersistedWriterMetadataType.LastLogProcessed, Address.NON_ADDRESS);
    }

    public void setsrcBaseSnapshotStart(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), ts);
    }

    public void setsrcBaseSnapshotDone() {
        long ts = writerMetaDataTable.getOrDefault(PersistedWriterMetadataType.LastSnapStart, Address.NON_EXIST);
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
