package org.corfudb.logreplication.transmitter;

import com.google.common.reflect.TypeToken;
import lombok.Data;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

@Data
public class PersistedReaderMetadata {
    private final String TABLE_PREFIX_NAME = "READER-";
    private final String LAST_SENT_SNAP_TS = "lastSentBaseSnapshotTimeStamp";
    private final String LAST_ACK_SNAP_TS = "lastAckedTimeStamp";

    private long lastSentBaseSnapshotTimestamp; //used by fullsync transmitter
    private long lastAckedTimestamp; //used by fullsync transmitter

    //private long lastProcessedSrcTimestamp; // Receiver will update this value

    private CorfuTable<String, Long> readerMetaDataTable;

    public PersistedReaderMetadata(CorfuRuntime rt, UUID src, UUID dst) {
        readerMetaDataTable = rt.getObjectsView()
                .build()
                .setStreamName(TABLE_PREFIX_NAME + src.toString() + "-" + dst.toString())
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                })
                .setSerializer(Serializers.JSON)
                .open();
    }

    public void setLastSentBaseSnapshotTimestamp(long ts) {
        readerMetaDataTable.put(LAST_SENT_SNAP_TS,ts);
        lastSentBaseSnapshotTimestamp = ts;
    }

    public void setLastAckedTimestamp(long ts) {
        readerMetaDataTable.put(LAST_ACK_SNAP_TS,ts);
        lastAckedTimestamp = ts;
    }
}
