package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class DefaultLogReplicationConfigAdapterForUpgrade implements ILogReplicationConfigAdapter {

    private Set<String> streamsToReplicate = new HashSet<>();
    private static final String STREAMS_TEST_TABLE =
        "StreamsToReplicateTestTable";

    public static final int MAP_COUNT = 5;
    public static final String TABLE_PREFIX = "Table00";
    public static final String NAMESPACE = "LR-Test";
    public static final String TAG_ONE = "tag_one";
    private final int indexOne = 1;
    private final int indexTwo = 2;
    private CorfuStore corfuStore;

    public DefaultLogReplicationConfigAdapterForUpgrade() {
        if (corfuStore != null) {
            List<CorfuStoreEntry<LogReplicationStreams.TableInfo,
                LogReplicationStreams.Namespace, CommonTypes.Uuid>> records;
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                records = txn.executeQuery(STREAMS_TEST_TABLE, p -> true);
                txn.commit();
            }
            records.forEach(record -> streamsToReplicate.add(record.getKey().getName()));
        } else {
            for (int i = 1; i <= MAP_COUNT; i++) {
                streamsToReplicate.add(NAMESPACE + "$" + TABLE_PREFIX + i);
            }
        }
    }

    // Provides the fully qualified names of streams to replicate
    @Override
    public Set<String> fetchStreamsToReplicate() {
        return streamsToReplicate;
    }

    @Override
    public String getVersion() {
        return "version_latest";
    }

    @Override
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        Map<UUID, List<UUID>> streamsToTagsMaps = new HashMap<>();
        UUID streamTagOneDefaultId = TableRegistry.getStreamIdForStreamTag(NAMESPACE, TAG_ONE);
        streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + "$" + TABLE_PREFIX + indexOne),
            Collections.singletonList(streamTagOneDefaultId));
        streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + "$" + TABLE_PREFIX + indexTwo),
            Collections.singletonList(streamTagOneDefaultId));
        return streamsToTagsMaps;
    }

    @Override
    public void setCorfuStore(CorfuStore corfuStore) {
        this.corfuStore = corfuStore;
    }
}
