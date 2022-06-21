package org.corfudb.runtime.collections.streaming;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.view.TableRegistry;
import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class LRStreamingTask<K extends Message, V extends Message, M extends Message> extends StreamingTask {

    private static final UUID LR_MULTI_NAMESPACE_STREAM_ID = UUID.randomUUID();


    public LRStreamingTask(CorfuRuntime runtime, ExecutorService workerPool,
                           @Nonnull Map<String, String> nsToStreamTags, @Nonnull Map<String, List<String>> nsToTables,
                           StreamListener listener,
                           long address,
                           int bufferSize) {
        super(runtime, workerPool, listener, String.format("listener_%s_%s", listener, LR_MULTI_NAMESPACE_STREAM_ID));

        TableRegistry registry = runtime.getTableRegistry();

        Set<UUID> streamsTracked = new HashSet<>();
        nsToStreamTags.entrySet().stream().forEach(nsToTag -> {
            final UUID streamId = TableRegistry.getStreamIdForStreamTag(nsToTag.getKey(), nsToTag.getValue());
            streamsTracked.add(streamId);
        });

        this.stream = new LRDeltaStream(runtime.getAddressSpaceView(), LR_MULTI_NAMESPACE_STREAM_ID, address,
                bufferSize, streamsTracked);

        for (Map.Entry<String, List<String>> nsToTablesEntry : nsToTables.entrySet()) {
            for (String table : nsToTablesEntry.getValue()) {
                UUID streamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(
                        nsToTablesEntry.getKey(), table));
                Table<K, V, M> t = registry.getTable(nsToTablesEntry.getKey(), table);
                String streamTag = nsToStreamTags.get(nsToTablesEntry.getKey());
                UUID streamTagId = TableRegistry.getStreamIdForStreamTag(nsToTablesEntry.getKey(), streamTag);
                if (!t.getStreamTags().contains(streamTagId)) {
                    throw new IllegalArgumentException(String.format("Interested table: %s does not " +
                        "have specified stream tag: %s", t.getFullyQualifiedTableName(), streamTag));
                }
                tableSchemas.put(streamId, new TableSchema<>(table, t.getKeyClass(),
                    t.getValueClass(), t.getMetadataClass()));
            }
        }
        status.set(StreamStatus.RUNNABLE);
    }
}
