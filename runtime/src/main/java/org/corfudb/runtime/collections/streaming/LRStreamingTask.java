package org.corfudb.runtime.collections.streaming;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.view.TableRegistry;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;


/**
 * This class is an extension of {@link StreamingTask}.  It tracks {@link LRDeltaStream} which is a composite stream
 * tracking multiple streams(stream tags) in a single ordered buffer.  All other behavior is the same as its super
 * class {@link StreamingTask}.
 * @param <K>
 * @param <V>
 * @param <M>
 */
@Slf4j
public class LRStreamingTask<K extends Message, V extends Message, M extends Message> extends StreamingTask {

    private static final String LR_MULTI_NAMESPACE_LOGICAL_STREAM = "LR_MultiNamespace_Logical_Stream";
    public static final UUID LR_MULTI_NAMESPACE_LOGICAL_STREAM_ID =
            UUID.nameUUIDFromBytes(LR_MULTI_NAMESPACE_LOGICAL_STREAM.getBytes());

    public LRStreamingTask(CorfuRuntime runtime, ExecutorService workerPool, @Nonnull Map<String, String> nsToStreamTag,
                           @Nonnull Map<String, List<String>> nsToTableNames, StreamListener listener, long address,
                           int bufferSize) {
        super(runtime, workerPool, listener, String.format("listener_%s_%s", listener,
                LR_MULTI_NAMESPACE_LOGICAL_STREAM_ID));

        // The namespaces in both maps should be the same
        Preconditions.checkState(Objects.equals(nsToStreamTag.keySet(), nsToTableNames.keySet()));

        TableRegistry registry = runtime.getTableRegistry();

        List<UUID> streamsTracked = new ArrayList<>();
        nsToStreamTag.entrySet().stream().forEach(nsToTag -> {
            final UUID streamId = TableRegistry.getStreamIdForStreamTag(nsToTag.getKey(), nsToTag.getValue());
            streamsTracked.add(streamId);
        });

        this.stream = new LRDeltaStream(runtime.getAddressSpaceView(), LR_MULTI_NAMESPACE_LOGICAL_STREAM_ID, address,
                bufferSize, streamsTracked);

        for (Map.Entry<String, List<String>> nsToTableNamesEntry : nsToTableNames.entrySet()) {
            for (String tableName : nsToTableNamesEntry.getValue()) {
                UUID streamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(
                        nsToTableNamesEntry.getKey(), tableName));
                Table<K, V, M> table;
                try {
                    table = registry.getTable(nsToTableNamesEntry.getKey(), tableName);
                } catch (IllegalArgumentException e) {
                    // The table was not opened using the client's runtime
                    log.error("Replicated Table {} was not opened using the client runtime.  Please open the table " +
                            "before subscribing", nsToTableNamesEntry.getKey(), tableName);
                    throw new StreamingException(String.format("Please open the replicated table [%s:%s] using the " +
                            "client runtime.", nsToTableNamesEntry.getKey(), tableName));
                }
                String streamTag = nsToStreamTag.get(nsToTableNamesEntry.getKey());
                UUID streamTagId = TableRegistry.getStreamIdForStreamTag(nsToTableNamesEntry.getKey(), streamTag);
                if (!table.getStreamTags().contains(streamTagId)) {
                    throw new IllegalArgumentException(String.format("Interested table: %s does not " +
                        "have specified stream tag: %s", table.getFullyQualifiedTableName(), streamTag));
                }
                tableSchemas.put(streamId, new TableSchema<>(tableName, table.getKeyClass(), table.getValueClass(),
                        table.getMetadataClass()));
            }
        }
        status.set(StreamStatus.RUNNABLE);
    }
}
