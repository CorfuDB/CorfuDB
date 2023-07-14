package org.corfudb.infrastructure.logreplication.config;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.view.TableRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.DEMO_NAMESPACE;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX;

/**
 * This class represents the Log Replication Configuration field(s) for ROUTING_QUEUES replication model.
 */
public class LogReplicationRoutingQueueConfig extends LogReplicationConfig {

    /**
     * Destination specific stream tag for snapshot sync.
     */
    @Getter
    private final String snapshotSyncStreamTag;

    /**
     * Destination specific stream tag for log entry sync.
     */
    @Getter
    private final String logEntrySyncStreamTag;

    /**
     * Name of the queue that will have replicated data on Sink side.
     */
    @Getter
    private final String sinkQueueName;

    /**
     * Stream id of the queue that will have replicated data on Sink side.
     */
    @Getter
    private final UUID sinkQueueStreamId;

    /**
     * Stream tag applied to the replicated queue on the Sink side.
     */
    @Getter
    private final UUID sinkQueueStreamTag;


    public LogReplicationRoutingQueueConfig(@NonNull LogReplication.LogReplicationSession session,
                                            ServerContext serverContext) {
        super(session, new HashSet<>(), new HashMap<>(), serverContext);
        this.snapshotSyncStreamTag = SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();
        this.logEntrySyncStreamTag = LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();
        this.sinkQueueName = TableRegistry.getFullyQualifiedTableName(DEMO_NAMESPACE, REPLICATED_QUEUE_NAME);
        this.sinkQueueStreamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(DEMO_NAMESPACE,
                REPLICATED_QUEUE_NAME));
        this.sinkQueueStreamTag = TableRegistry.getStreamIdForStreamTag(DEMO_NAMESPACE, REPLICATED_QUEUE_TAG);
        getStreamsToReplicate().add(snapshotSyncStreamTag);
        getDataStreamToTagsMap().put(sinkQueueStreamId, Collections.singletonList(sinkQueueStreamTag));
    }
}
