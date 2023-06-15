package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.SubscribeMaxRetryException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * LogReplication code resides in the infrastructure package.  Adding a dependency from this package(runtime) to
 * infrastructure introduces a circular dependency.  This class defines LR-specific constants and utility methods
 * required in runtime.  Note that the constants are unique and not duplicated from infrastructure.
 */
@Slf4j
public final class LogReplicationUtils {
    public static final String LR_STATUS_STREAM_TAG = "lr_status";

    // Retain the old name from LR v1 to avoid polluting the registry table with stale entries.
    public static final String REPLICATION_STATUS_TABLE_NAME = "LogReplicationStatus";

    // /-------Constants specific to the RoutingQueue Model --------/

    // Name of shared queue on the sender where log entry sync updates are placed transactionally
    public static final String LOG_ENTRY_SYNC_QUEUE_NAME_SENDER = "LRQ_Send_LogEntries";

    // Prefix of destination specific stream tag applied to entries on LOG_ENTRY_SYNC_QUEUE_NAME_SENDER
    public static final String LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX = "lrq_logentry_";

    // Name of shared queue on the sender where snapshot sync updates are placed
    public static final String SNAPSHOT_SYNC_QUEUE_NAME_SENDER = "LRQ_Send_SnapSync";

    // Prefix of destination specific stream tag applied to entries on SNAPSHOT_SYNC_QUEUE_NAME_SENDER
    public static final String SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX = "lrq_snapsync_";

    // Prefix of the name of queue as it will appear on the receiver after replicated.  The suffix will be the Sender
    // (Source) cluster id
    public static final String REPLICATED_QUEUE_NAME_PREFIX = "LRQ_Recv_";

    // Stream tag applied to the replicated queue on the receiver
    public static final String REPLICATED_QUEUE_TAG = "lrq_recv";

    // ---- End RoutingQueue Model constants -------/

    public static final UUID lrLogEntrySendQId = CorfuRuntime.getStreamID(TableRegistry
            .getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER));
    public static final UUID lrFullSyncSendQId = CorfuRuntime.getStreamID(TableRegistry
            .getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_SYNC_QUEUE_NAME_SENDER));

    public static boolean skipCheckpointFor(UUID streamId) {
        return streamId.equals(lrLogEntrySendQId) || streamId.equals(lrFullSyncSendQId);
    }

    private LogReplicationUtils() { }

    public static void subscribe(@Nonnull LogReplicationListener listener, @Nonnull String namespace,
                                 @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                 int bufferSize, CorfuStore corfuStore) {
        log.info("Client subscription process has started.");

        Map<String, List<String>> nsToTableName = new HashMap<>();
        nsToTableName.put(namespace, tablesOfInterest);
        nsToTableName.put(CORFU_SYSTEM_NAMESPACE, Arrays.asList(REPLICATION_STATUS_TABLE_NAME));

        Map<String, String> nsToStreamTags = new HashMap<>();
        nsToStreamTags.put(namespace, streamTag);
        nsToStreamTags.put(CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Perform subscription validation upfront since actual subscribe happens async.
        validateSubscription(listener, corfuStore, nsToTableName, nsToStreamTags, bufferSize);

        listener.getFullSyncExecutorService().submit(new ClientFullSyncAndSubscriptionTask(listener,
                corfuStore, namespace, nsToStreamTags, nsToTableName, bufferSize));
    }

    private static void validateSubscription(LogReplicationListener listener, CorfuStore corfuStore,
                                             Map<String, List<String>> nsToTableName,
                                             Map<String, String> nsToStreamTags, int bufferSize) {
        TableRegistry registry = corfuStore.getRuntime().getTableRegistry();

        // The namespaces in both maps should be the same
        Preconditions.checkState(Objects.equals(nsToStreamTags.keySet(), nsToTableName.keySet()));

        // Validate the buffer size.
        Preconditions.checkState(registry.getStreamingManager().validateBufferSize(bufferSize));

        if (registry.getStreamingManager().isListenerSubscribed(listener)) {
            // Multiple subscribers subscribing to same namespace and table is allowed
            // as long as the hashcode() and equals() method of the listeners are different.
            throw new StreamingException("StreamingManager::subscribe: listener already registered "
                    + listener, StreamingException.ExceptionCause.LISTENER_SUBSCRIBED);
        }

        for (Map.Entry<String, List<String>> nsToTableNamesEntry : nsToTableName.entrySet()) {
            for (String tableName : nsToTableNamesEntry.getValue()) {
                Table<Message, Message, Message> table;
                try {
                    table = registry.getTable(nsToTableNamesEntry.getKey(), tableName);
                } catch (IllegalArgumentException e) {
                    // The table was not opened using the client's runtime
                    log.error("Replicated Table {} was not opened using the client runtime.  Please open the table " +
                            "before subscribing", nsToTableNamesEntry.getKey(), tableName);
                    throw new StreamingException(String.format("Please open the replicated table [%s:%s] using the " +
                            "client runtime.", nsToTableNamesEntry.getKey(), tableName),
                            StreamingException.ExceptionCause.SUBSCRIBE_ERROR);
                }
                String tagToValidate = nsToStreamTags.get(nsToTableNamesEntry.getKey());
                UUID streamTagId = TableRegistry.getStreamIdForStreamTag(nsToTableNamesEntry.getKey(), tagToValidate);
                if (!table.getStreamTags().contains(streamTagId)) {
                    throw new IllegalArgumentException(String.format("Interested table: %s does not " +
                            "have specified stream tag: %s", table.getFullyQualifiedTableName(), tagToValidate));
                }
            }
        }
    }

    private static class ClientFullSyncAndSubscriptionTask implements Callable<Void> {

        private LogReplicationListener listener;
        private CorfuStore corfuStore;
        private String namespace;
        private Map<String, String> nsToStreamTags;
        private Map<String, List<String>> nsToTableName;
        private int bufferSize;
        private static final Duration retryThreshold = Duration.ofMinutes(2);

        // Settings for the subscription retries
        Consumer<ExponentialBackoffRetry> retrySettings = settings -> {
            settings.setMaxRetryThreshold(retryThreshold);
        };

        ClientFullSyncAndSubscriptionTask(LogReplicationListener listener, CorfuStore corfuStore,
                                          String namespace, Map<String, String> nsToStreamTags,
                                          Map<String, List<String>> nsToTableName, int bufferSize) {
            this.listener = listener;
            this.corfuStore = corfuStore;
            this.namespace = namespace;
            this.nsToStreamTags = nsToStreamTags;
            this.nsToTableName = nsToTableName;
            this.bufferSize = bufferSize;
        }

        @Override
        public Void call() {
            Optional<Counter> mvoTrimCounter = MicroMeterUtils.counter("logreplication.subscribe.trim.count");
            Optional<Counter> conflictCounter = MicroMeterUtils.counter("logreplication.subscribe.conflict.count");
            Optional<Timer.Sample> subscribeTimer = MicroMeterUtils.startTimer();

            Table<LogReplicationSession, ReplicationStatus, Message> replicationStatusTable =
                    openReplicationStatusTable(corfuStore);
           Long fullSyncTimestamp = null;
            try {
                fullSyncTimestamp = IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

                    try (TxnContext txnContext = corfuStore.txn(namespace)) {
                        long completionTimestamp;
                        // The transaction is started in the client's namespace and the Replication Status table resides in the
                        // system namespace.  Corfu Store does not validate the cross-namespace access as long as there are no
                        // writes on the table in the different namespace.  This hack is required here as we want client full
                        // sync to happen in the same transaction which checks the status of a snapshot sync so that there is no
                        // window between the check and full sync.
                        List<CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message>> entries =
                                txnContext.executeQuery(replicationStatusTable,
                                        entry -> entry.getKey().getSubscriber().getModel()
                                                .equals(LogReplication.ReplicationModel.LOGICAL_GROUPS) &&
                                                Objects.equals(entry.getKey().getSubscriber().getClientName(),
                                                        listener.getClientName()));

                        CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry = null;

                        // It is possible that there is no entry for the Logical Group Model in the status table in
                        // the following scenarios:
                        // 1. Request to subscribe is received before the Log Replication JVM or pod starts and
                        // initializes the table
                        // 2. Topology change which removes this cluster from the status table.
                        // We cannot differentiate between the two cases here so continue with the right
                        // behavior for the 1st case, i.e., subscribe and wait for the initial Snapshot Sync to get
                        // triggered.  If we are here because of the second case, the listener will not get any updates
                        // from LR and subscription will be a no-op.
                        if (entries.isEmpty()) {
                            log.warn("No record for client {} and Logical Group Model found in the Status Table.  " +
                                    "Subscription could have been attempted before LR startup.  Subscribe the listener and" +
                                    "wait for initial Snapshot Sync", listener.getClientName());
                        } else {
                            // For a given replication model and client, any Sink node will have a single session.
                            Preconditions.checkState(entries.size() == 1);
                            entry = entries.get(0);
                        }

                        if (entry == null || entry.getPayload().getSinkStatus().getDataConsistent()) {
                            // No snapshot sync is in progress
                            log.info("No Snapshot Sync is in progress.  Request the client to perform a full sync on its " +
                                    "tables.");
                            Optional<Timer.Sample> clientFullSyncTimer = MicroMeterUtils.startTimer();
                            long clientFullSyncStartTime = System.currentTimeMillis();
                            listener.performFullSyncAndMerge(txnContext);
                            long clientFullSyncEndTime = System.currentTimeMillis();
                            MicroMeterUtils.time(clientFullSyncTimer, "logreplication.client.fullsync.duration");
                            completionTimestamp = txnContext.getTxnSequence();
                            log.info("Client Full Sync and Merge took {} ms, completed at {}",
                                    clientFullSyncEndTime - clientFullSyncStartTime, txnContext.commit());
                        } else {
                            // Snapshot sync is in progress.  Retry the operation
                              log.info("Snapshot Sync is in progress.  Retrying the operation");
                            throw new RetryNeededException();
                        }

                        return completionTimestamp;
                    } catch (TransactionAbortedException tae) {
                        if (tae.getCause() instanceof TrimmedException) {
                            // If the snapshot version where this transaction started has been evicted from the JVM's MVO
                            // cache, a trimmed exception is thrown and requires a retry at a later timestamp.
                            incrementCount(mvoTrimCounter);
                            log.warn("Snapshot no longer available in the cache.  Retrying.", tae);
                        } else if (tae.getAbortCause() == AbortCause.CONFLICT) {
                            // Concurrent updates to the client's tables
                            incrementCount(conflictCounter);
                            log.warn("Concurrent updates to client tables.  Retrying.", tae);
                        } else {
                            log.error("Unexpected type of Transaction Aborted Exception", tae);
                        }
                        throw new RetryNeededException();
                    } catch (Exception e) {
                        log.error("Unexpected exception type hit", e);
                        throw new RetryNeededException();
                    } finally {
                        MicroMeterUtils.time(subscribeTimer, "logreplication.subscribe.duration");
                    }
                }).setOptions(retrySettings).run();
            } catch (InterruptedException e) {
                log.warn("Subscription has failed due to subscriber error, must be retried.");
                listener.onError(new StreamingException(e, StreamingException.ExceptionCause.SUBSCRIBE_ERROR));
            } catch (RetryExhaustedException re) {
                log.warn("Subscription has failed due to exceeding retry limit, must be retried.");
                listener.onError(new SubscribeMaxRetryException());
            }

            if (!Objects.isNull(fullSyncTimestamp)) {
                // Subscribe from the snapshot timestamp of this transaction, i.e., log tail when the transaction started.
                // Subscribing from the commit address will result in missed updates which took place between the start
                // and end of the transaction because reads in a transaction observe updates only till the snapshot when it
                // started.
                corfuStore.getRuntime().getTableRegistry().getStreamingManager().subscribeLogReplicationListener(
                        listener, nsToTableName, nsToStreamTags, fullSyncTimestamp, bufferSize);
            }
            return null;
        }

        private Table<LogReplicationSession, ReplicationStatus, Message> openReplicationStatusTable(CorfuStore corfuStore) {
            try {
                return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE_NAME,
                        LogReplicationSession.class, ReplicationStatus.class, null,
                        TableOptions.fromProtoSchema(ReplicationStatus.class));
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                log.error("Failed to open the Replication Status table", e);
                throw new StreamingException(e, StreamingException.ExceptionCause.SUBSCRIBE_ERROR);
            }
        }

        private void incrementCount(Optional<Counter> counter) {
            counter.ifPresent(Counter::increment);
        }
    }
}
