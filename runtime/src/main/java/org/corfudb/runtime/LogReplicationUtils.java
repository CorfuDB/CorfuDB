package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.IsolationLevel;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.LogReplication.ReplicationStatusKey;
import org.corfudb.runtime.LogReplication.ReplicationStatusVal;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;
import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * LogReplication code resides in the infrastructure package.  Adding a dependency from this package(runtime) to
 * infrastructure introduces a circular dependency.  This class defines LR-specific constants and utility methods required in
 * runtime.  Note that these methods are unique and not duplicated from infrastructure.
 */
@Slf4j
public class LogReplicationUtils {
    public static final String LR_STATUS_STREAM_TAG = "lr_status";

    public static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    private CorfuStore corfuStore;

    private ScheduledExecutorService schedulerThread;

    private static final int DEFAULT_BUFFER_SIZE = -1;

    public LogReplicationUtils(CorfuStore corfuStore) {
        try {
            this.corfuStore = corfuStore;
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE, ReplicationStatusKey.class,
                    ReplicationStatusVal.class, null, TableOptions.fromProtoSchema(ReplicationStatusVal.class));
            schedulerThread = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat(LogReplicationUtils.class.getName())
                    .build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.error("Failed to open the replication status table", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    public void subscribe(@Nonnull LRMultiNamespaceListener clientListener, @Nonnull String namespace,
                          @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest) {
        SubscriptionTask subscriptionTask = new SubscriptionTask(clientListener, namespace, streamTag,
                tablesOfInterest, DEFAULT_BUFFER_SIZE);
        schedulerThread.schedule(subscriptionTask, 0, TimeUnit.MILLISECONDS);
    }

    public void subscribe(@Nonnull LRMultiNamespaceListener clientListener, @Nonnull String namespace,
                          @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest, int bufferSize) {
        SubscriptionTask subscriptionTask = new SubscriptionTask(clientListener, namespace, streamTag,
                tablesOfInterest, bufferSize);
        schedulerThread.schedule(subscriptionTask, 0, TimeUnit.MILLISECONDS);
    }

    private class SubscriptionTask implements Runnable {
        private LRMultiNamespaceListener clientListener;
        private String namespace;
        private String streamTag;
        private List<String> tablesOfInterest;
        private int bufferSize;

        SubscriptionTask(LRMultiNamespaceListener clientListener, String namespace, String streamTag,
                         List<String> tablesOfInterest, int bufferSize) {
            this.clientListener = clientListener;
            this.namespace = namespace;
            this.streamTag = streamTag;
            this.tablesOfInterest = tablesOfInterest;
            this.bufferSize = bufferSize;
        }

        @Override
        public void run() {
            CorfuStoreMetadata.Timestamp timestamp = getValidSubscriptionTimestamp();

            if (bufferSize == DEFAULT_BUFFER_SIZE) {
                corfuStore.getRuntime().getTableRegistry().getStreamingManager().subscribeAcrossNamespaces(clientListener,
                        namespace, streamTag, tablesOfInterest, timestamp.getSequence());
            } else {
                corfuStore.getRuntime().getTableRegistry().getStreamingManager().subscribeAcrossNamespaces(clientListener,
                        namespace, streamTag, tablesOfInterest, timestamp.getSequence(), bufferSize);
            }
        }

        /**
         * Returns a valid timestamp at which the client stream listener can be subscribed.  A valid timestamp is one
         * which is outside the bounds of an ongoing full-sync.   Client callbacks are also invoked to ensure that
         * updates upto this timestamp have been read and processed.
         */
        private CorfuStoreMetadata.Timestamp getValidSubscriptionTimestamp() {

            // Number of milliseconds to wait before each retry
            int backoffRetryDurationMs = 5;

            // Max duration at which the wait time must be capped (2 minutes)
            int maxDurationThresholdMins = 2;

            // Retry until a timeout of max_duration_threshold_mins
            try {
                return IRetry.build(ExponentialBackoffRetry.class, () -> {
                    try {
                        // Check if snapshot sync is currently in progress.  If in progress, wait for it to finish
                        CorfuStoreMetadata.Timestamp latestTimestamp = getTimestamp();
                        if (checkSnapshotSyncOngoing(latestTimestamp)) {
                            waitSnapshotSyncCompletion(latestTimestamp);
                        }

                        // Invoke the client callback for multi table reads.  This will read all tables the client is
                        // interested in and return a timestamp.  Verify if snapshot sync was in progress at that
                        // timestamp.  If in progress, wait for it to complete and retry the whole workflow.
                        CorfuStoreMetadata.Timestamp multiTableReadTimestamp = clientListener.performMultiTableReads();
                        if (checkSnapshotSyncOngoing(multiTableReadTimestamp)) {
                            waitSnapshotSyncCompletion(multiTableReadTimestamp);
                            log.info("Snapshot sync was going on during multi table read.  Rerun the checks and " +
                                "re-trigger multiTable read");
                            throw new RetryNeededException();
                        }

                        // Snapshot sync was not in progress at the client's read timestamp.  Invoke the client callback
                        // to set the baseline at this timestamp.
                        clientListener.mergeTableOnSubscription(multiTableReadTimestamp);

                        // Return this timestamp.  The client's listener must be subscribed at this timestamp.
                        return multiTableReadTimestamp;
                    } catch (Exception e) {
                        log.error("Error while attempting to get a valid subscription timestamp.  Retrying.", e);
                        throw new RetryNeededException();
                    }
                }).setOptions(x -> {
                    x.setBackoffDuration(Duration.ofMillis(backoffRetryDurationMs));
                    x.setMaxRetryThreshold(Duration.ofMinutes(maxDurationThresholdMins));
                }).run();
            } catch (InterruptedException e) {
                log.error("Failed to get a valid subscription timestamp.", e);
                clientListener.onError(e);
            }
            return null;
        }

        /**
         * Gets the latest token from the sequencer and builds a timestamp from it
         * @return Timestamp
         */
        private CorfuStoreMetadata.Timestamp getTimestamp() {
            Token token = corfuStore.getRuntime().getSequencerView().query().getToken();
            return CorfuStoreMetadata.Timestamp.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();
        }

        /**
         * Checks if snapshot sync is ongoing at the given timestamp.
         * @param timestamp
         * @return
         */
        private boolean checkSnapshotSyncOngoing(CorfuStoreMetadata.Timestamp timestamp) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE, IsolationLevel.snapshot(timestamp))) {
                List<CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message>> entries =
                    txn.executeQuery(REPLICATION_STATUS_TABLE, p -> true);
                // In LR V1, it is a valid assumption that the size of replication status table will be 1 as there is
                // only 1 remote cluster.  This implementation will change in LR V2
                Preconditions.checkState(entries.size() == 1);

                CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> entry = entries.get(0);
                txn.commit();
                return !entry.getPayload().getDataConsistent();
            } catch (TransactionAbortedException e) {
                // Since this is a read-only transaction, the abort can only be if this version of the table is not
                // available in the JVM's cache.  The underlying cause for this error is TrimmedException
                // Logging the exception for debugging purposes.
                log.warn("Check for snapshot sync status failed with TX Abort.  Cause is: ", e.getCause());
                Preconditions.checkState(e.getCause() instanceof TrimmedException);

                // This means that there have been updates to the Replication Status table after the timestamp at
                // which the check was performed, these updates have been read in the JVM's cache and this version
                // has been evicted from the cache.  We cannot tell the status of snapshot sync.  Throw the exception
                // so that the caller retries the check with a later timestamp.
                throw e;
            }
        }

        /**
         * Wait for the snapshot sync to complete.  The completion is signalled when SnapshotSyncCompletionListener
         * invokes notifyAll().
         * @param timestamp
         */
        private void waitSnapshotSyncCompletion(CorfuStoreMetadata.Timestamp timestamp) throws InterruptedException {
            SnapshotSyncCompletionListener snapshotSyncCompletionListener = new SnapshotSyncCompletionListener(timestamp);
            corfuStore.subscribeListener(snapshotSyncCompletionListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG,
                    Arrays.asList(REPLICATION_STATUS_TABLE), timestamp);

            synchronized (timestamp) {
                while (!snapshotSyncCompletionListener.isSnapshotSyncComplete()) {
                    try {
                        timestamp.wait();
                    } catch (InterruptedException e) {
                        log.error("Exception while waiting for snapshot sync to complete", e);
                        corfuStore.unsubscribeListener(snapshotSyncCompletionListener);
                        throw e;
                    }
                }
                corfuStore.unsubscribeListener(snapshotSyncCompletionListener);
            }
        }
    }

    private class SnapshotSyncCompletionListener implements StreamListener {

        private CorfuStoreMetadata.Timestamp subscriptionTimestamp;

        @Getter
        private boolean snapshotSyncComplete;

        SnapshotSyncCompletionListener(CorfuStoreMetadata.Timestamp subscriptionTimestamp) {
            this.subscriptionTimestamp = subscriptionTimestamp;
            this.snapshotSyncComplete = false;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            Map<TableSchema, List<CorfuStreamEntry>> entries = results.getEntries();
            for (TableSchema tableSchema : entries.keySet()) {
                if (tableSchema.getTableName().equals(REPLICATION_STATUS_TABLE)) {
                    for (CorfuStreamEntry entry : entries.get(tableSchema)) {
                        ReplicationStatusVal status = (ReplicationStatusVal) entry.getPayload();
                        if (status.getDataConsistent()) {
                            // Snapshot Sync has ended.  Notify the waiting thread to continue
                            synchronized (subscriptionTimestamp) {
                                snapshotSyncComplete = true;
                                subscriptionTimestamp.notifyAll();
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Encountered an error while waiting for snapshot sync to complete.", throwable);
            throw new StreamingException(throwable);
        }

    }
}
