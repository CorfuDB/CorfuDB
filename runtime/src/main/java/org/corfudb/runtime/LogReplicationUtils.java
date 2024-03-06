package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

    private LogReplicationUtils() { }

    public static void subscribe(@Nonnull LogReplicationListener clientListener, @Nonnull String namespace,
                          @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest, int bufferSize,
                          CorfuStore corfuStore) {

        long subscriptionTimestamp = getSubscriptionTimestamp(corfuStore, namespace, clientListener);
        corfuStore.getRuntime().getTableRegistry().getStreamingManager().subscribeLogReplicationListener(
                clientListener, namespace, streamTag, tablesOfInterest, subscriptionTimestamp, bufferSize);
        log.info("Client subscription at timestamp {} successful.", subscriptionTimestamp);
    }


    private static long getSubscriptionTimestamp(CorfuStore corfuStore, String namespace,
                                                 LogReplicationListener clientListener) {
        Optional<Counter> mvoTrimCounter = MicroMeterUtils.counter("logreplication.subscribe.trim.count");
        Optional<Counter> conflictCounter = MicroMeterUtils.counter("logreplication.subscribe.conflict.count");
        Optional<Timer.Sample> subscribeTimer = MicroMeterUtils.startTimer();

        Table<LogReplicationSession, ReplicationStatus, Message> replicationStatusTable =
            openReplicationStatusTable(corfuStore);

        try {
            return IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txnContext = corfuStore.txn(namespace)) {
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
                                        clientListener.getClientName()));

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
                        log.info("No record for client {} and Logical Group Model found in the Status Table.  " +
                                "Subscription could have been attempted before LR startup.  Subscribe the listener and" +
                                "wait for initial Snapshot Sync", clientListener.getClientName());
                    } else {
                        // For a given replication model and client, any Sink node will have a single session.
                        Preconditions.checkState(entries.size() == 1);
                        entry = entries.get(0);
                    }

                    boolean snapshotSyncInProgress = false;

                    if (entry == null || entry.getPayload().getSinkStatus().getDataConsistent()) {
                        // No snapshot sync is in progress
                        log.info("No Snapshot Sync is in progress.  Request the client to perform a full sync on its " +
                            "tables.");
                        Optional<Timer.Sample> clientFullSyncTimer = MicroMeterUtils.startTimer();
                        long clientFullSyncStartTime = System.currentTimeMillis();
                        clientListener.performFullSyncAndMerge(txnContext);
                        long clientFullSyncEndTime = System.currentTimeMillis();
                        MicroMeterUtils.time(clientFullSyncTimer, "logreplication.client.fullsync.duration");
                        log.info("Client Full Sync and Merge took {} ms",
                                clientFullSyncEndTime - clientFullSyncStartTime);
                    } else {
                        // Snapshot sync is in progress.  Subscribe without performing a full sync on the tables.
                        log.info("Snapshot Sync is in progress.  Subscribing without performing a full sync on client" +
                            " tables.");
                        snapshotSyncInProgress = true;
                    }
                    txnContext.commit();

                    // Subscribe from the snapshot timestamp of this transaction, i.e., log tail when the transaction started.
                    // Subscribing from the commit address will result in missed updates which took place between the start
                    // and end of the transaction because reads in a transaction observe updates only till the snapshot when it
                    // started.
                    long subscriptionTimestamp = txnContext.getTxnSequence();

                    // Update the flags and variables on the listener based on whether snapshot sync was in progress.
                    // This must be done only after the transaction commits.
                    setListenerParamsForSnapshotSync(clientListener, subscriptionTimestamp, snapshotSyncInProgress);

                    return subscriptionTimestamp;
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
                }
            }).run();
        } catch (InterruptedException e) {
            throw new StreamingException(e, StreamingException.ExceptionCause.SUBSCRIBE_ERROR);
        } finally {
            MicroMeterUtils.time(subscribeTimer, "logreplication.subscribe.duration");
        }
    }


    private static Table<LogReplicationSession, ReplicationStatus, Message> openReplicationStatusTable(CorfuStore corfuStore) {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE_NAME,
                    LogReplicationSession.class, ReplicationStatus.class, null,
                    TableOptions.fromProtoSchema(ReplicationStatus.class));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.error("Failed to open the Replication Status table", e);
            throw new StreamingException(e, StreamingException.ExceptionCause.SUBSCRIBE_ERROR);
        }
    }

    private static void setListenerParamsForSnapshotSync(LogReplicationListener listener, long subscriptionTimestamp,
                                                         boolean snapshotSyncInProgress) {
        updateListenerFlagsForSnapshotSync(listener, snapshotSyncInProgress);

        // If client full sync was done, set its timestamp
        if (!snapshotSyncInProgress) {
            listener.getClientFullSyncTimestamp().set(subscriptionTimestamp);
        }
    }

    private static void updateListenerFlagsForSnapshotSync(LogReplicationListener clientListener,
                                                           boolean snapshotSyncInProgress) {
        clientListener.getClientFullSyncPending().set(snapshotSyncInProgress);
        clientListener.getSnapshotSyncInProgress().set(snapshotSyncInProgress);
    }

    private static void incrementCount(Optional<Counter> counter) {
        counter.ifPresent(Counter::increment);
    }

    /**
     * If full sync on client tables was not performed during subscription, attempt to perform it now and complete
     * the subscription.
     * @param corfuStore
     * @param clientListener
     * @param namespace
     */
    public static void attemptClientFullSync(CorfuStore corfuStore, LogReplicationListener clientListener,
                                             String namespace) {
        getSubscriptionTimestamp(corfuStore, namespace, clientListener);
    }
}
