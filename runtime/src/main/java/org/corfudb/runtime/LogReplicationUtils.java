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
import org.corfudb.runtime.LogReplication.ReplicationStatusKey;
import org.corfudb.runtime.LogReplication.ReplicationStatusVal;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * LogReplication code resides in the infrastructure package.  Adding a dependency from this package(runtime) to
 * infrastructure introduces a circular dependency.  This class defines LR-specific constants and utility methods required in
 * runtime.  Note that these methods are unique and not duplicated from infrastructure.
 */
@Slf4j
public final class LogReplicationUtils {
    public static final String LR_STATUS_STREAM_TAG = "lr_status";

    public static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

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

        Table<ReplicationStatusKey, ReplicationStatusVal, Message> replicationStatusTable =
            openReplicationStatusTable(corfuStore);

        try {
            return IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txnContext = corfuStore.txn(namespace)) {
                    // The transaction is started in the client's namespace and the Replication Status table resides in the
                    // system namespace.  Corfu Store does not validate the cross-namespace access as long as there are no
                    // writes on the table in the different namespace.  This hack is required here as we want client full
                    // sync to happen in the same transaction which checks the status of a snapshot sync so that there is no
                    // window between the check and full sync.
                    List<CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message>> entries =
                        txnContext.executeQuery(replicationStatusTable, p -> true);

                    // In LR V1, it is a valid assumption that the size of replication status table will be 1 as there is
                    // only 1 remote cluster.  This implementation will change in LR V2
                    Preconditions.checkState(entries.size() == 1);
                    CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> entry = entries.get(0);

                    if (entry.getPayload().getDataConsistent()) {
                        // No snapshot sync is in progress
                        log.info("No Snapshot Sync is in progress.  Request the client to perform a full sync on its " +
                            "tables.");
                        Optional<Timer.Sample> clientFullSyncTimer = MicroMeterUtils.startTimer();
                        clientListener.performFullSync(txnContext);
                        MicroMeterUtils.time(clientFullSyncTimer, "logreplication.client.fullsync.duration");
                    } else {
                        // Snapshot sync is in progress.  Subscribe without performing a full sync on the tables.
                        log.info("Snapshot Sync is in progress.  Subscribing without performing a full sync on client" +
                            " tables.");
                        clientListener.getClientFullSyncPending().set(true);
                    }
                    txnContext.commit();

                    // Subscribe from the snapshot timestamp of this transaction, i.e., log tail when the transaction started.
                    // Subscribing from the commit address will result in missed updates which took place between the start
                    // and end of the transaction because reads in a transaction observe updates only till the snapshot when it
                    // started.
                    long subscriptionTimestamp = txnContext.getTxnSequence();
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
            throw new StreamingException(e);
        } finally {
            MicroMeterUtils.time(subscribeTimer, "logreplication.subscribe.duration");
        }
    }


    private static Table<ReplicationStatusKey, ReplicationStatusVal, Message> openReplicationStatusTable(CorfuStore corfuStore) {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE, ReplicationStatusKey.class,
                    ReplicationStatusVal.class, null, TableOptions.fromProtoSchema(ReplicationStatusVal.class));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.error("Failed to open the Replication Status table", e);
            throw new StreamingException(e);
        }
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
        long subscriptionTimestamp = getSubscriptionTimestamp(corfuStore, namespace, clientListener);
        log.info("Client full sync completed at timestamp {}", subscriptionTimestamp);
        clientListener.getClientFullSyncTimestamp().set(subscriptionTimestamp);
        clientListener.getClientFullSyncPending().set(false);
    }
}
