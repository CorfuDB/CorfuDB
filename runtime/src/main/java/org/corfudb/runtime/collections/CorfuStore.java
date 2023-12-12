package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LiteRoutingQueueListener;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.CorfuGuidGenerator;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * CorfuStore is a protobuf API layer that provides all the features of CorfuDB.
 * <p>
 * Key APIs exposed are:
 * o-> TxnContext() for CRUD operations
 * o-> table lifecycle management
 * <p>
 * Created by zlokhandwala on 2019-08-02.
 */
@Slf4j
public class CorfuStore {

    @Getter
    private final CorfuRuntime runtime;

    private final CorfuStoreMetrics corfuStoreMetrics;

    /** Until the first request to access this guid generator is made,
     *  Do not attempt to instantiate the guid generator.
     */
    @Getter(lazy = true)
    private final CorfuGuidGenerator corfuGuidGenerator = new CorfuGuidGenerator(runtime);

    /**
     * Creates a new CorfuStore.
     *
     * @param runtime         Connected instance of the Corfu Runtime.
     */
    @Nonnull
    public CorfuStore(@Nonnull final CorfuRuntime runtime) {
        this.runtime = runtime;
        this.corfuStoreMetrics = new CorfuStoreMetrics();
    }

    /**
     * Fetches the latest logical timestamp (global tail) in Corfu's distributed log.
     *
     * @return Timestamp.
     */
    @Nonnull
    private Timestamp getTimestamp() {
        Token token = runtime.getSequencerView().query().getToken();
        return Timestamp.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();
    }

    /**
     * Creates and registers a table.
     * A table needs to be registered before it is used.
     *
     * @param namespace    Namespace of the table.
     * @param tableName    Table name.
     * @param kClass       Class of the Key Model.
     * @param vClass       Class of the Value Model.
     * @param mClass       Class of the Metadata Model.
     * @param tableOptions Table options.
     * @param <K>          Key type.
     * @param <V>          Value type.
     * @param <M>          Type of Metadata.
     * @return Table instance.
     * @throws NoSuchMethodException     Thrown if key/value class are not protobuf classes.
     * @throws InvocationTargetException Thrown if key/value class are not protobuf classes.
     * @throws IllegalAccessException    Thrown if key/value class are not protobuf classes.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(@Nonnull final String namespace,
                             @Nonnull final String tableName,
                             @Nonnull final Class<K> kClass,
                             @Nonnull final Class<V> vClass,
                             @Nullable final Class<M> mClass,
                             @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        long startTime = System.currentTimeMillis();
        Table table =
                runtime.getTableRegistry().openTable(namespace, tableName, kClass, vClass, mClass, tableOptions);
        corfuStoreMetrics.recordTableCount();
        log.info(CorfuRuntime.LOG_NOT_IMPORTANT, "openTable {}${} took {}ms", namespace, tableName, (System.currentTimeMillis() - startTime));
        return table;
    }

    /**
     * Fetches an existing table. This table should have been registered with this instance
     * of the Corfu runtime.
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     * @return Table instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String namespace,
                            @Nonnull final String tableName) {
        return runtime.getTableRegistry().getTable(namespace, tableName);
    }

    /**
     * Creates and registers a Queue backed by a Table.
     * A table needs to be registered before it is used.
     *
     * @param namespace    Namespace of the table.
     * @param queueName    Queue's table name.
     * @param vClass       Class of the Queue's record Model.
     * @param tableOptions Table options.
     * @param <V>          Value type.
     * @return Table instance.
     * @throws NoSuchMethodException     Thrown if key/value class are not protobuf classes.
     * @throws InvocationTargetException Thrown if key/value class are not protobuf classes.
     * @throws IllegalAccessException    Thrown if key/value class are not protobuf classes.
     */
    @Nonnull
    public <V extends Message>
    Table<Queue.CorfuGuidMsg, V, Queue.CorfuQueueMetadataMsg> openQueue(@Nonnull final String namespace,
                                                                        @Nonnull final String queueName,
                                                                        @Nonnull final Class<V> vClass,
                                                                        @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        return runtime.getTableRegistry().openTable(namespace, queueName,
                Queue.CorfuGuidMsg.class, vClass, Queue.CorfuQueueMetadataMsg.class, tableOptions);
    }

    /**
     * This api is to optimize the use of memory by only allowing
     * the underlying corfu table's objects to be garbage collected.
     * The metadata associated with the table (such as schema) are not freed.
     * The effects of this function are fully reversible as the
     * table will be re-synced on the next access.
     * Note that the table entries in the AddressSpaceView's cache may still linger around until
     * timeout or eviction as this method only removes references from ObjectViewCache
     *
     * @param namespace namespace this table belongs to
     * @param tableName name of table
     * @throws java.util.NoSuchElementException thrown if table was not found.
     */
    public void freeTableData(String namespace, String tableName) {
        runtime.getTableRegistry().freeTableData(namespace, tableName);
    }

    /**
     * Deletes a table instance. [NOT SUPPORTED.]
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     */
    public void deleteTable(String namespace, String tableName) {
        runtime.getTableRegistry().deleteTable(namespace, tableName);
    }

    /**
     * Lists all the tables in a particular namespace.
     * Lists all the tables in the database if namespace is null.
     *
     * @param namespace Namespace to query.
     * @return Collection of TableNames.
     */
    @Nonnull
    public Collection<TableName> listTables(@Nullable final String namespace) {
        return runtime.getTableRegistry().listTables(namespace);
    }

    /**
     * Start a transaction with snapshot isolation level at the latest available snapshot.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a Transaction context.
     */
    @Nonnull
    public TxnContext txn(@Nonnull final String namespace) {
        return this.txn(namespace, IsolationLevel.snapshot());
    }

    /**
     * Start a corfu transaction and bind it to a ThreadLocal TxnContext.
     *
     * @param namespace      Namespace of the tables involved in the transaction.
     * @param isolationLevel Snapshot (latest or specific) at which the transaction must execute.
     * @return Returns a transaction context instance.
     */
    @Nonnull
    public TxnContext txn(@Nonnull final String namespace, IsolationLevel isolationLevel) {
        return new TxnContext(
                this.runtime.getObjectsView(),
                this.runtime.getTableRegistry(),
                namespace,
                isolationLevel,
                false);
    }

    @Nonnull
    public ScopedTransaction scopedTxn(
            @Nonnull final String namespace, IsolationLevel isolationLevel,
            Table<?, ?, ?>... tables) {
        try {
            return IRetry.build(IntervalRetry.class, () -> {
                try {
                    return new ScopedTransaction(
                            this.runtime,
                            namespace,
                            isolationLevel,
                            tables);
                } catch (TrimmedException e) {
                    log.error("Error while attempting to snapshot tables {}.", tables, e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static <K extends Message, V extends Message, M extends Message>
    ScopedTransaction snapshotFederatedTables(String namespace, CorfuRuntime runtime) {
        ArrayList<Table> federatedTables = new ArrayList<>();
        runtime.getTableRegistry().getAllOpenTables().forEach(t -> {
            String tableNameWithoutNs = StringUtils.substringAfter(t.getFullyQualifiedTableName(),
                t.getNamespace()+"$");
            CorfuStoreMetadata.TableName tableName = CorfuStoreMetadata.TableName.newBuilder()
                .setNamespace(t.getNamespace())
                .setTableName(tableNameWithoutNs).build();
            CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata> tableRecord =
                runtime.getTableRegistry().getRegistryTable().get(tableName);
            if (tableRecord.getMetadata().getTableOptions().getIsFederated()) {
                federatedTables.add(t);
            }
        });
        return new ScopedTransaction(runtime, namespace,
            IsolationLevel.snapshot(), federatedTables.toArray(new Table[federatedTables.size()]));
    }

    /**
     * Return the address of the latest update made in this table.
     * <p>
     * Note: we can't deliberately return the tail of the stream map
     * as this entry might be a HOLE, we need to filter and return only that
     * corresponding to the last DATA entry.
     *
     * @param namespace - namespace that this table belongs to.
     * @param tableName - table name of this table without the namespace prefixed in.
     * @return stream tail of this table.
     */
    public long getHighestSequence(@Nonnull final String namespace,
                                   @Nonnull final String tableName) {

        // Overall strategy:
        // (1) Retrieve streams full address map from sequencer
        // (2) Read entries in reverse to confirm it's a DATA type entry (for optimization
        // read in batches). A batch size of 4 is guaranteed to suit one edge case case scenario: i.e.,
        // consecutive checkpoints enforcing holes (3 rounds of CP accumulate before trim happens == 3 holes)

        Optional<Timer.Sample> startTime = MeterRegistryProvider.getInstance().map(Timer::start);
        UUID streamId = CorfuRuntime.getStreamID(
                TableRegistry.getFullyQualifiedTableName(namespace, tableName));
        StreamAddressSpace streamAddressSpace = this.runtime.getSequencerView()
                .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS));

        int numBatches = 0;

        if (streamAddressSpace.size() != 0) {
            NavigableSet<Long> addresses = new TreeSet<>();
            streamAddressSpace.forEachUpTo(Address.MAX, addresses::add);
            Iterable<List<Long>> batches = Iterables.partition(addresses.descendingSet(),
                    runtime.getParameters().getHighestSequenceNumberBatchSize());

            for (List<Long> batch : batches) {
                numBatches++;
                // TODO: We can optimize this such that actual data is not transferred back to the client,
                //  we require an API that inspects the data at the remote and indicates whether its an actual DATA
                //  entry or a HOLE, e.g.: Map<Long, DataType> entryTypes = runtime.getAddressSpaceView().getDataType(batch)
                Map<Long, ILogData> entries = runtime.getAddressSpaceView().read(batch);
                for (Long address : batch) {
                    if (!entries.get(address).isHole()) {
                        corfuStoreMetrics.recordBatchReads(numBatches);
                        corfuStoreMetrics.recordNumberReads(numBatches * runtime.getParameters().getHighestSequenceNumberBatchSize());
                        corfuStoreMetrics.recordHighestSequenceNumberDuration(startTime);
                        return address;
                    }
                }
            }
        }

        corfuStoreMetrics.recordBatchReads(numBatches);
        corfuStoreMetrics.recordNumberReads(numBatches * runtime.getParameters().getHighestSequenceNumberBatchSize());
        corfuStoreMetrics.recordHighestSequenceNumberDuration(startTime);
        log.debug("Stream[{}${}][{}] no DATA entry found. Returning -1.",
                namespace, tableName, Utils.toReadableId(streamId));
        return -1;
    }

    /**
     * Subscribe to transaction updates on specific tables with the streamTag in the namespace.
     * Objects returned will honor transactional boundaries.
     * <p>
     * This will subscribe to transaction updates starting from the latest state of the log.
     * <p>
     * Note: if memory is a consideration consider using the other version of subscribe that is
     * able to specify the size of buffered transactions entries.
     *
     * @param streamListener   callback context
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables of interest will be sent to listener
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest) {
        this.subscribeListener(streamListener,namespace, streamTag, tablesOfInterest, getTimestamp());
    }

    /**
     * Subscribe to transaction updates on specific tables with the streamTag in the namespace.
     * Objects returned will honor transactional boundaries.
     * <p>
     * This will subscribe to transaction updates starting from the latest state of the log.
     * <p>
     * Note: if memory is a consideration consider using the other version of subscribe that is
     * able to specify the size of buffered transactions entries.
     *
     * @param streamListener   callback context
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag) {
        this.subscribeListener(streamListener,namespace, streamTag, getTablesOfInterest(namespace, streamTag), getTimestamp());
    }

    /**
     * Subscribe to transaction updates on specific tables with the streamTag in the namespace.
     * Objects returned will honor transactional boundaries.
     * <p>
     * Note: if memory is a consideration consider using the other version of subscribe that is
     * able to specify the size of buffered transactions entries.
     *
     * @param streamListener   callback context
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables of interest will be sent to listener
     * @param timestamp        if specified, all stream updates from this timestamp will be returned,
     *                         if null, only future updates will be returned
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                  @Nullable Timestamp timestamp) {
        runtime.getTableRegistry().getStreamingManager()
                .subscribe(streamListener, namespace, streamTag, tablesOfInterest,
                        (timestamp == null) ? getTimestamp().getSequence() : timestamp.getSequence());
    }

    /**
     * Subscribe to transaction updates on specific tables with the streamTag in the namespace.
     * Objects returned will honor transactional boundaries.
     *
     * @param streamListener   client listener for callback
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables of interest will be sent to listener
     * @param timestamp        if specified, all stream updates after this timestamp will be returned,
     *                         if null, only future updates will be returned
     * @param bufferSize       maximum size of buffered transaction entries
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                  @Nullable Timestamp timestamp, int bufferSize) {
        runtime.getTableRegistry().getStreamingManager()
                .subscribe(streamListener, namespace, streamTag, tablesOfInterest,
                        (timestamp == null) ? getTimestamp().getSequence() : timestamp.getSequence(), bufferSize);
    }

    /**
     * Subscribe to transaction updates on all tables with the specified streamTag and namespace.
     * Objects returned will honor transactional boundaries.
     *
     * Note: all tables belonging to this stream tag must have been previously opened or subscription will fail.
     *
     * @param streamListener   client listener for callback
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param timestamp        if specified, all stream updates after this timestamp will be returned,
     *                         if null, only future updates will be returned
     * @param bufferSize       maximum size of buffered transaction entries
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag, @Nullable Timestamp timestamp, int bufferSize) {
        List<String> tablesOfInterest = getTablesOfInterest(namespace, streamTag);
        subscribeListener(streamListener, namespace, streamTag, tablesOfInterest, timestamp, bufferSize);
    }

    /**
     * Subscribe to the routing queue notifications arriving from a remote cluster.
     */
    public void subscribeRoutingQListener(@Nonnull LiteRoutingQueueListener routingQueueListener) {
        Timestamp ts = routingQueueListener.performFullSync();
        this.subscribeListener(routingQueueListener, CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_TAG,
                Arrays.asList(LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + routingQueueListener.getSourceSiteId() +
                    "_" + routingQueueListener.getClientName()), ts);
    }

    /**
     * Subscribe to transaction updates on all tables with the specified streamTag and namespace.
     * Objects returned will honor transactional boundaries.
     * <p>
     * Note: if memory is a consideration consider using the other version of subscribe that is
     * able to specify the size of buffered transactions entries.
     * <p>
     * Note: all tables belonging to this stream tag must have been previously opened or subscription will fail.
     *
     * @param streamListener   callback context
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param timestamp        if specified, all stream updates from this timestamp will be returned,
     *                         if null, only future updates will be returned
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag, @Nullable Timestamp timestamp) {
        List<String> tablesOfInterest = getTablesOfInterest(namespace, streamTag);
        subscribeListener(streamListener, namespace, streamTag, tablesOfInterest, timestamp);
    }


    /**
     * Subscription API exclusively for Log Replicator(LR).  It is used for subscribing to transaction updates for
     * replicated tables containing 'streamTag'.  Internally, the API delivers updates such that
     * LogReplicationListener(stream listener) is able to bifurcate replicated data received within or outside an LR
     * snapshot sync.
     * Objects returned will honour transaction boundaries.
     *
     * Note: All replicated tables belonging to this stream tag must be opened on the Log Replication Sink(receiver)
     * regardless of whether any data is replicated for it.
     *
     * Note: If memory is a consideration, consider using the other version of this API which takes a custom buffer
     * size for updates.
     *
     * @param streamListener  log replication client listener
     * @param namespace       namespace of the replicated tables
     * @param streamTag       stream tag of the replicated tables
     */
    public void subscribeLogReplicationListener(@Nonnull LogReplicationListener streamListener,
                                                @Nonnull String namespace, @Nonnull String streamTag) {
        int uninitializedBufferSize = 0;
        LogReplicationUtils.subscribe(streamListener, namespace, streamTag, getTablesOfInterest(namespace, streamTag),
                uninitializedBufferSize, this);
    }

    /**
     * Subscription API exclusively for Log Replicator(LR).  It is used for subscribing to transaction updates for
     * replicated tables containing 'streamTag'.  Internally, the API delivers updates such that
     * LogReplicationListener(stream listener) is able to bifurcate replicated data received within or outside an LR
     * snapshot sync.
     * Objects returned will honour transaction boundaries.
     *
     * Note: All replicated tables belonging to this stream tag must be opened on the Log Replication Sink(receiver)
     * regardless of whether any data is replicated for it.
     *
     * @param streamListener   log replication client listener
     * @param namespace        namespace of the replicated tables
     * @param streamTag        stream tag of the replicated tables
     * @param bufferSize       maximum size of buffered transaction entries
     */
    public void subscribeLogReplicationListener(@Nonnull LogReplicationListener streamListener,
                                                @Nonnull String namespace, @Nonnull String streamTag, int bufferSize) {
        LogReplicationUtils.subscribe(streamListener, namespace, streamTag, getTablesOfInterest(namespace, streamTag),
                bufferSize, this);
    }

    /**
     * Get names of opened tables under the given namespace and stream tag.
     *
     * @param namespace
     * @param streamTag
     * @return table names (without namespace prefix)
     */
    public List<String> getTablesOfInterest(@Nonnull String namespace, @Nonnull String streamTag) {
        List<String> tablesOfInterest = runtime.getTableRegistry().listTables(namespace, streamTag);
        log.info("Tag[{}${}] :: Subscribing to {} tables - {}", namespace, streamTag, tablesOfInterest.size(), tablesOfInterest);
        return tablesOfInterest;
    }

    /**
     * Gracefully shutdown a streamer.
     * Once this call returns no further stream updates will be returned.
     *
     * @param streamListener - callback context.
     */
    public void unsubscribeListener(@Nonnull StreamListener streamListener) {
        runtime.getTableRegistry().getStreamingManager().unsubscribe(streamListener);
    }
}
