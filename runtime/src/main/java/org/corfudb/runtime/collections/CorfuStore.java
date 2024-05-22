package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.Queue.CorfuGuidMsg;
import org.corfudb.runtime.Queue.CorfuQueueMetadataMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;

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

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(@Nonnull FullyQualifiedTableName fqTableName,
                             @Nonnull final TableDescriptor<K, V, M> descriptor,
                             @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        long startTime = System.currentTimeMillis();

        Table<K, V, M> table = runtime
                .getTableRegistry()
                .openTable(fqTableName, descriptor, tableOptions);

        corfuStoreMetrics.recordTableCount();

        log.info(
                CorfuRuntime.LOG_NOT_IMPORTANT,
                "openTable {}${} took {}ms",
                fqTableName.rawNamespace(), fqTableName.rawTableName(), (System.currentTimeMillis() - startTime)
        );
        return table;
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
    @Deprecated
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(@Nonnull final String namespace,
                             @Nonnull final String tableName,
                             @Nonnull final Class<K> kClass,
                             @Nonnull final Class<V> vClass,
                             @Nullable final Class<M> mClass,
                             @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return openTable(
                FullyQualifiedTableName.build(namespace, tableName),
                TableDescriptor.build(kClass, vClass, mClass, tableOptions),
                tableOptions
        );
    }

    /**
     * Fetches an existing table. This table should have been registered with this instance
     * of the Corfu runtime.
     *
     * @param fqTableName Table name.
     * @return Table instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(FullyQualifiedTableName fqTableName) {
        return runtime.getTableRegistry().getTable(fqTableName);
    }

    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(String namespace, String tableName) {
        return getTable(FullyQualifiedTableName.build(namespace, tableName));
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
     */
    @Nonnull
    public <V extends Message>
    Table<CorfuGuidMsg, V, CorfuQueueMetadataMsg> openQueue(
            @Nonnull final String namespace, @Nonnull final String queueName,
            @Nonnull final Class<V> vClass, @Nonnull final TableOptions tableOptions) {

        return runtime.getTableRegistry().openTable(
                FullyQualifiedTableName.build(namespace, queueName),
                TableDescriptor.build(CorfuGuidMsg.class, vClass, CorfuQueueMetadataMsg.class, tableOptions),
                tableOptions
        );
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
        runtime.getTableRegistry()
                .freeTableData(FullyQualifiedTableName.build(namespace, tableName));
    }

    /**
     * Deletes a table instance. [NOT SUPPORTED.]
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     */
    public void deleteTable(String namespace, String tableName) {
        runtime.getTableRegistry()
                .deleteTable(FullyQualifiedTableName.build(namespace, tableName));
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
        UUID streamId = FullyQualifiedTableName.build(namespace, tableName)
                .toStreamId()
                .getId();
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
     * Note: if memory is a consideration consider use the other version of subscribe that is
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
     * Note: if memory is a consideration consider use the other version of subscribe that is
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
     * Note: if memory is a consideration consider use the other version of subscribe that is
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
     * Get names of opened tables under the given namespace and stream tag.
     *
     * @param namespace
     * @param streamTag
     * @return table names (without namespace prefix)
     */
    private List<String> getTablesOfInterest(@Nonnull String namespace, @Nonnull String streamTag) {
        List<String> tablesOfInterest = runtime.getTableRegistry().listTables(namespace, streamTag);
        log.info("Tag[{}${}] :: Subscribing to {} tables - {}", namespace, streamTag, tablesOfInterest.size(), tablesOfInterest);
        return tablesOfInterest;
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
     * Gracefully shutdown a streamer.
     * Once this call returns no further stream updates will be returned.
     *
     * @param streamListener - callback context.
     */
    public void unsubscribeListener(@Nonnull StreamListener streamListener) {
        runtime.getTableRegistry().getStreamingManager().unsubscribe(streamListener);
    }
}
