package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;

/**
 * CorfuStoreShim is a thin layer over CorfuStore that provides certain metadata management
 * that carry business logic specific to verticals.
 * <p>
 * Created by hisundar on 2020-09-16
 */
public class CorfuStoreShim {

    private final CorfuStore corfuStore;

    public CorfuStoreShim(CorfuRuntime runtime) {
        this.corfuStore = new CorfuStore(runtime);
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
     * @return Table instance.
     * @throws NoSuchMethodException     Thrown if key/value class are not protobuf classes.
     * @throws InvocationTargetException Thrown if key/value class are not protobuf classes.
     * @throws IllegalAccessException    Thrown if key/value class are not protobuf classes.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(
            @Nonnull String namespace, @Nonnull String tableName,
            @Nonnull Class<K> kClass, @Nonnull Class<V> vClass,
            @Nullable Class<M> mClass, @Nonnull TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        TableOptions tableOpts = TableOptions.fromProtoSchema(vClass, tableOptions);
        return corfuStore.openTable(
                FullyQualifiedTableName.build(namespace, tableName),
                TableDescriptor.build(kClass, vClass, mClass, tableOpts),
                tableOpts
        );
    }

    /**
     * Fetches an existing table. This table should have been registered with this instance
     * of the Corfu runtime by the long form above.
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     * @return Table instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull String namespace, @Nonnull String tableName) {
        return corfuStore.getTable(FullyQualifiedTableName.build(namespace, tableName));
    }

    /**
     * Creates and registers a Queue backed by a Table.
     * A table needs to be registered before it is used.
     *
     * @param namespace    Namespace of the table.
     * @param queueName    Queue's table name.
     * @param vClass       Class of the Queue's record Model.
     * @param tableOptions Table options.
     * @return Table instance.
     */
    @Nonnull
    public <V extends Message>
    Table<Queue.CorfuGuidMsg, V, Queue.CorfuQueueMetadataMsg> openQueue(
            @Nonnull String namespace, @Nonnull String queueName,
            @Nonnull Class<V> vClass, @Nonnull TableOptions tableOptions) {
        return corfuStore.openQueue(namespace, queueName, vClass, tableOptions);
    }

    /**
     * Deletes a table instance. [NOT SUPPORTED.]
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     */
    public void deleteTable(String namespace, String tableName) {
        corfuStore.deleteTable(namespace, tableName);
    }

    /**
     *
     * @param namespace the namespace this table belongs to
     * @param tableName name of the table
     * @throws java.util.NoSuchElementException if the table does not exist.
     */
    public void freeTableData(String namespace, String tableName) {
        corfuStore.freeTableData(namespace, tableName);
    }

    /**
     * Lists all the tables in a particular namespace.
     * Lists all the tables in the database if namespace is null.
     *
     * @param namespace Namespace to query.
     * @return Collection of TableNames.
     */
    @Nonnull
    public Collection<CorfuStoreMetadata.TableName> listTables(@Nullable String namespace) {
        return corfuStore.listTables(namespace);
    }

    /**
     * Start a transaction with snapshot isolation level at the latest available snapshot.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a Transaction context.
     */
    @Nonnull
    public ManagedTxnContext tx(@Nonnull final String namespace) {
        return this.tx(namespace, IsolationLevel.snapshot());
    }

    /**
     * Start appending mutations to a transaction.
     * The transaction does not begin until either a commit or the first read is invoked.
     * On read or commit the latest available snapshot will be used to resolve the transaction
     * unless the isolation level has a snapshot timestamp value specified.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @param isolationLevel Snapshot (latest or specific) at which the transaction must execute.
     * @return Returns a transaction context instance.
     */
    @Nonnull
    public ManagedTxnContext tx(@Nonnull final String namespace, IsolationLevel isolationLevel) {
        TxnContext txnContext = new TxnContext(corfuStore.getRuntime().getObjectsView(),
                corfuStore.getRuntime().getTableRegistry(),
                namespace,
                isolationLevel, false);
        return new ManagedTxnContext(txnContext, false);
    }

    /**
     * Return the address of the latest updated made in this table.
     *
     * @param namespace - namespace that this table belongs to.
     * @param tableName - table name of this table without the namespace prefixed in.
     * @return stream tail of this table.
     */
    public long getHighestSequence(@Nonnull String namespace, @Nonnull String tableName) {
        return corfuStore.getHighestSequence(namespace, tableName);
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
        corfuStore.subscribeListener(streamListener, namespace, streamTag);
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
    public void subscribeListener(@Nonnull StreamListener streamListener,
                                  @Nonnull String namespace,
                                  @Nonnull String streamTag,
                                  @Nonnull List<String> tablesOfInterest) {
        corfuStore.subscribeListener(streamListener, namespace, streamTag, tablesOfInterest);
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
    public void subscribeListener(@Nonnull StreamListener streamListener,
                                  @Nonnull String namespace,
                                  @Nonnull String streamTag,
                                  @Nonnull List<String> tablesOfInterest,
                                  @Nullable CorfuStoreMetadata.Timestamp timestamp) {
        corfuStore.subscribeListener(streamListener, namespace, streamTag, tablesOfInterest, timestamp);
    }

    /**
     * Subscribe to transaction updates on specific tables with the streamTag in the namespace.
     * Objects returned will honor transactional boundaries.
     *
     * @param streamListener   client listener for callback
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables of interest will be sent to listener
     * @param timestamp        if specified, all stream updates after this timestamp will be returned
     *                         if null, only future updates will be returned
     * @param bufferSize       maximum size of buffered transaction entries
     */
    public void subscribeListener(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                  @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                  @Nullable CorfuStoreMetadata.Timestamp timestamp, int bufferSize) {
        corfuStore.subscribeListener(streamListener, namespace, streamTag, tablesOfInterest, timestamp, bufferSize);
    }

    /**
     * Gracefully shutdown a streamer.
     * Once this call returns no further stream updates will be returned.
     *
     * @param streamListener - callback context.
     */
    public void unsubscribeListener(@Nonnull StreamListener streamListener) {
        corfuStore.unsubscribeListener(streamListener);
    }

    /**
     * Start a transaction with snapshot isolation level at the latest available snapshot.
     * Allow nested transactions for interoperability with older transaction types.
     * WARNING: Dirty reads *may not* be
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a Transaction context.
     */
    @Nonnull
    @Deprecated
    public ManagedTxnContext txn(@Nonnull final String namespace) {
        TxnContext txnContext = TxnContext.getMyTxnContext();
        boolean isNested = true;
        if (txnContext == null) {
            txnContext = new TxnContext(corfuStore.getRuntime().getObjectsView(),
                    corfuStore.getRuntime().getTableRegistry(),
                    namespace,
                    IsolationLevel.snapshot(), true);
            isNested = false;
        }
        return new ManagedTxnContext(txnContext, isNested);
    }

    public CorfuRuntime getRuntime() {
        return corfuStore.getRuntime();
    }
}
