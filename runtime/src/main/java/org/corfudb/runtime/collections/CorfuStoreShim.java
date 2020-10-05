package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;

/**
 * CorfuStoreShim is a thin layer over CorfuStore that provides certain metadata management
 * that carry business logic specific to verticals.
 *
 * Created by hisundar on 2020-09-16
 */
public class CorfuStoreShim {
    private final CorfuStore corfuStore;

    public CorfuStoreShim(CorfuRuntime runtime) {
        corfuStore = new CorfuStore(runtime);
    }

    /**
     * Fetches the latest logical timestamp (global tail) in Corfu's distributed log.
     *
     * @return Timestamp.
     */
    @Nonnull
    public CorfuStoreMetadata.Timestamp getTimestamp() {
        return corfuStore.getTimestamp();
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
        return corfuStore.openTable(namespace, tableName, kClass, vClass, mClass, tableOptions);
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
        return corfuStore.getTable(namespace, tableName);
    }

    /**
     * Lists all the tables in a particular namespace.
     * Lists all the tables in the database if namespace is null.
     *
     * @param namespace Namespace to query.
     * @return Collection of TableNames.
     */
    @Nonnull
    public Collection<CorfuStoreMetadata.TableName> listTables(@Nullable final String namespace) {
        return corfuStore.listTables(namespace);
    }

    /**
     * Start a transaction at the isolation level of the latest available corfu snapshot.
     * The transaction does not begin until either a commit is invoked or a read happens.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a transaction builder instance.
     */
    @Nonnull
    public TxnContextShim txn(@Nonnull final String namespace) {
        return new TxnContextShim(corfuStore.txn(namespace));
    }

    /**
     * Start appending mutations to a transaction.
     * The transaction does not begin until the first read or commit is invoked.
     * On read or commit the latest available snapshot will be used to resolve the transaction.
     *
     * @param namespace      Namespace of the tables involved in the transaction.
     * @param isolationLevel Snapshot (latest or specific) at which the transaction must execute.
     * @return Returns a transaction builder instance.
     */
    @Nonnull
    public TxnContextShim txn(@Nonnull final String namespace, IsolationLevel isolationLevel) {
        return new TxnContextShim(corfuStore.txn(namespace, isolationLevel));
    }

    /**
     * Subscribe to a specific a table in a namespace or the entire namespace.
     * Objects returned will honor transactional boundaries
     *
     * @param streamListener   - callback context.
     * @param namespace        - the CorfuStore namespace to subscribe to.
     * @param tablesOfInterest - If specified, only updates from these tables will be returned.
     * @param timestamp        - If specified, all stream updates from this timestamp will be returned
     *                         - if null, only future updates will be returned.
     */
    public <K extends Message, V extends Message, M extends Message>
    void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                   @Nonnull List<TableSchema<K, V, M>> tablesOfInterest,
                   @Nullable CorfuStoreMetadata.Timestamp timestamp) {
        corfuStore.subscribe(streamListener, namespace, tablesOfInterest, timestamp);
    }

    /**
     * Gracefully shutdown a streamer.
     * Once this call returns no further stream updates will be returned.
     *
     * @param streamListener - callback context.
     */
    public void unsubscribe(@Nonnull StreamListener streamListener) {
        corfuStore.unsubscribe(streamListener);
    }
}
