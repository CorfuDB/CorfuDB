package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * TxBuilder() is a layer that aggregates mutations made to CorfuStore protobuf tables
 * It can help reduce the footprint of a CorfuStore transaction by only having writes in it.
 * All mutations/writes are aggregated and applied at once a the time of commit() call
 * where a real corfu transaction is started.
 *
 * Created by zlokhandwala on 2019-08-05.
 */
public class TxBuilder {

    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    private final String namespace;
    private Timestamp timestamp;
    private final List<Runnable> operations;

    /**
     * Creates a new TxBuilder.
     *
     * @param objectsView   ObjectsView from the Corfu client.
     * @param tableRegistry Table Registry.
     * @param namespace     Namespace boundary defined for the transaction.
     */
    @Nonnull
    TxBuilder(@Nonnull final ObjectsView objectsView,
              @Nonnull final TableRegistry tableRegistry,
              @Nonnull final String namespace) {
        this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.operations = new ArrayList<>();
    }

    private <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return this.tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     * Creates a record for the specified key.
     *
     * @param tableName Table name to perform the create on.
     * @param key       Key.
     * @param value     Value to create.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder create(@Nonnull final String tableName,
                     @Nonnull final K key,
                     @Nonnull final V value,
                     @Nullable final M metadata) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> {
            table.create(key, value, metadata);
        });
        return this;
    }

    /**
     * Updates the value on the specified key.
     *
     * @param tableName Table name to perform the update on.
     * @param key       Key.
     * @param value     Value to update.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder update(@Nonnull final String tableName,
                     @Nonnull final K key,
                     @Nonnull final V value,
                     @Nullable final M metadata) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> {
            table.update(key, value, metadata);
        });
        return this;
    }

    /**
     *
     * @param streamId
     * @param updateEntry
     * @return
     */
    public TxBuilder logUpdate(UUID streamId, SMREntry updateEntry) {

        operations.add(() -> {
        TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry);
        });
        return this;
    }

    /**
     * Touches the specified key without mutating the version of the record.
     * This provides read after write conflict semantics.
     *
     * @param tableName Table name to perform the touch on.
     * @param key       Key to touch.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder touch(@Nonnull final String tableName,
                    @Nonnull final K key) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> {
            //TODO: Validate the get is executed.
            CorfuRecord<V, M> record = table.get(key);
        });
        return this;
    }

    /**
     * Deletes the specified key.
     *
     * @param tableName Table name to perform the delete on.
     * @param key       Key.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder delete(@Nonnull final String tableName,
                     @Nonnull final K key) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> table.delete(key));
        return this;
    }

    private void txBegin() {
        Transaction.TransactionBuilder transactionBuilder = this.objectsView
                .TXBuild()
                .type(TransactionType.OPTIMISTIC);
        if (timestamp != null) {
            transactionBuilder.snapshot(new Token(timestamp.getEpoch(), timestamp.getSequence()));
        }
        transactionBuilder.build().begin();
    }

    private void txEnd() {
        this.objectsView.TXEnd();
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the latest timestamp, applies all the updates and then
     * ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     */
    public void commit() {
        commit(null);
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the specified snapshot or at the latest snapshot
     * if no snapshot is specified, applies all the updates and then ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     *
     * @param timestamp Timestamp to commit the transaction on.
     */
    public void commit(final Timestamp timestamp) {
        this.timestamp = timestamp;
        try {
            txBegin();
            operations.forEach(Runnable::run);
        } finally {
            txEnd();
            operations.clear();
        }
    }
}
