package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

/**
 * TxBuilder() is a layer that aggregates mutations made to CorfuStore protobuf tables
 * It can help reduce the footprint of a CorfuStore transaction by only having writes in it.
 * All mutations/writes are aggregated and applied at once a the time of commit() call
 * where a real corfu transaction is started.
 *
 * Created by zlokhandwala on 2019-08-05.
 */
@Slf4j
@Builder
public class TxBuilder {
    @NonNull
    private final ObjectsView objectsView;
    @NonNull
    private final TableRegistry tableRegistry;
    @NonNull
    private final String namespace;
    @Builder.Default
    private final List<Runnable> operations = new ArrayList<>();
    @Builder.Default
    private final int numRetries = 3;

    private <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@NonNull final String tableName) {
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
    @NonNull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder create(@NonNull final String tableName,
                     @NonNull final K key,
                     @NonNull final V value,
                     @Nullable final M metadata) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> table.create(key, value, metadata));
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
    @NonNull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder update(@NonNull final String tableName,
                     @NonNull final K key,
                     @NonNull final V value,
                     @Nullable final M metadata) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> {
            table.update(key, value, metadata);
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
    @NonNull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder touch(@NonNull final String tableName,
                    @NonNull final K key) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> {
            // This get isn't validated for execution.
            table.get(key);
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
    @NonNull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder delete(@NonNull final String tableName,
                     @NonNull final K key) {
        Table<K, V, M> table = getTable(tableName);
        operations.add(() -> table.delete(key));
        return this;
    }

    private void txBegin(final Timestamp timestamp) {
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
        int numRetriesRemaining = numRetries;
        while (numRetriesRemaining > 0) {
            numRetriesRemaining--;
            try {
                txBegin(timestamp);
                operations.forEach(Runnable::run);
            } catch (TransactionAbortedException txAbort) {
                if (txAbort.getAbortCause() == AbortCause.SEQUENCER_OVERFLOW &&
                    timestamp == null) {
                    // If in spite of corfu picking the latest timestamp, the transaction
                    // fails with a sequencer overflow, auto-retry a few times.
                    log.info("TxBuilder::commit failed on SEQUENCER_OVERFLOW: will retry {} times",
                            numRetriesRemaining);
                    continue;
                }
                log.info("TxBuilder::commit failed due to SEQUENCER_OVERFLOW:");
                throw txAbort;
            }
            txEnd();
            operations.clear();
        }
    }
}
