package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Thin shim layer around CorfuStore's TxnContext for providing metadata management
 * capabilities.
 *
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class TxnContextShim implements AutoCloseable {
    /**
     * Internal CorfuStore's txnContext
     */
    private TxnContext txnContext;

    public TxnContextShim(TxnContext txnContext) {
        this.txnContext = txnContext;
    }

    /**
     *************************** WRITE APIs *****************************************
     */

    /**
     * put the value on the specified key create record if it does not exist.
     * There are several overloaded flavors of put to support optional fields like
     * metadata, tableNames and the force set of metadata
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim putRecord(@Nonnull Table<K, V, M> table,
                             @Nonnull final K key,
                             @Nonnull final V value) {
        return putRecord(table, key, value, null, false);
    }

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim putRecord(@Nonnull String tableName,
                             @Nonnull final K key,
                             @Nonnull final V value) {
        return putRecord(this.txnContext.getTable(tableName), key, value, null, false);
    }

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim putRecord(@Nonnull Table<K, V, M> table,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata) {
        return putRecord(table, key, value, metadata, false);
    }

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim putRecord(@Nonnull String tableName,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata) {
        return putRecord(this.txnContext.getTable(tableName), key, value, metadata, false);
    }

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim putRecord(@Nonnull String tableName,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata,
                             boolean forceSetMetadata) {
        return putRecord(this.txnContext.getTable(tableName), key, value, metadata, forceSetMetadata);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param forceSetMetadata if specified, metadata will not be validated, just set
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim putRecord(@Nonnull Table<K, V, M> table,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata,
                             boolean forceSetMetadata) {

        if (forceSetMetadata || metadata == null) {
            this.txnContext = txnContext.put(table, key, value, metadata);
            return this;
        }

        this.txnContext = txnContext.merge(table, key,
                this::fixUpMetadata, new CorfuRecord<>(value, metadata));
        return this;
    }

    /**
     * Core logic for handling metadata modifications on mutations
     *
     * @param oldRecord - The previous fetched record that exists in the table.
     * @param deltaUpdate - New record that will be inserted/updated.
     * @param <K> - type of the key
     * @param <V> - type of the value or payload
     * @param <M> - type of the metadata
     * @return the merged record that will be inserted into the table.
     */
    private <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> fixUpMetadata(CorfuRecord<V, M> oldRecord, CorfuRecord<V, M> deltaUpdate) {
        M deltaMetadata = deltaUpdate.getMetadata();
        final Message.Builder builder = deltaMetadata.toBuilder();
        long currentTime = System.currentTimeMillis();
        for (Descriptors.FieldDescriptor fieldDescriptor : deltaMetadata.getDescriptorForType().getFields()) {
            switch (fieldDescriptor.getName()) {
                case "revision":
                    if (oldRecord == null || oldRecord.getMetadata() == null) {
                        builder.setField(fieldDescriptor, 0L);
                    } else {
                        Long prevRevision = (Long) oldRecord.getMetadata().getField(fieldDescriptor);
                        Long givenRevision = (Long)deltaMetadata.getField(fieldDescriptor);
                        if ((givenRevision > 0 && // Validate revision only if set
                                prevRevision.longValue() == givenRevision.longValue())
                                || givenRevision == 0) { // Do not validate revision if field isn't set
                            builder.setField(fieldDescriptor, prevRevision + 1);
                        } else {
                            throw new StaleRevisionUpdateException(prevRevision, givenRevision);
                        }
                    }
                    break;
                case "create_time":
                    if (oldRecord == null) {
                        builder.setField(fieldDescriptor, currentTime);
                    }
                    break;
                case "last_modified_time":
                    builder.setField(fieldDescriptor, currentTime);
                    break;
                default: // By default just merge any field not explicitly set with prev value
                    if (oldRecord != null
                            && oldRecord.getMetadata() != null
                            && oldRecord.getMetadata().hasField(fieldDescriptor)
                            && !deltaMetadata.hasField(fieldDescriptor)) {
                        builder.setField(fieldDescriptor, oldRecord.getMetadata().getField(fieldDescriptor));
                    } else {
                        builder.setField(fieldDescriptor, deltaMetadata.getField(fieldDescriptor));
                    }
            }
        }
        return new CorfuRecord<V, M>(deltaUpdate.getPayload(), (M) builder.build());
    }

    /**
     * touch() a key to generate a conflict on it. The value will not be modified.
     *
     * @param table    Table object to perform the touch() in.
     * @param key      Key of the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
     * @return TxnContext instance.
     * @throws UnsupportedOperationException if attempted on a non-existing object.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim touch(@Nonnull Table<K, V, M> table,
                         @Nonnull final K key) {
        this.txnContext = txnContext.touch(table, key);
        return this;
    }

    /**
     * touch() variant that accepts a table name instead of table object.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim touch(@Nonnull String tableName,
                         @Nonnull final K key) {
        return this.touch(txnContext.getTable(tableName), key);
    }

    /**
     *
     * @param table - table object to test if record exists
     * @param key - key or identifier to test for existence.
     * @param <K> - type of the key
     * @param <V> - type of payload or value
     * @param <M> - type of metadata
     * @return true if record exists and false if record does not exist.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull final K key) {
        CorfuStoreEntry<K, V, M> record = this.txnContext.getRecord(table, key);
        return record.getPayload() != null;
    }

    /**
     * Variant of isExists that works on tableName instead of the table object.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull String tableName, @Nonnull final K key) {
        return this.isExists(txnContext.getTable(tableName), key);
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the delete on.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim clear(@Nonnull Table<K, V, M> table) {
        this.txnContext = txnContext.clear(table);
        return this;
    }

    /**
     * Deletes the specified key.
     *
     * @param table Table object to perform the delete on.
     * @param key   Key of the record to be deleted.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim delete(@Nonnull Table<K, V, M> table,
                          @Nonnull final K key) {
        this.txnContext = txnContext.delete(table, key);
        return this;
    }

    public <K extends Message, V extends Message, M extends Message>
    TxnContextShim delete(@Nonnull String tableName,
                          @Nonnull final K key) {
        return this.delete(txnContext.getTable(tableName), key);
    }

    /**
     *************************** READ API *****************************************
     */

    /**
     * get the full record from the table given a key.
     * If this is invoked on a Read-Your-Writes transaction, it will result in starting a corfu transaction
     * and applying all the updates done so far.
     *
     * @param table Table object to retrieve the record from
     * @param key   Key of the record.
     * @return CorfuStoreEntry<Key, Value, Metadata> instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry getRecord(@Nonnull Table<K, V, M> table,
                              @Nonnull final K key) {
        return txnContext.getRecord(table, key);
    }

    /**
     * Query by a secondary index.
     *
     * @param table     Table object.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I extends Comparable<I>>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        return txnContext.getByIndex(table, indexName, indexKey);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull final Table<K, V, M> table) {
        return txnContext.count(table);
    }

    /**
     * Get all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull final Table<K, V, M> table) {
        return txnContext.keySet(table);
    }

    /**
     * Scan and filter by entry.
     *
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final Table<K, V, M> table,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        return txnContext.executeQuery(table, entryPredicate);
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the specified snapshot or at the latest snapshot
     * if no snapshot is specified, applies all the updates and then ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     *
     * @return - address at which the commit of this transaction occurred.
     */
    public long commit() {
        return txnContext.commit();
    }

    /**
     * Explicitly end a read only transaction or just clean up resources.
     */
    public void close() {
        txnContext.close();
    }

    /**
     * Explicitly abort a transaction in case of an external failure
     */
    public void txAbort() {
        txnContext.txAbort();
    }
}
