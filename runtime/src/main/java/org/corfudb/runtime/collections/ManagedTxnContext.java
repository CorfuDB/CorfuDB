package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.Getter;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.view.Address;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Shim layer around CorfuStore's TxnContext for providing metadata management
 * capabilities, nested transaction support and more!
 *
 * Created by hisundar on 2020-09-16
 */
public class ManagedTxnContext implements AutoCloseable {

    /**
     * "Why can't you just inherit TxnContext instead of copying everything over?!! Grrrr.."
     * If we inherit TxnContext the support for autocloseable would be broken because,
     * -> To support AutoCloseable and nested transactions, we cannot return the parent
     * TxnContext out to the caller directly, because if the nested transactions goes out
     * of scope it will invoke close() and close the parent transaction prematurely.
     * So we need to return something that is not the parent object. Hence composition is
     * favored over inheritance.
     */
    private final TxnContext txnContext;

    @Getter
    private final boolean isNested;

    /**
     *  Use CorfuStoreShim.txn() to instantiate this object
     */
     ManagedTxnContext(@Nonnull final TxnContext txnContext,
                       @Nonnull final boolean isNested) {
         this.txnContext = txnContext;
         this.isNested = isNested;
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata, should be null at time of table creation.
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value) {
        putRecord(table, key, value, null, false);
    }

    /**
     * putRecord into a table using opened table object as a simple key-value.
     *
     * @param tableName - full string representation of the tableName
     * @param key - the key of the record being inserted
     * @param value - the payload or value of the record to be inserted
     * @param <K> - type of the key or identifier.
     * @param <V> - type of the value or payload
     * @param <M> - type of the metadata should be null at time of table creation.
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull String tableName,
                   @Nonnull final K key,
                   @Nonnull final V value) {
        putRecord(this.getTable(tableName), key, value, null, false);
    }

    public <K extends Message, V extends Message, M extends Message> Table<K, V, M> getTable(@Nonnull String tableName) {
        return this.txnContext.getTable(tableName);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record.
     * @param value Value or payload of the record.
     * @param metadata Metadata associated with the record for validations.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        putRecord(table, key, value, metadata, false);
    }

    /**
     * touch() is a call to create a conflict on a read in a write-only transaction
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record to touch.
     */
    public <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        this.txnContext.touch(table, key);
    }

    /**
     * touch() a key to generate a conflict on it given tableName.
     *
     * @param tableName Table object to perform the touch() in.
     * @param key       Key of the record.
     * @throws UnsupportedOperationException if attempted on a non-existing object.
     */
    public <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull String tableName, @Nonnull K key) {
        this.txnContext.touch(tableName, key);
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the delete on.
     */
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull Table<K, V, M> table) {
        this.txnContext.clear(table);
    }

    /**
     * Clears the entire table given the table name.
     *
     * @param tableName Full table name of table to be cleared.
     */
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull String tableName) {
        this.txnContext.clear(tableName);
    }

    /**
     * Deletes the specified key Without metadata validations.
     *
     * @param table Table object to perform the delete on.
     * @param key   Key of the record to be deleted.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext delete(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return this.txnContext.delete(table, key);
    }

    /**
     * Deletes the specified key on a table given its full name.
     *
     * @param tableName Table object to perform the delete on.
     * @param key       Key of the record to be deleted.
     */
    public <K extends Message, V extends Message, M extends Message>
    void delete(@Nonnull String tableName, @Nonnull K key) {
        this.txnContext.delete(tableName, key);
    }

    /**
     * Enqueue a message object into the CorfuQueue.
     *
     * @param table  Table object to perform the delete on.
     * @param record Record to be inserted into the Queue.
     * @return Queue.CorfuGuidMsg which can identify a record in the underlying table.
     */
    public <K extends Message, V extends Message, M extends Message>
    K enqueue(@Nonnull Table<K, V, M> table, @Nonnull V record) {
        return this.txnContext.enqueue(table, record);
    }

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
    CorfuStoreEntry<K, V, M> getRecord(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return this.txnContext.getRecord(table, key);
    }

    /**
     * get the full record from the table given a key.
     * If this is invoked on a Read-Your-Writes transaction, it will result in starting a corfu transaction
     * and applying all the updates done so far.
     *
     * @param tableName Table object to retrieve the record from
     * @param key       Key of the record.
     * @return CorfuStoreEntry<Key, Value, Metadata> instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry getRecord(@Nonnull String tableName, @Nonnull K key) {
        return this.txnContext.getRecord(tableName, key);
    }

    /**
     * Query by a secondary index.
     *
     * @param table     Table object.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull String indexName,
                                              @Nonnull I indexKey) {
        return this.txnContext.getByIndex(table, indexName, indexKey);
    }

    /**
     * Query by a secondary index given just the full tableName.
     *
     * @param tableName fullyQualified name of the table.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     *                  For non-primitive keys, 'null' will return all those instances
     *                  for which the secondary key was not explicitly set (when its parent was set).
     *                  For primitive keys, 'null' has no meaning, instead, specifying the primitive's
     *                  default value (as defined in Proto3, e.g., 0 for scalar or "" for strings)
     *                  will return all instances for which the secondary key field was unset (when its parent was set).
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull String tableName,
                                              @Nonnull String indexName,
                                              I indexKey) {
        return this.txnContext.getByIndex(tableName, indexName, indexKey);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull Table<K, V, M> table) {
        return this.txnContext.count(table);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param tableName - the namespace+table name of the table.
     * @return Count of records.
     */
    public int count(@Nonnull String tableName) {
        return this.txnContext.count(tableName);
    }

    /**
     * Gets all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull Table<K, V, M> table) {
        return this.txnContext.keySet(table);
    }

    /**
     * Get all the keys of a table just given its tableName.
     *
     * @param tableName fullyQualifiedTableName whose keys are requested.
     * @return keyset of the table
     */
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull String tableName) {
        return this.txnContext.keySet(tableName);
    }

    /**
     * Scan and filter by entry.
     *
     * @param table                    Table< K, V, M > object on which the scan must be done.
     * @param corfuStoreEntryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull Table<K, V, M> table,
                                                @Nonnull Predicate<CorfuStoreEntry<K, V, M>> corfuStoreEntryPredicate) {
        return this.txnContext.executeQuery(table, corfuStoreEntryPredicate);
    }

    /**
     * Scan and filter by entry.
     *
     * @param tableName                fullyQualified tablename to filter the entries on.
     * @param corfuStoreEntryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull String tableName,
                                                @Nonnull Predicate<CorfuStoreEntry<K, V, M>> corfuStoreEntryPredicate) {
        return this.txnContext.executeQuery(tableName, corfuStoreEntryPredicate);
    }

    /**
     * Execute a join of 2 tables.
     *
     * @param table1         First table in the join query.
     * @param table2         Second table to join with the first.
     * @param query1         Predicate to filter entries in table 1.
     * @param query2         Predicate to filter entries in table 2.
     * @param joinPredicate  Predicate to filter entries during the join.
     * @param joinFunction   Function to merge entries.
     * @param joinProjection Project the merged entries.
     * @return Result of query.
     */
    @Nonnull
    public <K1 extends Message,
            K2 extends Message,
            V1 extends Message,
            V2 extends Message,
            M1 extends Message,
            M2 extends Message,
            T,
            U> QueryResult<U> executeJoinQuery(
                    @Nonnull Table<K1, V1, M1> table1,
                    @Nonnull Table<K2, V2, M2> table2,
                    @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
                    @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
                    @Nonnull BiPredicate<V1, V2> joinPredicate,
                    @Nonnull BiFunction<V1, V2, T> joinFunction, Function<T, U> joinProjection) {
        return this.txnContext.executeJoinQuery(table1, table2, query1, query2, joinPredicate, joinFunction, joinProjection);
    }

    /**
     * Execute a join of 2 tables.
     *
     * @param table1         First table object.
     * @param table2         Second table to join with the first one.
     * @param query1         Predicate to filter entries in table 1.
     * @param query2         Predicate to filter entries in table 2.
     * @param queryOptions1  Query options to transform table 1 filtered values.
     * @param queryOptions2  Query options to transform table 2 filtered values.
     * @param joinPredicate  Predicate to filter entries during the join.
     * @param joinFunction   Function to merge entries.
     * @param joinProjection Project the merged entries.
     * @return Result of query.
     */
    @Nonnull
    public <K1 extends Message,
            K2 extends Message,
            V1 extends Message,
            V2 extends Message,
            M1 extends Message,
            M2 extends Message,
            R, S, T, U> QueryResult<U> executeJoinQuery(
                    @Nonnull Table<K1, V1, M1> table1,
                    @Nonnull Table<K2, V2, M2> table2,
                    @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
                    @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
                    @Nonnull QueryOptions<K1, V1, M1, R> queryOptions1,
                    @Nonnull QueryOptions<K2, V2, M2, S> queryOptions2,
                    @Nonnull BiPredicate<R, S> joinPredicate,
                    @Nonnull BiFunction<R, S, T> joinFunction, Function<T, U> joinProjection) {
        return this.txnContext.executeJoinQuery(table1,
                table2, query1, query2, queryOptions1,
                queryOptions2, joinPredicate, joinFunction, joinProjection);
    }

    /**
     * Test if a record exists in a table.
     *
     * @param table - table object to test if record exists
     * @param key   - key or identifier to test for existence.
     * @return true if record exists and false if record does not exist.
     */
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return this.txnContext.isExists(table, key);
    }

    /**
     * Variant of isExists that works on tableName instead of the table object.
     *
     * @param tableName - namespace + tablename of table being tested
     * @param key       - key to check for existence
     * @return - true if record exists and false if record does not exist.
     */
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull String tableName, @Nonnull K key) {
        return this.txnContext.isExists(tableName, key);
    }

    /**
     * Return all the Queue entries ordered by their parent transaction.
     * <p>
     * Note that the key in these entries would be the CorfuQueueIdMsg.
     *
     * @param table Table< K, V, M > object aka queue on which the scan must be done.
     * @return Collection of Queue records sorted by their transaction commit time.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<Table.CorfuQueueRecord> entryList(@Nonnull Table<K, V, M> table) {
        return this.txnContext.entryList(table);
    }

    /**
     * @return true if the transaction was started by this layer, false otherwise
     */
    public boolean isInTransaction() {
        return this.txnContext.isInMyTransaction();
    }


    /**
     * Explicitly abort a transaction in case of an external failure
     */
    public void txAbort() {
        this.txnContext.txAbort();
    }

    /**
     * putRecord into a table using just the tableName with metadata validations.
     *
     * @param tableName - full string representation of the table
     * @param key - the key of the record being inserted
     * @param value - the payload or value of the record to be inserted
     * @param metadata - the metadata which will be validated
     * @param <K> - type of the key or identifier.
     * @param <V> - type of the value or payload
     * @param <M> - type of the metadata
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull String tableName,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        putRecord(this.getTable(tableName), key, value, metadata, false);
    }

    /**
     * put the value on the specified key & create record if it does not exist given tableName
     *
     * @param tableName        Table object to perform the create/update on.
     * @param key              Key of the record.
     * @param value            Value or payload of the record.
     * @param metadata         Metadata associated with the record.
     * @param forceSetMetadata if specified, metadata will not be validated, just set
     * @param <K>              Type of Key.
     * @param <V>              Type of Value.
     * @param <M>              Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull String tableName,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata,
                   boolean forceSetMetadata) {
        putRecord(this.txnContext.getTable(tableName), key, value, metadata, forceSetMetadata);
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
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable M metadata,
                   boolean forceSetMetadata) {

        if (forceSetMetadata) {
            this.txnContext.putRecord(table, key, value, metadata);
            return;
        }

        if (metadata == null) {
            if (table.getMetadataOptions().isMetadataEnabled()) {
                metadata = (M) table.getMetadataOptions().getDefaultMetadataInstance();
            } else { // metadata is null and no metadata schema has been specified
                this.txnContext.putRecord(table, key, value, null);
                return;
            }
        }

        MergeCallbackImpl mergeCallback = new MergeCallbackImpl();
        this.txnContext.merge(table, key, mergeCallback, new CorfuRecord<>(value, metadata));
    }

    public String getNamespace() {
        return this.txnContext.getNamespace();
    }

    public Map<UUID, Table> getTablesInTxn() {
        return this.txnContext.getTablesInTxn();
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return txnContext.toString();
    }

    static class MergeCallbackImpl implements TxnContext.MergeCallback {
        /**
         * Core logic for handling metadata modifications on mutations
         *
         * @param table       - The table object on which the merge is being done.
         * @param key         - Key of the record on which merge is being done.
         * @param oldRecord   - The previous fetched record that exists in the table.
         * @param deltaUpdate - New record that will be inserted/updated.
         * @param <K>         - type of the key
         * @param <V>         - type of the value or payload
         * @param <M>         - type of the metadata
         * @return the merged record that will be inserted into or deleted from the table
         */
        @Override
        public <K extends Message, V extends Message, M extends Message>
        CorfuRecord<V, M> doMerge(Table<K, V, M> table,
                                  K key,
                                  CorfuRecord<V, M> oldRecord,
                                  CorfuRecord<V, M> deltaUpdate) {
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
                            Long givenRevision = (Long) deltaMetadata.getField(fieldDescriptor);
                            if (givenRevision == 0 || // Do not validate revision if field isn't set
                                    (givenRevision > 0 && // Validate revision only if set
                                            prevRevision.longValue() == givenRevision.longValue())) {
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
    }

    /**
     * Delete the record given the table object and metadata for validation.
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record to be deleted.
     * @param metadata Metadata to validate against.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void deleteRecord(@Nonnull Table<K, V, M> table,
                      @Nonnull final K key,
                      @Nullable final M metadata) {
        if (metadata == null) {
            this.txnContext.delete(table, key);
            return;
        }
        MergeCallbackForDeleteImpl mergeCallbackForDelete = new MergeCallbackForDeleteImpl();
        this.txnContext.merge(table, key, mergeCallbackForDelete, new CorfuRecord<>(null, metadata));
    }

    /**
     *
     * Delete a record given just tableName and the metadata for validation against.
     *
     * @param tableName - full string representation of the table
     * @param key - the key of the record being inserted
     * @param metadata - the metadata which will be validated
     * @param <K> - type of the key or identifier.
     * @param <V> - type of the value or payload
     * @param <M> - type of the metadata
     */
    public <K extends Message, V extends Message, M extends Message>
    void deleteRecord(@Nonnull String tableName,
                      @Nonnull final K key,
                      @Nullable final M metadata) {
        deleteRecord(this.getTable(tableName), key, metadata);
    }

    static class MergeCallbackForDeleteImpl implements TxnContext.MergeCallback {
        /**
         * Core logic for handling metadata modifications on delete with metadata.
         *
         * @param table       - The table object on which the merge is being done.
         * @param key         - Key of the record on which merge is being done.
         * @param oldRecord   - The previous fetched record that exists in the table.
         * @param deltaUpdate - Record with only the metadata passed in for validation.
         * @param <K>         - type of the key
         * @param <V>         - type of the value or payload
         * @param <M>         - type of the metadata
         * @return null if validation was successful or StaleRevision exception if not.
         */
        @Override
        public <K extends Message, V extends Message, M extends Message>
        CorfuRecord<V, M> doMerge(Table<K, V, M> table,
                                  K key,
                                  CorfuRecord<V, M> oldRecord,
                                  CorfuRecord<V, M> deltaUpdate) {
            M deltaMetadata = deltaUpdate.getMetadata();
            if (deltaUpdate.getPayload() != null) {
                throw new IllegalArgumentException("Non-null payload sent for delete validation");
            }

            final Message.Builder builder = deltaMetadata.toBuilder();
            for (Descriptors.FieldDescriptor fieldDescriptor : deltaMetadata.getDescriptorForType().getFields()) {
                if ("revision".equals(fieldDescriptor.getName())) {
                    if (oldRecord == null || oldRecord.getMetadata() == null) {
                        return null;
                    } else {
                        Long prevRevision = (Long) oldRecord.getMetadata().getField(fieldDescriptor);
                        Long givenRevision = (Long) deltaMetadata.getField(fieldDescriptor);
                        if (givenRevision == 0 || // Do not validate revision if field isn't set
                                (givenRevision > 0 && // Validate revision only if set
                                prevRevision.longValue() == givenRevision.longValue())) {
                            return null;
                        } else {
                            throw new StaleRevisionUpdateException(prevRevision, givenRevision);
                        }
                    }
                }
            }
            return null;
        }
    }

    /**
     * To allow nested transactions, we need to record all post commit callbacks.
     * This callback can be used to obtain all metadata and values that successfully
     * made it to the database.
     * This can be very useful in case there are modifications made by the merge
     * callback since protobuf objects are immutable by design.
     * @param commitCallback - post commit callback invoked after transaction commits.
     */
    public void addCommitCallback(@Nonnull TxnContext.CommitCallback commitCallback) {
        this.txnContext.addCommitCallback(commitCallback);
    }

    /**
     * Commit the transaction.
     * For a transaction that only has write operations, this method will start and end
     * a corfu transaction keeping the "critical" section small while batching up all updates.
     * For a transaction that has reads, this will end the transaction.
     * The commit returns successfully if the write transaction was committed.
     * Otherwise this throws a TransactionAbortedException with a cause.
     * The cause and the caller's intent of the transaction can determine if this aborted
     * Transaction can be retried.
     * If there are any commit callbacks registered, they will be invoked on successful commit.
     *
     * @return - address at which the commit of this transaction occurred.
     */
    public long commit() {
        if (isNested) {
            return Address.NON_ADDRESS;
        }
        return this.txnContext.commit();
    }

    /**
     * Nested transactions can still invoke the auto-closeable functions messing things up.
     * To prevent the mess-up, make sure that nested transactions don't close().
     */
    @Override
    public void close() {
        if (isNested) {
            return;
        }
        this.txnContext.close();
    }
}
