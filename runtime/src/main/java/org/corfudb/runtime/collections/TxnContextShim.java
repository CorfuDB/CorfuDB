package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Thin shim layer around CorfuStore's TxnContext for providing metadata management
 * capabilities.
 *
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class TxnContextShim extends TxnContext implements AutoCloseable {

    public TxnContextShim(@Nonnull final ObjectsView objectsView,
                          @Nonnull final TableRegistry tableRegistry,
                          @Nonnull final String namespace,
                          @Nonnull final IsolationLevel isolationLevel) {
        super(objectsView, tableRegistry, namespace, isolationLevel);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     * If metadata schema is present then metadata will be populated automatically in this layer.
     *
     * There are several overloaded flavors of put to support optional fields like
     * metadata, tableNames and the force set of metadata
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
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
     * @param <M> - type of the metadata
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull String tableName,
                   @Nonnull final K key,
                   @Nonnull final V value) {
        putRecord(this.getTable(tableName), key, value, null, false);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record.
     * @param value Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        putRecord(table, key, value, metadata, false);
    }

    /**
     *
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
        putRecord(super.getTable(tableName), key, value, metadata, forceSetMetadata);
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
            super.putRecord(table, key, value, metadata);
            return;
        }

        if (metadata == null) {
            if (table.getMetadataOptions().isMetadataEnabled()) {
                metadata = (M) table.getMetadataOptions().getDefaultMetadataInstance();
            } else { // metadata is null and no metadata schema has been specified
                super.putRecord(table, key, value, null);
                return;
            }
        }

        MergeCallbackImpl mergeCallback = new MergeCallbackImpl();
        super.merge(table, key, mergeCallback, new CorfuRecord<>(value, metadata));
    }

    class MergeCallbackImpl implements TxnContext.MergeCallback {
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
        @Override
        public <K extends Message, V extends Message, M extends Message>
        CorfuRecord<V, M> doMerge(Table<K, V, M> table,
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
    }

    /**
     * Protobuf objects are immutable. So any metadata modifications made here won't be
     * reflected back into the caller's in-memory object directly.
     * The caller is only really interested in the modified values of those transactions
     * that successfully commit.
     * To reflect metadata changes made here, we modify commit() to accept a callback
     * that carries all the final values of the changes made by this transaction.
     */
    public interface CommitCallback {
        /**
         * This callback returns a list of stream entries as opposed to CorfuStoreEntries
         * because if this transaction had operations like clear() then the CorfuStoreEntry
         * would just be empty.
         *
         * @param mutations - A group of all tables touched by this transaction along with
         *                    the updates made in each table.
         */
        void onCommit(Map<String, List<CorfuStreamEntry>> mutations);
    }

    /**
     * A wrapper around commit that will callback with all mutations made in this transaction.
     * @param commitCallback - callback with all final values of mutations made.
     * @return commitAddress at which all the transactional updates were written.
     */
    public long commit(@Nonnull CommitCallback commitCallback) {
        if (!TransactionalContext.isInTransaction()) {
            throw new IllegalStateException("commit(callback) called without a transaction!");
        }
        // CorfuStore should have only one transactional context since nesting is prohibited.
        AbstractTransactionalContext rootContext = TransactionalContext.getRootContext();
        Map<UUID, Table> tablesInTxn = getTablesInTxn();

        long commitAddress = super.commit(); // now invoke the actual commit
        // If we are here this means commit was successful, now invoke the callback with
        // the final versions of all the mutated objects.
        MultiObjectSMREntry writeSet = rootContext.getWriteSetInfo().getWriteSet();
        final Map<String, List<CorfuStreamEntry>> mutations = new HashMap<>();
        tablesInTxn.forEach((uuid, table) -> {
            List<CorfuStreamEntry> writesInTable = writeSet.getSMRUpdates(uuid).stream().map(entry ->
                    CorfuStreamEntry.fromSMREntry(entry, 0)).collect(Collectors.toList());
            mutations.put(table.getFullyQualifiedTableName(), writesInTable);
        });
        commitCallback.onCommit(mutations);
        return commitAddress;
    }
}
