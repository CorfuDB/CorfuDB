package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Thin shim layer around CorfuStore's TxnContext for providing metadata management
 * capabilities.
 *
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class TxnContextShim extends TxnContext {

    public TxnContextShim(@Nonnull final ObjectsView objectsView,
                          @Nonnull final TableRegistry tableRegistry,
                          @Nonnull final String namespace,
                          @Nonnull final IsolationLevel isolationLevel) {
        super(objectsView, tableRegistry, namespace, isolationLevel);
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
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull String tableName,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        putRecord(this.getTable(tableName), key, value, metadata, false);
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
                   @Nullable final M metadata,
                   boolean forceSetMetadata) {

        if (forceSetMetadata || metadata == null) {
            super.putRecord(table, key, value, metadata);
            return;
        }

        super.merge(table, key, this::fixUpMetadata, new CorfuRecord<>(value, metadata));
    }

    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull String tableName,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata,
                   boolean forceSetMetadata) {
        putRecord(super.getTable(tableName), key, value, metadata, forceSetMetadata);
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
}
