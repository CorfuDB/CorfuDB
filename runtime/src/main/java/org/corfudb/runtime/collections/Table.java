package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.TransactionType;

import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.serializer.ISerializer;
import lombok.Getter;

/**
 * Wrapper over the CorfuTable.
 * It accepts a primary key - which is a protobuf message.
 * The value is a CorfuRecord which comprises of 2 fields - Payload and Metadata. These are protobuf messages as well.
 * <p>
 */
public class Table<K extends Message, V extends Message, M extends Message> {

    private final CorfuRuntime corfuRuntime;

    private final CorfuTable<K, CorfuRecord<V, M>> corfuTable;

    /**
     * Namespace this table belongs in.
     */
    @Getter
    private final String namespace;

    /**
     * Fully qualified table name: created by the namespace and the table name.
     */
    @Getter
    private final String fullyQualifiedTableName;

    @Getter
    private final MetadataOptions metadataOptions;

    /**
     * Returns a Table instance backed by a CorfuTable.
     *
     * @param namespace               Namespace of the table.
     * @param fullyQualifiedTableName Fully qualified table name.
     * @param valueSchema             Value schema to identify secondary keys.
     * @param corfuRuntime            Connected instance of the Corfu Runtime.
     * @param serializer              Protobuf Serializer.
     */
    @Nonnull
    public Table(@Nonnull final String namespace,
                 @Nonnull final String fullyQualifiedTableName,
                 @Nonnull final V valueSchema,
                 @Nullable final M metadataSchema,
                 @Nonnull final CorfuRuntime corfuRuntime,
                 @Nonnull final ISerializer serializer,
                 @Nonnull final StreamingMap<K, V> streamingMap) {

        this.corfuRuntime = corfuRuntime;
        this.namespace = namespace;
        this.fullyQualifiedTableName = fullyQualifiedTableName;
        this.metadataOptions = Optional.ofNullable(metadataSchema)
                .map(schema -> MetadataOptions.builder()
                                              .metadataEnabled(true)
                                              .defaultMetadataInstance(schema)
                                              .build())
                .orElse(MetadataOptions.builder().build());

        this.corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(CorfuTable.<K, CorfuRecord<V, M>>getTableType())
                .setStreamName(this.fullyQualifiedTableName)
                .setSerializer(serializer)
                .setArguments(new ProtobufIndexer(valueSchema), streamingMap)
                .open();
    }

    /**
     * Returns the underlying CorfuTable object.
     * NOTE: For internal use only.
     *
     * @return CorfuTable instance.
     */
    public CorfuTable<K, CorfuRecord<V, M>> getUnderlyingObject() {
        return corfuTable;
    }

    /**
     * Begins an optimistic transaction under the assumption that no transactional reads would be done.
     */
    private boolean TxBegin() {
        if (TransactionalContext.isInTransaction()) {
            return false;
        }
        corfuRuntime.getObjectsView()
                .TXBuild()
                .type(TransactionType.OPTIMISTIC)
                .build()
                .begin();
        return true;
    }

    /**
     * Ends an ongoing transaction.
     */
    private void TxEnd() {
        corfuRuntime.getObjectsView().TXEnd();
    }

    /**
     * Create a new record.
     *
     * @param key      Key
     * @param value    Value
     * @param metadata Record metadata.
     * @return Previously stored record if any.
     */
    @Nullable
    CorfuRecord<V, M> create(@Nonnull final K key,
                             @Nullable final V value,
                             @Nullable final M metadata) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            CorfuRecord<V, M> previous = corfuTable.get(key);
            if (previous != null) {
                throw new RuntimeException("Cannot create a record on existing key.");
            }
            M newMetadata = null;
            if (metadataOptions.isMetadataEnabled()) {
                if (metadata == null) {
                    throw new RuntimeException("Table::create needs non-null metadata");
                }
                M metadataDefaultInstance = (M) getMetadataOptions().getDefaultMetadataInstance();
                newMetadata = getNewMetadata(metadataDefaultInstance, metadata);
            }
            return corfuTable.put(key, new CorfuRecord<>(value, newMetadata));
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Fetch the value for a key.
     *
     * @param key Key.
     * @return Corfu Record for key.
     */
    @Nullable
    public CorfuRecord<V, M> get(@Nonnull final K key) {
        return corfuTable.get(key);
    }

    /**
     * Update an existing key with the provided value.
     *
     * @param key      Key.
     * @param value    Value.
     * @param metadata Metadata.
     * @return Previously stored value for the provided key.
     */
    @Nullable
    CorfuRecord<V, M> update(@Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            M newMetadata = null;
            if (metadataOptions.isMetadataEnabled()) {
                if (metadata == null) {
                    throw new RuntimeException("Table::update needs non-null metadata");
                }
                CorfuRecord<V, M> previous = corfuTable.get(key);
                M previousMetadata = Optional.ofNullable(previous)
                        .map(CorfuRecord::getMetadata)
                        .orElse((M)metadataOptions.getDefaultMetadataInstance());
                validateVersion(previousMetadata, metadata);
                newMetadata = getNewMetadata(previousMetadata, metadata);
            }
            return corfuTable.put(key, new CorfuRecord<>(value, newMetadata));
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Delete a record mapped to the specified key.
     *
     * @param key Key.
     * @return Previously stored Corfu Record.
     */
    @Nullable
    CorfuRecord<V, M> delete(@Nonnull final K key) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            return corfuTable.remove(key);
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Count of records in the table.
     *
     * @return Count of records.
     */
    int count() {
        return corfuTable.size();
    }

    /**
     * Scan and filter.
     *
     * @param p Predicate to filter the values.
     * @return Collection of filtered values.
     */
    @Nonnull
    Collection<CorfuRecord<V, M>> scanAndFilter(@Nonnull final Predicate<CorfuRecord<V, M>> p) {
        return new ArrayList<>(corfuTable.scanAndFilter(p));
    }

    /**
     * Get by secondary index.
     *
     * @param indexName Index name.
     * @param indexKey  Index key.
     * @param <I>       Type of index key.
     * @return Collection of entries filtered by the secondary index.
     */
    @Nonnull
    protected <I extends Comparable<I>>
    Collection<Entry<K, V>> getByIndex(@Nonnull final String indexName,
                                       @Nonnull final I indexKey) {
        return corfuTable.getByIndex(() -> indexName, indexKey).stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().getPayload()))
                .collect(Collectors.toList());
    }

    Set<Descriptors.FieldDescriptor.Type> versionTypes = new HashSet<>(Arrays.asList(
            Descriptors.FieldDescriptor.Type.INT32,
            Descriptors.FieldDescriptor.Type.INT64,
            Descriptors.FieldDescriptor.Type.UINT32,
            Descriptors.FieldDescriptor.Type.UINT64,
            Descriptors.FieldDescriptor.Type.SFIXED32,
            Descriptors.FieldDescriptor.Type.SFIXED64
    ));

    private <M extends Message> void validateVersion(@Nullable M previousMetadata,
                                                     @Nullable M userMetadata) {
        // TODO: do a lookup instead of a search if possible
        for (Descriptors.FieldDescriptor fieldDescriptor : userMetadata.getDescriptorForType().getFields()) {
            if (!fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                continue;
            }
            if (!versionTypes.contains(fieldDescriptor.getType())) {
                throw new IllegalArgumentException("Version field needs to be an Integer or Long type."+
                " Current type="+fieldDescriptor.getType());
            }
            long validatingVersion = (long) userMetadata.getField(fieldDescriptor);
            if (validatingVersion <= 0) {
                continue; // Don't validate if user has not explicitly set the version field.
            }
            long previousVersion = Optional.ofNullable(previousMetadata)
                    .map(m -> (Long) m.getField(fieldDescriptor))
                    .orElse(-1L);
            if (validatingVersion != previousVersion) {
                throw new RuntimeException("Stale object Exception: " + previousVersion + " != "
                        + validatingVersion);
            }
        }
    }

    private <M extends Message> M getNewMetadata(@Nonnull M previousMetadata,
                                                 @Nullable M userMetadata) {
        M.Builder builder = previousMetadata.toBuilder();
        for (Descriptors.FieldDescriptor fieldDescriptor : previousMetadata.getDescriptorForType().getFields()) {
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                builder.setField(
                        fieldDescriptor,
                        Optional.ofNullable(previousMetadata.getField(fieldDescriptor))
                                .map(previousVersion -> ((Long) previousVersion) + 1)
                                .orElse(0L));
            } else if (userMetadata != null){
                builder.setField(fieldDescriptor, userMetadata.getField(fieldDescriptor));
            }
        }
        return (M) builder.build();
    }

}
