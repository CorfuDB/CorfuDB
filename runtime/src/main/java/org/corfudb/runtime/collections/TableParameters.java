package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.CorfuOptions.PersistenceOptions;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;

/**
 * Table parameters including table's namespace, fullyQualifiedTableName,
 * key, value, and metadata classes, value and metadata schemas.
 * This class is mainly used to create an instance of {@link Table} class from its constructor.
 * This class also helps in reducing the number of parameters to the Table constructor.
 *
 * @param <K> - Default instance type of the protobuf generated key schema
 * @param <V> - Default instance type of the protobuf generated payload schema
 * @param <M> - Default instance type of the protobuf generated metadata schema
 */
@Builder(access = AccessLevel.PUBLIC)
public class TableParameters<K extends Message, V extends Message, M extends Message>{

    // Namespace of the table
    @NonNull
    @Getter
    private final String namespace;

    // Fully qualified table name
    @NonNull
    @Getter
    private final String fullyQualifiedTableName;

    // Key class
    @NonNull
    @Getter
    private final Class<K> kClass;

    // Value class
    @NonNull
    @Getter
    private final Class<V> vClass;

    // Metadata class
    @Getter
    private final Class<M> mClass;

    // Value schema to identify secondary keys
    @NonNull
    @Getter
    private final V valueSchema;

    // Default metadata instance
    @Getter
    private final M metadataSchema;

    // These schema options are extracted from TableOptions and passed around for feature flags
    // like stream_tags, secondary indexes, backup_restore, log replication etc
    @Getter
    private final SchemaOptions schemaOptions;

    @Getter
    private final PersistenceOptions persistenceOptions;

    @Getter
    private final boolean secondaryIndexesDisabled;

    public TableDescriptor<K, V, M> getDescriptor() {
        return new TableDescriptor<>(kClass, vClass, mClass, true);
    }
}
