package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Definition of the fully qualified Corfu Store Table initialized with default protobuf instances.
 * This class is necessary for StreamListener's deserialization of the stream entries.
 * It also allows a way to apply a filter and only specify the tables of interest to streaming.
 *
 * @param <K> - Default instance type of the protobuf generated key schema
 * @param <V> - Default instance type of the protobuf generated payload schema
 * @param <M> - Default instance type of the protobuf generated metadata schema
 *
 * created by hisundar on 2019-10-29
 */

@EqualsAndHashCode
@AllArgsConstructor
public class TableSchema<K extends Message, V extends Message, M extends Message> {
    private final String tableName;

    private final TableDescriptor<K, V, M> descriptor;

    @Deprecated
    public TableSchema(@Nonnull String tableName, @Nonnull Class<K> keyClass,
                       @Nonnull Class<V> payloadClass, @Nullable Class<M> metadataClass) {
        this(tableName, TableDescriptor.build(keyClass, payloadClass, metadataClass));
    }

    public String getTableName() {
        return this.tableName;
    }

    public Class<K> getKeyClass() {
        return descriptor.getKClass();
    }

    public Class<V> getPayloadClass() {
        return descriptor.getVClass();
    }

    public Class<M> getMetadataClass() {
        return descriptor.getMClass();
    }
}
