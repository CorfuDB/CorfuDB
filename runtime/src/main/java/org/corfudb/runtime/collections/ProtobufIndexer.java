package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuOptions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

/**
 * This layer implements the
 * ProtobufIndexer uses the special FieldOptions that the application can place on its
 * protobuf definitions (like secondary_key) and create secondary indexes callbacks over CorfuTable
 * based on that.
 *
 * Created by hisundar on 2019-08-12.
 */
public class ProtobufIndexer implements Index.Registry<Message, CorfuRecord<Message,
        Message>> {

    private final HashMap<String,
            Index.Spec<Message, CorfuRecord<Message, Message>, ? extends Comparable<?>>>
            indices = new HashMap<>();

    ProtobufIndexer(Message payloadSchema) {
        payloadSchema.getDescriptorForType().getFields().forEach(this::registerIndices);
    }

    private <T extends Comparable<T>> Index.Spec<Message, CorfuRecord<Message, Message>, ? extends Comparable<?>>
    getIndex(String indexName, FieldDescriptor fieldDescriptor) {

        return new Index.Spec<>(
                () -> indexName,
                (Index.Function<Message, CorfuRecord<Message, Message>, T>)
                        (key, val) -> ClassUtils.cast(val.getPayload().getField(fieldDescriptor)));
    }

    void registerIndices(final Descriptors.FieldDescriptor fieldDescriptor) {

        if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getSecondaryKey()) {
            final String indexName = fieldDescriptor.getName();
            if (fieldDescriptor.getType() == FieldDescriptor.Type.GROUP) {
                throw new IllegalArgumentException("group is a deprecated, unsupported type");
            }
            indices.put(indexName, getIndex(indexName, fieldDescriptor));
        }
    }

    @Override
    public Optional<
            Index.Spec<Message, CorfuRecord<Message, Message>, ? extends Comparable<?>>
            > get(Index.Name name) {
        return Optional.ofNullable(name).map(indexName -> indices.get(indexName));
    }

    @Override
    public Iterator<Index.Spec<Message, CorfuRecord<Message, Message>, ? extends Comparable<?>>> iterator() {
        return indices.values().iterator();

    }
}
