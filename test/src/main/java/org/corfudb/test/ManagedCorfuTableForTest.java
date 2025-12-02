package org.corfudb.test;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.ImmutableCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;
import org.corfudb.util.serializer.ProtobufSerializer;

import java.util.HashSet;
import java.util.UUID;

@Builder
public class ManagedCorfuTableForTest<K extends Message, V extends Message, M extends Message> {
    @NonNull
    private final CorfuRuntime rt;

    @NonNull
    private final Class<K> kClass;
    @NonNull
    private final Class<V> vClass;
    @NonNull
    private final Class<M> mClass;

    private final SchemaOptions schemaOptions;

    @Default
    private final GenericCorfuTable<?,K, CorfuRecord<V, M>> table = new PersistentCorfuTable<>();

    @Default
    private final String namespace = "some-namespace";
    @Default
    private final String tableName = "some-table";

    public static ManagedCorfuTableForTest<Uuid, ExampleValue, ManagedMetadata> buildExample(CorfuRuntime rt)
            throws Exception {
        return ManagedCorfuTableForTest.<Uuid, ExampleValue, ManagedMetadata>builder()
                .rt(rt)
                .kClass(Uuid.class)
                .vClass(ExampleValue.class)
                .mClass(ManagedMetadata.class)
                .schemaOptions(TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions())
                .build();
    }

    public static ManagedCorfuTableForTest<Uuid, Uuid, Uuid> buildDefault(CorfuRuntime rt) {
        return ManagedCorfuTableForTest.<Uuid, Uuid, Uuid>builder()
                .rt(rt)
                .kClass(Uuid.class)
                .vClass(Uuid.class)
                .mClass(Uuid.class)
                .build();
    }

    @SneakyThrows
    public void execute(ThrowableConsumer<GenericCorfuTable<?,K, CorfuRecord<V, M>>> action) {
        try {
            setup(table);
            action.accept(table);
        } finally {
            table.close();
        }
    }

    @SneakyThrows
    private void setup(ICorfuSMR smr) {

        String defaultInstanceMethodName = "getDefaultInstance";
        K defaultKeyMessage = (K) kClass.getMethod(defaultInstanceMethodName).invoke(null);
        addTypeToClassMap(defaultKeyMessage);

        V defaultValueMessage = (V) vClass.getMethod(defaultInstanceMethodName).invoke(null);
        addTypeToClassMap(defaultValueMessage);

        M defaultMetadataMessage = (M) mClass.getMethod(defaultInstanceMethodName).invoke(null);
        addTypeToClassMap(defaultMetadataMessage);

        Object[] args = getArgs(defaultValueMessage);

        ProtobufSerializer serializer = getSerializer();
        final String fullyQualifiedTableName = getFullyQualifiedTableName();

        MVOCorfuCompileProxy proxy = new MVOCorfuCompileProxy(
                rt,
                UUID.nameUUIDFromBytes(fullyQualifiedTableName.getBytes()),
                table.getTableTypeToken().getRawType(),
                PersistentCorfuTable.class,
                args,
                serializer,
                new HashSet<UUID>(),
                smr,
                ObjectOpenOption.CACHE,
                rt.getObjectsView().getMvoCache()
        );

        smr.setCorfuSMRProxy(proxy);
    }

    private Object[] getArgs(V defaultValueMessage) {
        Object[] args = {};

        // If no schema options are provided, omit secondary indexes.
        if (schemaOptions != null) {
            args = new Object[]{new ProtobufIndexer(defaultValueMessage, schemaOptions)};
        }
        return args;
    }

    /**
     * Fully qualified table name created to produce the stream uuid.
     */
    private String getFullyQualifiedTableName() {
        return namespace + "$" + tableName;
    }

    /**
     * Adds the schema to the class map to enable serialization of this table data.
     */
    private <T extends Message> void addTypeToClassMap(T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        ProtobufSerializer serializer = getSerializer();

        serializer.getClassMap().put(typeUrl, msg.getClass());
    }

    private ProtobufSerializer getSerializer() {
        return rt
                .getSerializers()
                .getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE, ProtobufSerializer.class);
    }

    /**
     * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
     * Note: This is same as used in Any.proto.
     */
    private String getTypeUrl(Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }
}
