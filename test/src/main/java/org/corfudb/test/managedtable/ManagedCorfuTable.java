package org.corfudb.test.managedtable;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.ExampleSchemas.Person;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

@Builder
public class ManagedCorfuTable<K extends Message, V extends Message, M extends Message> {
    @NonNull
    private final ManagedCorfuTableConfig<K, V, M> config;
    @NonNull
    private final ManagedCorfuTableSetup<K, V, M> tableSetup;

    public static ManagedCorfuTable<Uuid, ExampleValue, ManagedMetadata> buildExample(CorfuRuntime rt) {

        ManagedCorfuTableConfig<Uuid, ExampleValue, ManagedMetadata> cfg = ManagedCorfuTableConfig
                .<Uuid, ExampleValue, ManagedMetadata>builder()
                .rt(rt)
                .kClass(Uuid.class)
                .vClass(ExampleValue.class)
                .mClass(ManagedMetadata.class)
                .build();

        ManagedCorfuTableSetup<Uuid, ExampleValue, ManagedMetadata> tableInit =
                new ManagedCorfuTableSetupManager<Uuid, ExampleValue, ManagedMetadata>().getPersistentCorfu();
        return ManagedCorfuTable
                .<Uuid, ExampleValue, ManagedMetadata>builder()
                .config(cfg)
                .tableSetup(tableInit)
                .build();

    }

    public static ManagedCorfuTable<Uuid, Uuid, Uuid> buildWithUuid(
            CorfuRuntime rt, ManagedCorfuTableSetup<Uuid, Uuid, Uuid> tableSetup) {
        ManagedCorfuTableConfig<Uuid, Uuid, Uuid> cfg = ManagedCorfuTableConfig
                .<Uuid, Uuid, Uuid>builder()
                .rt(rt)
                .kClass(Uuid.class)
                .vClass(Uuid.class)
                .mClass(Uuid.class)
                .build();

        return ManagedCorfuTable
                .<Uuid, Uuid, Uuid>builder()
                .config(cfg)
                .tableSetup(tableSetup)
                .build();
    }

    public static ManagedCorfuTable<Uuid, Uuid, Uuid> buildDefault(CorfuRuntime rt) {
        ManagedCorfuTableConfig<Uuid, Uuid, Uuid> cfg = ManagedCorfuTableConfig
                .<Uuid, Uuid, Uuid>builder()
                .rt(rt)
                .kClass(Uuid.class)
                .vClass(Uuid.class)
                .mClass(Uuid.class)
                .build();

        ManagedCorfuTableSetup<Uuid, Uuid, Uuid> tableInit =
                new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistentCorfu();

        return ManagedCorfuTable
                .<Uuid, Uuid, Uuid>builder()
                .config(cfg)
                .tableSetup(tableInit)
                .build();
    }

    @SneakyThrows
    public void execute(ThrowableConsumer<GenericCorfuTable<?, K, CorfuRecord<V, M>>> action) {
        try (GenericCorfuTable<? extends SnapshotGenerator<?>, K, CorfuRecord<V, M>> table = tableSetup.setup(config)) {
            action.accept(table);
        }
    }

    @Builder
    @Getter
    public static class ManagedCorfuTableConfig<K extends Message, V extends Message, M extends Message> {
        @NonNull
        private final CorfuRuntime rt;

        @NonNull
        private final Class<K> kClass;
        @NonNull
        private final Class<V> vClass;
        @NonNull
        private final Class<M> mClass;

        @Default
        private final String namespace = "some-namespace";
        @Default
        private final String tableName = "some-table";

        @Default
        private final boolean withSchema = true;

        private final String defaultInstanceMethodName = TableOptions.DEFAULT_INSTANCE_METHOD_NAME;

        public static ManagedCorfuTableConfig<Uuid, Uuid, Uuid> buildUuid(CorfuRuntime rt) {
            return ManagedCorfuTableConfig
                    .<Uuid, Uuid, Uuid>builder()
                    .rt(rt)
                    .kClass(Uuid.class)
                    .vClass(Uuid.class)
                    .mClass(Uuid.class)
                    .build();
        }


        Object[] getArgs() throws Exception {
            if (withSchema) {
                return new Object[]{getProtobufIndexer()};
            } else {
                return new Object[]{};
            }
        }

        public ProtobufIndexer getProtobufIndexer() throws Exception {
            SchemaOptions schemaOptions = getSchemaOptions();
            V msg = getDefaultValueMessage();
            return new ProtobufIndexer(msg, schemaOptions);
        }

        public SchemaOptions getSchemaOptions() throws Exception {
            return TableOptions.fromProtoSchema(vClass).getSchemaOptions();
        }

        /**
         * Fully qualified table name created to produce the stream uuid.
         */
        String getFullyQualifiedTableName() {
            return namespace + "$" + tableName;
        }

        public V getDefaultValueMessage() throws Exception {
            return (V) vClass.getMethod(defaultInstanceMethodName).invoke(null);
        }

        public K getDefaultKeyMessage() throws Exception {
            return (K) kClass.getMethod(defaultInstanceMethodName).invoke(null);
        }

        public M getDefaultMetadataMessage() throws Exception {
            return (M) mClass.getMethod(defaultInstanceMethodName).invoke(null);
        }
    }
}
