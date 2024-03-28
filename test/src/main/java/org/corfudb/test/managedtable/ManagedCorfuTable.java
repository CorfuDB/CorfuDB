package org.corfudb.test.managedtable;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.test.CorfuTableSpec.CorfuTableSpecContext;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;

import java.util.Objects;
import java.util.function.Consumer;

@Accessors(fluent = true, chain = true)
public class ManagedCorfuTable<K extends Message, V extends Message, M extends Message> {
    @Setter
    private ManagedCorfuTableConfig<K, V, M> config;
    @Setter
    private ManagedRuntime managedRt;
    @Setter
    private ManagedCorfuTableSetup<K, V, M> tableSetup;

    public static <K extends Message, V extends Message, M extends Message> ManagedCorfuTable<K, V, M> build() {
        return new ManagedCorfuTable<>();
    }

    public static <K extends Message, V extends Message, M extends Message> ManagedCorfuTable<K, V, M>
    from(ManagedCorfuTableConfig<K, V, M> config, ManagedRuntime managedRt) {
        return new ManagedCorfuTable<K, V, M>()
                .config(config)
                .managedRt(managedRt);
    }

    public static ManagedCorfuTable<Uuid, ExampleValue, ManagedMetadata> buildExample(ManagedRuntime rt) {

        ManagedCorfuTableConfig<Uuid, ExampleValue, ManagedMetadata> cfg = ManagedCorfuTableConfig
                .<Uuid, ExampleValue, ManagedMetadata>builder()
                .tableDescriptor(TableDescriptors.EXAMPLE_VALUE)
                .build();

        return ManagedCorfuTable.<Uuid, ExampleValue, ManagedMetadata>build()
                .config(cfg)
                .managedRt(rt)
                .tableSetup(ManagedCorfuTableSetupManager.persistentCorfu());
    }

    public static ManagedCorfuTable<Uuid, Uuid, Uuid> buildDefault(ManagedRuntime rt) {
        ManagedCorfuTableConfig<Uuid, Uuid, Uuid> cfg = ManagedCorfuTableConfig
                .<Uuid, Uuid, Uuid>builder()
                .tableDescriptor(TableDescriptors.UUID)
                .build();

        return ManagedCorfuTable
                .<Uuid, Uuid, Uuid>build()
                .config(cfg)
                .managedRt(rt)
                .tableSetup(ManagedCorfuTableSetupManager.persistentCorfu());
    }

    @SneakyThrows
    public void execute(ThrowableConsumer<CorfuTableSpecContext<K, V, M>> action) throws Exception {
        Objects.requireNonNull(tableSetup);

        managedRt.connect(rt -> {
            try (GenericCorfuTable<? extends SnapshotGenerator<?>, K, CorfuRecord<V, M>> table =
                         tableSetup.setup(rt, config)) {
                action.accept(new CorfuTableSpecContext<>(rt, table));
            }
        });
    }

    public static class TableDescriptors {
        public static TableDescriptor<Uuid, Uuid, Uuid> UUID = new TableDescriptor<>(Uuid.class, Uuid.class, Uuid.class);
        public static TableDescriptor<Uuid, ExampleValue, ManagedMetadata> EXAMPLE_VALUE = new TableDescriptor<>(
                Uuid.class, ExampleValue.class, ManagedMetadata.class
        );
    }


    @Builder
    @Getter
    public static class ManagedCorfuTableConfig<K extends Message, V extends Message, M extends Message> {
        @NonNull
        private final TableDescriptor<K, V, M> tableDescriptor;

        @Default
        private final String namespace = "some-namespace";

        @Default
        private final String tableName = "some-table";

        @Default
        private final boolean withSchema = true;

        public static ManagedCorfuTableConfig<Uuid, Uuid, Uuid> buildUuid() {
            return ManagedCorfuTableConfig
                    .<Uuid, Uuid, Uuid>builder()
                    .tableDescriptor(TableDescriptors.UUID)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, ExampleValue, ManagedMetadata> buildExampleVal(
                Consumer<ManagedCorfuTableConfigBuilder<Uuid, ExampleValue, ManagedMetadata>> customizer) {

            ManagedCorfuTableConfigBuilder<Uuid, ExampleValue, ManagedMetadata> builder = ManagedCorfuTableConfig
                    .<Uuid, ExampleValue, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.EXAMPLE_VALUE);
            customizer.accept(builder);

            return builder.build();
        }


        Object[] getArgs() throws Exception {
            if (withSchema) {
                return new Object[]{getProtobufIndexer()};
            } else {
                return new Object[]{};
            }
        }

        public ProtobufIndexer getProtobufIndexer() throws Exception {
            SchemaOptions schemaOptions = tableDescriptor.getSchemaOptions();
            V msg = tableDescriptor.getDefaultValueMessage();
            return new ProtobufIndexer(msg, schemaOptions);
        }

        /**
         * Fully qualified table name created to produce the stream uuid.
         */
        String getFullyQualifiedTableName() {
            return namespace + "$" + tableName;
        }
    }
}
