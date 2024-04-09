package org.corfudb.test.managedtable;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.test.CorfuTableSpec.CorfuTableSpecContext;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedCorfuTableProtobufConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;

import java.util.Objects;

@Accessors(fluent = true, chain = true)
public class ManagedCorfuTable<K, V> {
    @Setter
    private ManagedCorfuTableConfig<K, V> config;
    @Setter
    private ManagedRuntime managedRt;
    @Setter
    private ManagedCorfuTableSetup<K, V> tableSetup;

    public static <K, V> ManagedCorfuTable<K, V> build() {
        return new ManagedCorfuTable<>();
    }

    public static <K, V> ManagedCorfuTable<K, V>
    from(ManagedCorfuTableConfig<K, V> config, ManagedRuntime managedRt) {
        return new ManagedCorfuTable<K, V>()
                .config(config)
                .managedRt(managedRt);
    }

    public static ManagedCorfuTable<Uuid, ExampleValue> buildExample(ManagedRuntime rt) {

        ManagedCorfuTableConfig<Uuid, ExampleValue> cfg = ManagedCorfuTableProtobufConfig
                .<Uuid, ExampleValue, ManagedMetadata>builder()
                .tableDescriptor(TableDescriptors.EXAMPLE_VALUE)
                .build();

        return ManagedCorfuTable.<Uuid, ExampleValue>build()
                .config(cfg)
                .managedRt(rt)
                .tableSetup(ManagedCorfuTableSetupManager.persistentProtobufCorfu());
    }

    public static ManagedCorfuTable<Uuid, Uuid> buildDefault(ManagedRuntime rt) {
        ManagedCorfuTableProtobufConfig<Uuid, Uuid, Uuid> cfg = ManagedCorfuTableProtobufConfig
                .<Uuid, Uuid, Uuid>builder()
                .tableDescriptor(TableDescriptors.UUID)
                .build();

        return ManagedCorfuTable
                .<Uuid, Uuid>build()
                .config(cfg)
                .managedRt(rt)
                .tableSetup(ManagedCorfuTableSetupManager.persistentProtobufCorfu());
    }

    @SneakyThrows
    public void execute(ThrowableConsumer<CorfuTableSpecContext<K, V>> action) throws Exception {
        Objects.requireNonNull(tableSetup);

        managedRt.connect(rt -> {
            try (GenericCorfuTable<? extends SnapshotGenerator<?>, K, V> table = tableSetup.open(rt, config)) {
                action.accept(new CorfuTableSpecContext<>(rt, table));
            }
        });
    }

    @SneakyThrows
    public void noRtExecute(ThrowableConsumer<CorfuTableSpecContext<K, V>> action) throws Exception {
        Objects.requireNonNull(tableSetup);

        CorfuRuntime rt = managedRt.getRt();
        try (GenericCorfuTable<? extends SnapshotGenerator<?>, K, V> table = tableSetup.open(rt, config)) {
            action.accept(new CorfuTableSpecContext<>(rt, table));
        }
    }

    public static class TableDescriptors {
        public static TableDescriptor<Uuid, Uuid, Uuid> UUID = new TableDescriptor<>(Uuid.class, Uuid.class, Uuid.class);
        public static TableDescriptor<Uuid, ExampleValue, ManagedMetadata> EXAMPLE_VALUE = new TableDescriptor<>(
                Uuid.class, ExampleValue.class, ManagedMetadata.class
        );
    }
}
