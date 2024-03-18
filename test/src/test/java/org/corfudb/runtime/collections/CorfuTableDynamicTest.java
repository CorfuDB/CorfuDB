package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.PersistentCorfuTableTest.CacheSizeForTest;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CorfuTableDynamicTest extends AbstractViewTest {


    @TestFactory
    public Stream<DynamicTest> dynamicTests() {
        List<ManagedCorfuTableSetup<Uuid, Uuid, Uuid>> tables = new ArrayList<>();
        tables.add(new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistentCorfu());
        tables.add(new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistedCorfu());

        return tables.stream().map(tableSetup -> DynamicTest.dynamicTest("dynamic", () -> {
            addSingleServer(SERVERS.PORT_0);

            buildNewManagedRuntime(getLargeRtParams(), rt -> {
                ManagedCorfuTableConfig<Uuid, Uuid, Uuid> cfg = ManagedCorfuTableConfig
                        .<Uuid, Uuid, Uuid>builder()
                        .rt(rt)
                        .kClass(Uuid.class)
                        .vClass(Uuid.class)
                        .mClass(Uuid.class)
                        .schemaOptions(null)
                        .build();

                ManagedCorfuTable
                        .<Uuid, Uuid, Uuid>builder()
                        .config(cfg)
                        .tableSetup(tableSetup)
                        .build()
                        .execute(corfuTable -> {
                            for (int i = 0; i < 100; i++) {
                                Uuid uuidMsg = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                                CorfuRecord<Uuid, Uuid> value1 = new CorfuRecord<>(uuidMsg, uuidMsg);
                                corfuTable.insert(uuidMsg, value1);
                            }
                        });
            });

            cleanupBuffers();
        }));
    }

    private CorfuRuntimeParameters getLargeRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.LARGE.size)
                .build();
    }

    private CorfuRuntimeParameters getMediumRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.MEDIUM.size)
                .build();
    }

    private CorfuRuntimeParameters getSmallRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.SMALL.size)
                .build();
    }
}
