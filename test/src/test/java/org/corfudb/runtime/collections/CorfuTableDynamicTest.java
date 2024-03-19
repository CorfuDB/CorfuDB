package org.corfudb.runtime.collections;

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

import static org.corfudb.test.RtParamsForTest.getLargeRtParams;

public class CorfuTableDynamicTest extends AbstractViewTest {

    @TestFactory
    public Stream<DynamicTest> dynamicTests() {
        List<ManagedCorfuTableSetup<Uuid, Uuid, Uuid>> tables = new ArrayList<>();
        tables.add(new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistentCorfu());
        tables.add(new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistedCorfu());

        return tables.stream().map(tableSetup -> {
            return DynamicTest.dynamicTest("dynamicTestFor-" + tableSetup, () -> {
                addSingleServer(SERVERS.PORT_0);

                buildNewManagedRuntime(getLargeRtParams(), rt -> {
                    ManagedCorfuTableConfig<Uuid, Uuid, Uuid> cfg = ManagedCorfuTableConfig
                            .<Uuid, Uuid, Uuid>builder()
                            .rt(rt)
                            .kClass(Uuid.class)
                            .vClass(Uuid.class)
                            .mClass(Uuid.class)
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
            });
        });
    }
}
