package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.corfutable.CorfuTableSpec;
import org.corfudb.runtime.collections.corfutable.GetVersionedObjectOptimizationSpec;
import org.corfudb.runtime.collections.corfutable.MultiRuntimeSpec;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.corfudb.test.RtParamsForTest.getLargeRtParams;

public class CorfuTableDynamicTest extends AbstractViewTest {

    @TestFactory
    public Stream<DynamicTest> getVersionedObjectOptimizationSpec() {
        return dynamicTests(tableSetup -> {
            addSingleServer(SERVERS.PORT_0);
            test(
                    getLargeRtParams(),
                    tableSetup,
                    (rt, corfuTable) -> new GetVersionedObjectOptimizationSpec(corfuTable, rt)
            );
            cleanupBuffers();
        });
    }

    @TestFactory
    public Stream<DynamicTest> multiRuntimeSpec() {
        return dynamicTests(tableSetup -> {
            addSingleServer(SERVERS.PORT_0);
            test(
                    getLargeRtParams(),
                    tableSetup,
                    (rt, corfuTable) -> new MultiRuntimeSpec(corfuTable, rt)
            );
            cleanupBuffers();
        });
    }

    public Stream<DynamicTest> dynamicTests(ThrowableConsumer<ManagedCorfuTableSetup<Uuid, Uuid, Uuid>> setupConsumer) {
        List<ManagedCorfuTableSetup<Uuid, Uuid, Uuid>> tables = new ArrayList<>();
        tables.add(new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistentCorfu());
        tables.add(new ManagedCorfuTableSetupManager<Uuid, Uuid, Uuid>().getPersistedCorfu());

        return tables
                .stream()
                .map(tableSetup -> DynamicTest.dynamicTest(tableSetup.toString(), () -> setupConsumer.accept(tableSetup)));
    }

    private void test(
            CorfuRuntimeParameters rtParams, ManagedCorfuTableSetup<Uuid, Uuid, Uuid> tableSetup,
            BiFunction<CorfuRuntime, GenericCorfuTable<?, Uuid, CorfuRecord<Uuid, Uuid>>, CorfuTableSpec> specLambda
    ) throws Exception {

        buildNewManagedRuntime(rtParams, rt -> {
            ManagedCorfuTable
                    .<Uuid, Uuid, Uuid>builder()
                    .config(ManagedCorfuTableConfig.buildUuid(rt))
                    .tableSetup(tableSetup)
                    .build()
                    .execute(corfuTable -> specLambda.apply(rt, corfuTable).test());
        });
    }
}
