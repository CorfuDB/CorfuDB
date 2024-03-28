package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.corfutable.GetVersionedObjectOptimizationSpec;
import org.corfudb.runtime.collections.corfutable.MultiRuntimeSpec;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.CorfuTableSpec;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.corfudb.test.managedtable.ManagedRuntime;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.corfudb.test.RtParamsForTest.getLargeRtParams;

public class CorfuTableDynamicTest extends AbstractViewTest {

    @TestFactory
    public Stream<DynamicTest> getVersionedObjectOptimizationSpec() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        return dynamicTest(
                getLargeRtParams(),
                ManagedCorfuTableConfig.buildUuid(),
                new GetVersionedObjectOptimizationSpec()
        );
    }

    @TestFactory
    public Stream<DynamicTest> multiRuntimeSpec() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        return dynamicTest(
                getLargeRtParams(),
                ManagedCorfuTableConfig.buildUuid(),
                new MultiRuntimeSpec()
        );
    }

    private <K extends Message, V extends Message, M extends Message>
    Stream<DynamicTest> dynamicTest(
            CorfuRuntimeParameters rtParams,
            ManagedCorfuTableConfig<K, V, M> cfg,
            CorfuTableSpec<K, V, M> spec
    ) {

        List<ManagedCorfuTableSetup<K, V, M>> tables = ImmutableList.of(
                ManagedCorfuTableSetupManager.persistentCorfu(),
                ManagedCorfuTableSetupManager.persistedCorfu()
        );

        return tables.stream()
                .map(tableSetup -> DynamicTest.dynamicTest(tableSetup.toString(), () -> {
                            ManagedRuntime managedRt = ManagedRuntime
                                    .from(rtParams)
                                    .setup(rt -> rt.parseConfigurationString(getDefaultConfigurationString()));

                            ManagedCorfuTable<K, V, M> managedTable = ManagedCorfuTable
                                    .<K, V, M>build()
                                    .config(cfg)
                                    .managedRt(managedRt)
                                    .tableSetup(tableSetup);

                            managedTable.execute(spec::test);
                            cleanupBuffers();
                        })
                );
    }
}
